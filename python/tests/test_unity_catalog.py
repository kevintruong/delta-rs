import json
import os
import shutil
import socket
import subprocess
import sysconfig
import time
import urllib.request
from contextlib import contextmanager
from http import HTTPStatus
from http.client import HTTPConnection
from http.server import BaseHTTPRequestHandler, HTTPServer
from threading import Thread
from typing import Generator
from urllib.parse import urlsplit

import pytest
from arro3.core import Table

from deltalake import DeltaTable
from deltalake.schema import Field, PrimitiveType, Schema


def _api_url(server: str, path: str) -> str:
    return f"{server}/api/2.1/unity-catalog/{path}"


def _post(server: str, path: str, body: dict) -> None:
    request = urllib.request.Request(
        _api_url(server, path),
        data=json.dumps(body).encode(),
        headers={"Content-Type": "application/json"},
    )
    urllib.request.urlopen(request, timeout=10).close()


@contextmanager
def _catalog_fault_proxy(
    server: str,
    fault: str | None = None,
    *,
    first_config_status: HTTPStatus | None = None,
):
    """Serve a single-table fixture with serialized requests and a shared revision.

    Its only metadata mutations are creation and the conditional Delta POST.
    The fixture owns its revision counter and uses the native server for storage.
    """
    target = urlsplit(server)
    counts = {"requests": 0, "writes": 0, "creates": 0, "config_reads": 0}
    revision = 0
    native_etag = None

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            self.forward()

        def do_POST(self):
            self.forward()

        def forward(self):
            nonlocal native_etag, revision
            counts["requests"] += 1
            body = self.rfile.read(int(self.headers.get("Content-Length", "0")))
            path = urlsplit(self.path).path
            if self.command == "GET" and path.endswith("/delta/v1/config"):
                counts["config_reads"] += 1
                if counts["config_reads"] == 1 and first_config_status is not None:
                    self.send_error(first_config_status)
                    return
            if self.command == "POST" and path == "/api/2.1/unity-catalog/tables":
                counts["creates"] += 1
            guarded_write = (
                self.command == "POST" and "/delta/v1/catalogs/" in self.path
            )
            inject_fault = False
            if guarded_write:
                counts["writes"] += 1
                inject_fault = counts["writes"] == 1
                if inject_fault and fault == "unknown":
                    self.send_error(HTTPStatus.SERVICE_UNAVAILABLE)
                    return
                payload = json.loads(body)
                guard = next(
                    r for r in payload["requirements"] if r["type"] == "assert-etag"
                )
                if guard["etag"] != f"fixture-{revision}" or (
                    inject_fault and fault == "stale_etag"
                ):
                    error = b'{"error":{"type":"UpdateRequirementConflictException"}}'
                    self.send_response(HTTPStatus.CONFLICT)
                    self.send_header("Content-Length", str(len(error)))
                    self.end_headers()
                    self.wfile.write(error)
                    return
                guard["etag"] = native_etag
                body = json.dumps(payload).encode()

            headers = {
                name: self.headers[name]
                for name in ("Authorization", "Content-Type")
                if name in self.headers
            }
            connection = HTTPConnection(target.hostname, target.port, timeout=5)
            try:
                connection.request(self.command, self.path, body or None, headers)
                response = connection.getresponse()
                response_body = response.read()
                if (
                    response.status == HTTPStatus.OK
                    and "/delta/v1/catalogs/" in self.path
                ):
                    payload = json.loads(response_body)
                    native_etag = payload["metadata"]["etag"]
                    if guarded_write:
                        revision += 1
                    payload["metadata"]["etag"] = f"fixture-{revision}"
                    response_body = json.dumps(payload).encode()
                if (
                    inject_fault
                    and fault == "ack_lost"
                    and response.status == HTTPStatus.OK
                ):
                    self.send_error(HTTPStatus.SERVICE_UNAVAILABLE)
                    return
                self.send_response(response.status)
                if (
                    self.command == "GET"
                    and "/unity-catalog/tables/" in self.path
                    and response.status == HTTPStatus.OK
                ):
                    response_body = json.dumps(
                        json.loads(response_body), sort_keys=True
                    ).encode()
                    self.send_header("ETag", f'"fixture-{revision}"')
                if content_type := response.getheader("Content-Type"):
                    self.send_header("Content-Type", content_type)
                self.send_header("Content-Length", str(len(response_body)))
                self.end_headers()
                self.wfile.write(response_body)
            finally:
                connection.close()

        def log_message(self, format, *args):
            pass

    with HTTPServer(("127.0.0.1", 0), Handler) as proxy:
        thread = Thread(target=proxy.serve_forever, daemon=True)
        thread.start()
        try:
            yield f"http://127.0.0.1:{proxy.server_port}", counts
        finally:
            proxy.shutdown()
            thread.join()


def _catalog_record(server: str, identifier: tuple[str, str, str]):
    with urllib.request.urlopen(
        _api_url(server, f"tables/{'.'.join(identifier)}"), timeout=5
    ) as response:
        return json.load(response)


@pytest.fixture(scope="module")
def unity_catalog_server(tmp_path_factory) -> Generator[str, None, None]:
    """Start `uc-server` and return its URL.

    The dependency group `unitycatalog` installs `uc-server`.
    """
    uc_server = shutil.which("uc-server") or shutil.which(
        "uc-server", path=sysconfig.get_path("scripts")
    )
    assert uc_server, "uc-server is not installed: uv pip install --group unitycatalog"

    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    server = f"http://127.0.0.1:{port}"

    process = subprocess.Popen(
        [uc_server],
        env={
            **os.environ,
            "UC_SERVER_BIND": f"127.0.0.1:{port}",
            "UC_SERVER_DATA_DIR": str(tmp_path_factory.mktemp("uc-server")),
            "UC_DB_URL": "memory",
        },
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    try:
        deadline = time.monotonic() + 30
        while True:
            try:
                urllib.request.urlopen(_api_url(server, "catalogs"), timeout=1).close()
                break
            except OSError:
                assert process.poll() is None, "uc-server stopped"
                assert time.monotonic() < deadline, "uc-server did not start"
                time.sleep(0.05)
        yield server
    finally:
        process.terminate()
        process.wait()


def test_catalog_rejects_invalid_sync_profile():
    from deltalake.catalog import load_catalog

    with pytest.raises(ValueError, match="sync profile"):
        load_catalog("unity", sync_profile="databricks")
    with pytest.raises(ValueError, match="sync profile"):
        load_catalog("glue", sync_profile="oss_rust_server_0_0_2")


@pytest.mark.unitycatalog
@pytest.mark.integration
@pytest.mark.parametrize(
    "status", [HTTPStatus.SERVICE_UNAVAILABLE, HTTPStatus.NOT_FOUND]
)
def test_catalog_registration_config_failure(unity_catalog_server, tmp_path, status):
    from deltalake.catalog import load_catalog
    from deltalake.exceptions import CatalogError

    identifier = (f"negotiation_{status.value}", "fixtures", "orders")
    _post(unity_catalog_server, "catalogs", {"name": identifier[0]})
    _post(
        unity_catalog_server,
        "schemas",
        {"catalog_name": identifier[0], "name": identifier[1]},
    )
    table = DeltaTable.create(tmp_path, Schema([Field("id", PrimitiveType("long"))]))
    with _catalog_fault_proxy(unity_catalog_server, first_config_status=status) as (
        proxy,
        counts,
    ):
        catalog = load_catalog(
            "unity",
            options={
                "workspace_url": proxy,
                "access_token": "fixture",
                "allow_http_url": "true",
            },
            sync_profile="oss_rust_server_0_0_2",
        )
        if status == HTTPStatus.SERVICE_UNAVAILABLE:
            with pytest.raises(CatalogError, match="catalog provider request failed"):
                catalog.register_table(identifier, tmp_path)
            assert counts["creates"] == 0
        assert catalog.register_table(identifier, tmp_path).status == "created"
        assert counts["creates"] == 1
        properties = _catalog_record(unity_catalog_server, identifier)["properties"]
        assert properties["delta-rs.table-id"] == table.metadata().id
        if status == HTTPStatus.SERVICE_UNAVAILABLE:
            assert counts["config_reads"] == 2
            assert properties["delta-rs.sync-version"] == str(table.version())
            assert properties["delta-rs.sync-digest"]
        else:
            assert counts["config_reads"] == 1
            assert "delta-rs.sync-version" not in properties
            assert (
                catalog.register_table(identifier, tmp_path).status
                == "already_registered"
            )


@pytest.mark.unitycatalog
@pytest.mark.integration
@pytest.mark.parametrize("fault", ["stale_etag", "unknown", "ack_lost"])
def test_catalog_guarded_sync(unity_catalog_server: str, tmp_path, fault: str):
    from deltalake.catalog import load_catalog
    from deltalake.exceptions import (
        CatalogConflictError,
        CatalogOutcomeUnknownError,
        CatalogUnsupportedError,
    )

    identifier = (f"guarded_{fault}", "fixtures", "orders")
    catalog_name, schema_name, _ = identifier
    _post(unity_catalog_server, "catalogs", {"name": catalog_name})
    _post(
        unity_catalog_server,
        "schemas",
        {"name": schema_name, "catalog_name": catalog_name},
    )
    path = tmp_path / "orders"
    table = DeltaTable.create(path, Schema([Field("order_id", PrimitiveType("long"))]))

    with _catalog_fault_proxy(unity_catalog_server, fault) as (proxy, counts):
        options = {
            "workspace_url": proxy,
            "access_token": "fixture-token",
            "allow_http_url": "true",
        }
        unqualified = load_catalog("unity", options=options)
        assert unqualified.supports_guarded_sync is False
        with pytest.raises(CatalogUnsupportedError):
            unqualified.sync_table_metadata(identifier)
        assert counts["requests"] == 0

        catalog = load_catalog(
            "unity", options=options, sync_profile="oss_rust_server_0_0_2"
        )
        assert catalog.supports_guarded_sync is True
        registration = catalog.register_table(identifier, path)
        before = _catalog_record(unity_catalog_server, identifier)
        assert [column["name"] for column in before["columns"]] == ["order_id"]
        assert before["table_id"] == registration.provider_id
        assert before["properties"]["delta-rs.sync-version"] == "0"

        stock = load_catalog(
            "unity",
            options={**options, "workspace_url": unity_catalog_server},
            sync_profile="oss_rust_server_0_0_2",
        )
        with pytest.raises(CatalogUnsupportedError):
            stock.sync_table_metadata(identifier)

        table.alter.add_columns(Field("carrier", PrimitiveType("string")))
        committed_version = DeltaTable(path).version()
        assert committed_version == 1
        delta_log = {
            entry.name: entry.read_bytes()
            for entry in (path / "_delta_log").glob("*.json")
        }

        if fault == "ack_lost":
            updated = catalog.sync_table_metadata(identifier)
            assert counts["writes"] == 1
        else:
            error = (
                CatalogOutcomeUnknownError
                if fault == "unknown"
                else CatalogConflictError
            )
            with pytest.raises(error) as caught:
                catalog.sync_table_metadata(identifier)
            if isinstance(caught.value, CatalogOutcomeUnknownError):
                assert caught.value.identifier == identifier
                attempted = caught.value.attempted_stamp
                assert attempted is not None
                assert attempted.delta_id == registration.delta_id
                assert attempted.delta_version == committed_version
            assert counts["writes"] == 1
            assert _catalog_record(unity_catalog_server, identifier) == before
            assert DeltaTable(path).version() == committed_version

            updated = catalog.sync_table_metadata(identifier)
            assert counts["writes"] == 2

        assert updated.status == "updated"
        assert updated.stamp.delta_id == registration.delta_id
        assert updated.stamp.delta_version == committed_version
        assert updated.stamp.format_version == 1
        assert updated.stamp.digest
        after = _catalog_record(unity_catalog_server, identifier)
        assert after["table_id"] == before["table_id"]
        assert after["properties"]["delta-rs.sync-version"] == str(committed_version)
        assert [
            (column["name"], column["type_text"]) for column in after["columns"]
        ] == [
            ("order_id", "bigint"),
            ("carrier", "string"),
        ]
        assert catalog.load_table(identifier).version() == committed_version

        writes = counts["writes"]
        unchanged = catalog.sync_table_metadata(identifier)
        assert unchanged.status == "unchanged"
        assert unchanged.stamp == updated.stamp
        assert counts["writes"] == writes
        assert {
            entry.name: entry.read_bytes()
            for entry in (path / "_delta_log").glob("*.json")
        } == delta_log
        if fault == "unknown":
            assert attempted == updated.stamp
            table.alter.add_columns(Field("tracking", PrimitiveType("string")))
            later = catalog.sync_table_metadata(identifier)
            assert later.stamp.delta_version > attempted.delta_version
            assert later.stamp.digest != attempted.digest


@pytest.mark.unitycatalog
@pytest.mark.pyarrow
@pytest.mark.integration
@pytest.mark.timeout(timeout=30, method="thread")
def test_uc_read_deltatable(
    unity_catalog_server: str,
    existing_sample_table: DeltaTable,
    sample_table: Table,
    monkeypatch,
):
    """Test delta table reads using Unity Catalog URL (uc://)"""
    import pyarrow as pa

    _post(unity_catalog_server, "catalogs", {"name": "unity"})
    _post(unity_catalog_server, "schemas", {"name": "default", "catalog_name": "unity"})
    _post(
        unity_catalog_server,
        "tables",
        {
            "name": "testtable",
            "catalog_name": "unity",
            "schema_name": "default",
            "table_type": "EXTERNAL",
            "data_source_format": "DELTA",
            "columns": [],
            "storage_location": existing_sample_table.table_uri,
        },
    )

    monkeypatch.setenv("DATABRICKS_WORKSPACE_URL", unity_catalog_server)
    # The server does not check the token, but the client needs one.
    monkeypatch.setenv("DATABRICKS_ACCESS_TOKEN", "123456")
    monkeypatch.setenv("UNITY_ALLOW_HTTP_URL", "true")

    dt = DeltaTable("uc://unity.default.testtable")

    assert dt.is_deltatable(dt.table_uri), True
    expected = pa.table(sample_table).to_pydict()
    assert dt.to_pyarrow_dataset().to_table().to_pydict() == expected


@pytest.mark.unitycatalog
@pytest.mark.pyarrow
@pytest.mark.integration
@pytest.mark.timeout(timeout=30, method="thread")
def test_uc_register_then_load_by_name(
    unity_catalog_server: str,
    existing_sample_table: DeltaTable,
    sample_table: Table,
    tmp_path,
):
    from deltalake import write_deltalake
    from deltalake.catalog import load_catalog
    from deltalake.exceptions import CatalogConflictError

    _post(unity_catalog_server, "catalogs", {"name": "lifecycle"})
    _post(
        unity_catalog_server,
        "schemas",
        {"name": "default", "catalog_name": "lifecycle"},
    )
    catalog = load_catalog(
        "unity",
        options={
            "workspace_url": unity_catalog_server,
            "access_token": "test-token",
            "allow_http_url": "true",
        },
    )
    identifier = ("lifecycle", "default", "orders")
    location = existing_sample_table.table_uri
    before = DeltaTable(location)

    created = catalog.register_table(identifier, location)
    assert created.status == "created"
    assert created.provider_id
    assert created.delta_id == before.metadata().id
    assert created.table.metadata().id == created.delta_id
    assert created.table.version() == before.version()

    reused = catalog.register_table(identifier, location)
    assert reused.status == "already_registered"
    assert reused.provider_id == created.provider_id
    assert reused.delta_id == created.delta_id

    other = tmp_path / "other"
    write_deltalake(other, sample_table)
    with pytest.raises(CatalogConflictError):
        catalog.register_table(identifier, other)
    loaded = catalog.load_table(identifier)
    assert loaded.metadata().id == created.delta_id
    assert loaded.to_pyarrow_dataset().to_table().to_pydict() == (
        before.to_pyarrow_dataset().to_table().to_pydict()
    )
    assert DeltaTable(location).version() == before.version()
