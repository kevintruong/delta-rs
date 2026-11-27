import json
import os
import shutil
import socket
import subprocess
import sysconfig
import time
import urllib.request
from typing import Generator

import pytest
from arro3.core import Table

from deltalake import DeltaTable


def _api_url(server: str, path: str) -> str:
    return f"{server}/api/2.1/unity-catalog/{path}"


def _post(server: str, path: str, body: dict) -> None:
    request = urllib.request.Request(
        _api_url(server, path),
        data=json.dumps(body).encode(),
        headers={"Content-Type": "application/json"},
    )
    urllib.request.urlopen(request, timeout=10).close()


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
