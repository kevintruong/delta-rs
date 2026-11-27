import json
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from threading import Thread
from urllib.parse import parse_qs, urlsplit

import pytest

from deltalake.catalog import load_catalog
from deltalake.exceptions import CatalogOutcomeUnknownError, TableNotFoundError


class CatalogHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def reply(self, status, body=b"", content_type="application/json", headers=None):
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        for key, value in (headers or {}).items():
            self.send_header(key, value)
        self.end_headers()
        if self.command != "HEAD":
            self.wfile.write(body)

    def read_json(self):
        return json.loads(self.rfile.read(int(self.headers["Content-Length"])))

    def log_message(self, *args):
        pass


@contextmanager
def serve_catalog(handler):
    with ThreadingHTTPServer(("127.0.0.1", 0), handler) as server:
        thread = Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            yield f"http://127.0.0.1:{server.server_port}"
        finally:
            server.shutdown()
            thread.join(timeout=5)


def test_catalog_configuration_and_identifiers(monkeypatch):
    with pytest.raises(ValueError, match="provider"):
        load_catalog("other")
    with pytest.raises(ValueError, match="unknown Glue catalog option"):
        load_catalog("glue", options={"unrecognized": "value"})

    with monkeypatch.context() as env:
        for key in (
            "DATABRICKS_HOST",
            "DATABRICKS_WORKSPACE_URL",
            "UNITY_WORKSPACE_URL",
        ):
            env.delenv(key, raising=False)
        with pytest.raises(ValueError, match="workspace_url"):
            load_catalog("unity", options={"access_token": "test-token"})
    with pytest.raises(ValueError, match="workspace_url"):
        load_catalog(
            "unity",
            options={"workspace_url": "not a URL", "access_token": "test-token"},
        )

    catalog = load_catalog(
        "unity",
        options={
            "workspace_url": "http://127.0.0.1:1",
            "access_token": "test-token",
            "allow_http_url": "true",
        },
    )
    with pytest.raises(TypeError, match="tuple"):
        catalog.load_table("demo.default.orders")
    with pytest.raises(ValueError, match="identifier"):
        catalog.load_table(("demo", "orders"))
    with pytest.raises(ValueError, match="identifier"):
        catalog.load_table(("demo", "default", ""))


@pytest.mark.timeout(timeout=30, method="thread")
def test_unconfirmed_unity_registration_is_not_replayed(existing_sample_table):
    calls = []

    class Tables(CatalogHandler):
        def do_GET(self):
            calls.append((self.command, self.path))
            body = json.dumps(
                {"error_code": "TABLE_NOT_FOUND", "message": "absent"}
            ).encode()
            self.reply(404, body)

        def do_POST(self):
            calls.append((self.command, self.path))
            self.read_json()
            self.reply(500)

    with serve_catalog(Tables) as endpoint:
        catalog = load_catalog(
            "unity",
            options={
                "workspace_url": endpoint,
                "access_token": "test-token",
                "allow_http_url": "true",
            },
        )
        identifier = ("demo", "default", "orders")
        with pytest.raises(CatalogOutcomeUnknownError, match="unable to confirm"):
            catalog.register_table(identifier, existing_sample_table.table_uri)
        with pytest.raises(TableNotFoundError):
            catalog.load_table(identifier)

    base = "/api/2.1/unity-catalog/tables"
    assert calls == [
        ("GET", f"{base}/demo.default.orders"),
        ("POST", base),
        ("GET", f"{base}/demo.default.orders"),
        ("GET", f"{base}/demo.default.orders"),
    ]


@pytest.mark.timeout(timeout=30, method="thread")
def test_glue_register_then_load_by_name(existing_sample_table, tmp_path, monkeypatch):
    # Serve the committed log through an S3 test endpoint and use real Glue SDK
    # request serialization against the same loopback server.
    log = (tmp_path / "_delta_log/00000000000000000000.json").read_bytes()
    commit_key = "orders/_delta_log/00000000000000000000.json"
    calls = []
    entry = None

    class GlueAndS3(CatalogHandler):
        def do_POST(self):
            nonlocal entry
            operation = self.headers["X-Amz-Target"]
            payload = self.read_json()
            calls.append((operation, payload))
            if operation == "AWSGlue.CreateTable":
                entry = {
                    **payload["TableInput"],
                    "DatabaseName": payload["DatabaseName"],
                    "CatalogId": payload["CatalogId"],
                }
                self.reply(200, b"{}")
            elif operation == "AWSGlue.GetTable" and entry is not None:
                self.reply(200, json.dumps({"Table": entry}).encode())
            elif operation == "AWSGlue.GetTable":
                self.reply(
                    400, b'{"__type":"EntityNotFoundException","Message":"absent"}'
                )
            else:
                self.reply(400, b"unexpected Glue operation")

        def do_HEAD(self):
            self.do_GET()

        def do_GET(self):
            request = urlsplit(self.path)
            query = parse_qs(request.query)
            headers = {
                "ETag": '"test-commit"',
                "Last-Modified": "Fri, 03 Jan 2025 00:00:00 GMT",
            }
            if request.path == f"/catalog-test/{commit_key}":
                byte_range = self.headers.get("Range")
                if byte_range:
                    start, stop = byte_range.removeprefix("bytes=").split("-")
                    last = int(stop) if stop else len(log) - 1
                    headers["Content-Range"] = f"bytes {start}-{last}/{len(log)}"
                    self.reply(206, log[int(start) : last + 1], headers=headers)
                else:
                    self.reply(200, log, headers=headers)
            elif request.path.rstrip("/") == "/catalog-test" and "list-type" in query:
                prefix = query.get("prefix", [""])[0]
                contents = ""
                if commit_key.startswith(prefix):
                    contents = (
                        f"<Contents><Key>{commit_key}</Key>"
                        "<LastModified>2025-01-03T00:00:00.000Z</LastModified>"
                        f'<ETag>"test-commit"</ETag><Size>{len(log)}</Size>'
                        "<StorageClass>STANDARD</StorageClass></Contents>"
                    )
                body = f"<ListBucketResult><IsTruncated>false</IsTruncated>{contents}</ListBucketResult>"
                self.reply(200, body.encode(), content_type="application/xml")
            else:
                self.reply(404)

    with serve_catalog(GlueAndS3) as endpoint:
        for key, value in {
            "AWS_REGION": "us-east-1",
            "AWS_ACCESS_KEY_ID": "test-key",
            "AWS_SECRET_ACCESS_KEY": "test-secret",
            "AWS_EC2_METADATA_DISABLED": "true",
            "AWS_ENDPOINT_URL": endpoint,
            "AWS_ENDPOINT_URL_GLUE": endpoint,
        }.items():
            monkeypatch.setenv(key, value)
        catalog = load_catalog(
            "glue",
            options={"catalog_id": "123456789012"},
            storage_options={
                "AWS_ENDPOINT_URL": endpoint,
                "AWS_ALLOW_HTTP": "true",
            },
        )
        identifier = ("logistics", "orders")
        created = catalog.register_table(identifier, "s3://catalog-test/orders")
        assert created.status == "created"
        assert created.delta_id == existing_sample_table.metadata().id
        assert created.provider_id is None

        reused = catalog.register_table(identifier, "s3://catalog-test/orders")
        assert reused.status == "already_registered"
        assert reused.delta_id == created.delta_id
        loaded = catalog.load_table(identifier)
        assert loaded.metadata().id == created.delta_id
        assert loaded.version() == existing_sample_table.version()

    assert [operation for operation, _ in calls] == [
        "AWSGlue.GetTable",
        "AWSGlue.CreateTable",
        "AWSGlue.GetTable",
        "AWSGlue.GetTable",
        "AWSGlue.GetTable",
    ]
    assert all(payload["CatalogId"] == "123456789012" for _, payload in calls)
