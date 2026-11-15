"""Cloud Bot API getFile + bounded download: limits, terminal states, no retry storms."""

import hashlib
import json
import logging
import ssl
import subprocess
import threading
from dataclasses import replace
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import httpx
import pytest
from cloud import CloudFiles
from test_ingest import update

from archive.config import CLOUD_DOWNLOAD_LIMIT
from archive.ingest import Ingestor
from archive.telegram import BotAPI, BotError, FileTooBig
from archive.worker import Worker, claim

LIMIT = CLOUD_DOWNLOAD_LIMIT


def rows(db, sql):
    return db.execute(sql).fetchall()


def leaked(db, token):
    dump = []
    for table in ("events", "attachments", "transcripts", "jobs", "poll_state"):
        dump.append(json.dumps(rows(db, f"SELECT * FROM {table}"), default=str))
    return token in "".join(dump)


class Redirector(httpx.BaseTransport):
    """Sends api.telegram.org traffic to a local TLS server, keeping SNI and Host."""

    def __init__(self, port, cafile):
        context = ssl.create_default_context(cafile=str(cafile))
        self.inner = httpx.HTTPTransport(verify=context)
        self.port = port

    def handle_request(self, request):
        assert request.url.scheme == "https" and request.url.host == "api.telegram.org"
        request.url = request.url.copy_with(host="127.0.0.1", port=self.port)
        request.extensions = {**request.extensions, "sni_hostname": "api.telegram.org"}
        return self.inner.handle_request(request)

    def close(self):
        self.inner.close()


@pytest.fixture
def https_cloud(tmp_path, cfg):
    """Real local HTTPS server with a certificate for api.telegram.org."""
    cert, key = tmp_path / "cert.pem", tmp_path / "key.pem"
    subprocess.run(
        [
            "openssl",
            "req",
            "-x509",
            "-newkey",
            "rsa:2048",
            "-nodes",
            "-days",
            "1",
            "-subj",
            "/CN=api.telegram.org",
            "-addext",
            "subjectAltName=DNS:api.telegram.org",
            "-keyout",
            str(key),
            "-out",
            str(cert),
        ],
        check=True,
        capture_output=True,
    )
    files = {"documents/file_7.pdf": b"synthetic cloud bytes" * 1000}
    seen = []

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args):
            return

        def reply(self, status, body=b"", headers=()):
            self.send_response(status)
            for name, value in headers:
                self.send_header(name, value)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def do_POST(self):
            body = json.loads(self.rfile.read(int(self.headers["Content-Length"])) or b"{}")
            method = self.path.rsplit("/", 1)[-1]
            seen.append(("POST", method))
            if method == "getFile":
                path = {"good": "documents/file_7.pdf", "moved": "documents/moved"}.get(
                    body["file_id"]
                )
                result = {"file_path": path}
                self.reply(200, json.dumps({"ok": True, "result": result}).encode())
            else:
                self.reply(404)

        def do_GET(self):
            prefix = "/file/bot" + cfg.bot_token + "/"
            assert self.path.startswith(prefix)
            name = self.path[len(prefix) :]
            seen.append(("GET", name))
            if name == "documents/moved":
                self.reply(302, headers=[("Location", "https://127.0.0.1:1/elsewhere")])
            else:
                self.reply(200, files[name])

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.load_cert_chain(cert, key)
    server.socket = context.wrap_socket(server.socket, server_side=True)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield Redirector(server.server_address[1], cert), files, seen
    finally:
        server.shutdown()
        server.server_close()


def test_real_https_relative_file_path_hash_and_no_redirects(db, cfg, https_cloud, caplog):
    caplog.set_level(logging.DEBUG)
    transport, files, seen = https_cloud
    cfg = replace(cfg, dsn=db.info.dsn)
    with Ingestor(db, cfg) as ing:
        ing.ingest(update(document={"file_id": "good", "file_unique_id": "u-good"}))
        ing.ingest(update(2, mid=2, document={"file_id": "moved", "file_unique_id": "u-moved"}))
    with BotAPI(cfg, transport) as bot:
        worker = Worker(db, cfg, bot)
        assert worker.once() and worker.once()
    content = files["documents/file_7.pdf"]
    good, moved = rows(db, "SELECT * FROM attachments ORDER BY id")
    assert good["status"] == "ready" and good["size"] == len(content)
    assert good["sha256"] == hashlib.sha256(content).hexdigest()
    assert (cfg.media_root / good["relative_path"]).read_bytes() == content
    assert not list(cfg.media_root.rglob(".partial-*"))
    # A redirect is an error with backoff, never followed to another host.
    assert moved["status"] == "pending"
    job = db.execute("SELECT * FROM jobs WHERE attachment_id=%s", (moved["id"],)).fetchone()
    assert job["status"] == "pending" and job["error_code"] == "bot_error"
    later = db.execute("SELECT next_run>now() AS later FROM jobs WHERE id=%s", (job["id"],))
    assert later.fetchone()["later"]
    assert ("GET", "documents/moved") in seen and len(seen) == 4
    assert cfg.bot_token not in caplog.text and not leaked(db, cfg.bot_token)


def ingest_and_drain(db, cfg, cloud, **document):
    with Ingestor(db, cfg) as ing:
        ing.ingest(update(document={"file_unique_id": "unique", **document}))
    worker = Worker(db, cfg, cloud.bot())
    while worker.once():
        pass
    return db.execute("SELECT * FROM attachments").fetchone()


@pytest.mark.parametrize("declared", [True, False])
def test_exactly_twenty_megabytes_is_archived(db, cfg, declared):
    cfg = replace(cfg, dsn=db.info.dsn)
    content = b"\x01" * LIMIT
    cloud = CloudFiles(cfg, {"edge": content}, report_size=declared)
    extra = {"file_size": LIMIT} if declared else {}
    row = ingest_and_drain(db, cfg, cloud, file_id="edge", **extra)
    assert row["status"] == "ready" and row["size"] == LIMIT == 20 * 1024 * 1024
    assert row["sha256"] == hashlib.sha256(content).hexdigest()


@pytest.mark.parametrize("chunked", [False, True])
def test_one_byte_over_limit_without_declared_size(db, cfg, chunked):
    cfg = replace(cfg, dsn=db.info.dsn)
    content = b"\x02" * (LIMIT + 1)
    cloud = CloudFiles(cfg, {"big": content}, report_size=False)
    if chunked:
        # No Content-Length: the streaming counter must stop at limit+1.
        handler = cloud.handler

        def streaming(request):
            response = handler(request)
            if request.method == "GET":
                return httpx.Response(200, content=iter([content[:LIMIT], content[LIMIT:]]))
            return response

        cloud.handler = streaming
    row = ingest_and_drain(db, cfg, cloud, file_id="big")
    assert row["status"] == "oversized_not_downloaded"
    assert row["relative_path"] is None and row["sha256"] is None and row["size"] is None
    assert not [p for p in cfg.media_root.rglob("*") if p.is_file()]
    assert cloud.count("getFile") == 1


def test_declared_one_byte_over_limit_never_calls_telegram(db, cfg):
    cfg = replace(cfg, dsn=db.info.dsn)
    cloud = CloudFiles(cfg, {})
    row = ingest_and_drain(db, cfg, cloud, file_id="declared", file_size=LIMIT + 1)
    assert row["status"] == "oversized_not_downloaded" and row["declared_size"] == LIMIT + 1
    assert cloud.calls == []


def test_missing_size_rejection_survives_retry_replay_restart(db, cfg):
    cfg = replace(cfg, dsn=db.info.dsn)
    cloud = CloudFiles(cfg, {}, too_big={"huge-voice"})
    voice = update(voice={"file_id": "huge-voice", "file_unique_id": "huge-unique"})
    with Ingestor(db, cfg) as ing:
        ing.ingest(voice)
    # Crash after claim: the expired lease is reclaimed and still ends terminal.
    claim(db, cfg)
    db.execute("UPDATE jobs SET lease_until=now()-interval '1 second'")
    worker = Worker(db, cfg, cloud.bot())
    assert worker.once()
    row = db.execute("SELECT * FROM attachments").fetchone()
    assert row["status"] == "oversized_not_downloaded"
    assert row["error_code"] == "cloud_download_limit"
    assert (row["file_id"], row["file_unique_id"]) == ("huge-voice", "huge-unique")
    assert row["declared_size"] is None and row["relative_path"] is None
    transcript = db.execute("SELECT * FROM transcripts").fetchone()
    assert (transcript["status"], transcript["error_code"]) == (
        "unavailable",
        "file_not_downloaded",
    )
    assert transcript["text"] is None
    job = db.execute("SELECT * FROM jobs").fetchone()
    assert job["status"] == "done" and job["error_code"] == "oversized_not_downloaded"
    assert not db.execute("SELECT 1 FROM jobs WHERE kind='transcribe'").fetchone()
    # Telegram replay and a process restart: no new rows, jobs or Telegram calls.
    with Ingestor(db, cfg) as restarted:
        restarted.ingest(voice)
    restarted_worker = Worker(db, cfg, cloud.bot())
    for _ in range(5):
        assert not restarted_worker.once()
    assert cloud.count("getFile") == 1
    assert db.execute("SELECT count(*) AS n FROM attachments").fetchone()["n"] == 1
    assert db.execute("SELECT count(*) AS n FROM jobs").fetchone()["n"] == 1
    assert db.execute("SELECT text FROM messages").fetchone()["text"] == "synthetic"
    assert db.execute("SELECT count(*) AS n FROM revisions").fetchone()["n"] == 1


def test_other_getfile_errors_back_off_and_end_failed(db, cfg):
    cfg = replace(cfg, dsn=db.info.dsn, max_attempts=2)
    cloud = CloudFiles(cfg, {})  # unknown file_id: generic 400, not the size limit
    with Ingestor(db, cfg) as ing:
        ing.ingest(update(document={"file_id": "gone"}))
    worker = Worker(db, cfg, cloud.bot())
    assert worker.once()
    job = db.execute("SELECT *,next_run>now() AS later FROM jobs").fetchone()
    assert job["status"] == "pending" and job["later"], "retry must back off"
    assert not worker.once(), "no immediate retry storm"
    db.execute("UPDATE jobs SET next_run=now()")
    assert worker.once()
    assert db.execute("SELECT status FROM jobs").fetchone()["status"] == "failed"
    row = db.execute("SELECT * FROM attachments").fetchone()
    assert row["status"] == "failed" and row["file_id"] == "gone"
    assert cloud.count("getFile") == 2


def test_transport_errors_never_carry_token_bearing_urls(cfg, caplog):
    caplog.set_level(logging.DEBUG)

    def broken(request):
        raise httpx.ConnectError("synthetic failure for " + str(request.url), request=request)

    with BotAPI(cfg, httpx.MockTransport(broken)) as bot:
        for action in (
            lambda: bot.get_file("x"),
            lambda: bot.download("documents/file_1", 10).__enter__(),
        ):
            with pytest.raises(BotError) as caught:
                action()
            assert cfg.bot_token not in str(caught.value)
            assert caught.value.__cause__ is None and caught.value.__suppress_context__
    assert cfg.bot_token not in caplog.text


def test_download_rejects_unsafe_paths_and_encodings(cfg):
    def handler(request):
        return httpx.Response(200, content=b"x", headers={"Content-Encoding": "gzip"})

    with BotAPI(cfg, httpx.MockTransport(handler)) as bot:
        for path in ("/abs/file", "../x", "a/../b", "a//b", "https://x/y", "a/b?c", ""):
            with pytest.raises(BotError, match="invalid_file_result"):
                with bot.download(path, 10):
                    pass
        with pytest.raises(BotError, match="file_response_error"):
            with bot.download("documents/file_1", 10):
                pass


def test_download_rejects_declared_content_length_before_reading(cfg):
    reads = []

    def body():
        reads.append(1)
        yield b"x" * 11

    def handler(request):
        return httpx.Response(200, content=body(), headers={"Content-Length": "11"})

    with BotAPI(cfg, httpx.MockTransport(handler)) as bot:
        # The header alone refuses the file: entering the download already fails.
        with pytest.raises(FileTooBig):
            bot.download("documents/file_1", 10).__enter__()
        with bot.download("documents/file_1", 11) as chunks:
            assert b"".join(chunks) == b"x" * 11


def test_download_stops_at_running_total_without_content_length(cfg):
    def handler(request):
        return httpx.Response(200, content=iter([b"x" * 6, b"y" * 5, b"z" * 100]))

    received = []
    with BotAPI(cfg, httpx.MockTransport(handler)) as bot:
        with pytest.raises(FileTooBig):
            with bot.download("documents/file_1", 10) as chunks:
                for chunk in chunks:
                    received.append(chunk)
    # Nothing beyond the limit is ever handed to storage.
    assert sum(map(len, received)) <= 10
