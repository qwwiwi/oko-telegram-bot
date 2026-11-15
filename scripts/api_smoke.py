"""Actual localhost process + real DB/file smoke; upstream transport is synthetic."""

import hashlib
import json
import os
import socket
import subprocess
import sys
import time
from pathlib import Path

import httpx

from archive.approvals import activate
from archive.config import load
from archive.db import connect
from archive.ingest import Ingestor
from archive.poller import poll_once
from archive.telegram import BotAPI
from archive.worker import Worker


def main():
    root = Path(os.environ["ARCHIVE_DISPOSABLE_TEST_ROOT"]) / "smoke"
    root.mkdir(mode=0o700)
    dsn = os.environ["ARCHIVE_DISPOSABLE_TEST_DSN"]
    token = "s" * 32
    config = root / "synthetic.toml"
    os.environ["ARCHIVE_DATABASE_DSN"] = dsn
    # Concatenated so the source holds no token-shaped literal; synthetic, never real.
    os.environ["ARCHIVE_BOT_TOKEN"] = "1234567890:" + "synthetic_smoke_" + "y" * 19
    config.write_text(f'''
expected_bot_id = 1234567890
owner_tg_id = 1000000001
founder_id = 999
media_root = {json.dumps(str(root / "media"))}
export_root = {json.dumps(str(root / "output"))}
[chats."-10"]
slug = "smoke"
[agents.smoke]
sha256 = "{hashlib.sha256(token.encode()).hexdigest()}"
chats = [-10]
files = true
rate_per_minute = 120
''')
    config.chmod(0o600)
    base = [sys.executable, "-m", "archive", "--config", str(config)]
    subprocess.run([*base, "migrate"], check=True)
    cfg = load(config)
    content = b"synthetic localhost file smoke\n"
    update = {
        "update_id": 700,
        "message": {
            "chat": {"id": -10, "type": "supergroup"},
            "message_id": 700,
            "date": 1700000000,
            "text": "synthetic smoke text",
            "document": {"file_id": "synthetic-file", "file_size": len(content)},
            "voice": {"file_id": "synthetic-big-voice", "file_unique_id": "big-unique"},
        },
    }

    def transport(req):
        assert req.url.scheme == "https" and req.url.host == "api.telegram.org"
        if req.url.path == "/file/bot" + cfg.bot_token + "/documents/file_1.txt":
            return httpx.Response(200, content=content)
        method = req.url.path.rsplit("/", 1)[-1]
        if method == "getFile" and json.loads(req.content)["file_id"] == "synthetic-big-voice":
            # Missing declared size: Telegram's own cloud-limit rejection.
            return httpx.Response(
                400, json={"ok": False, "description": "Bad Request: file is too big"}
            )
        results = {
            "getMe": {"id": cfg.expected_bot_id},
            "getWebhookInfo": {"url": ""},
            "getUpdates": [update],
            "getFile": {"file_path": "documents/file_1.txt", "file_size": len(content)},
        }
        return httpx.Response(200, json={"ok": True, "result": results[method]})

    with connect(dsn) as conn, BotAPI(cfg, httpx.MockTransport(transport)) as bot:
        conn.execute("""TRUNCATE events,mapping_failures,messages,revisions,attachments,
                     transcripts,jobs,unknown_chats,enrollment_hints,chat_approvals
                     RESTART IDENTITY CASCADE""")
        # Synthetic stand-in for the owner's «Да» (tests/test_approvals.py covers the flow).
        activate(conn, -10, cfg.owner_tg_id)
        bot.preflight()
        with Ingestor(conn, cfg) as ing:
            poll_once(ing, bot)
        worker = Worker(conn, cfg, bot)
        assert worker.once() and worker.once() and not worker.once()
    subprocess.run([*base, "export", "--chat", "-10", "--date", "2023-11-15"], check=True)
    status = json.loads(subprocess.check_output([*base, "status"], text=True))
    assert status["poller"]["last_poll"] is not None
    assert "oldest_pending_job" in status and "smoke text" not in str(status)
    assert status["chats"] == [{"status": "active", "count": 1}]
    assert (cfg.export_root / "smoke/chat/2023-11-15.json").is_file()
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        sock.listen(128)
        port = sock.getsockname()[1]
        child = subprocess.Popen(
            [*base, "api", "--fd", str(sock.fileno())],
            pass_fds=(sock.fileno(),),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        try:
            with httpx.Client(
                base_url=f"http://127.0.0.1:{port}",
                trust_env=False,
                timeout=2,
                headers={"Authorization": "Bearer " + token},
            ) as client:
                for _ in range(100):
                    if child.poll() is not None:
                        raise RuntimeError("api_child_exited")
                    try:
                        if client.get("/health").status_code == 200:
                            break
                    except httpx.HTTPError:
                        pass
                    time.sleep(0.05)
                else:
                    raise RuntimeError("api_not_ready")
                assert client.get("/health").json() == {"status": "ok"}
                assert client.get("/v1/status").json()["messages"] == 1
                assert client.get("/v1/chats").json()[0]["chat_id"] == -10
                assert client.get("/v1/search?q=smoke").json()["items"][0]["message_id"] == 700
                assert client.get("/v1/chats/-10/messages/700").status_code == 200
                file = client.get("/v1/files/1")
                assert file.content == content
                assert file.headers["content-type"] == "application/octet-stream"
                missing = client.get("/v1/files/2")
                assert missing.status_code == 409
                assert missing.json()["status"] == "oversized_not_downloaded"
                detail = client.get("/v1/chats/-10/messages/700").json()
                assert [t["status"] for t in detail["transcripts"]] == ["unavailable"]
                assert detail["attachments"][1]["file_unique_id"] == "big-unique"
                assert client.get("/v1/status").json()["files_oversized_not_downloaded"] == 1
                assert (
                    client.get("/v1/chats/-10/summary?date=2023-11-15").json()["decisions"]
                    == "unclassified"
                )
                assert client.get("/v1/chats/-20/messages").status_code == 404
                with connect(dsn) as conn:
                    audited = conn.execute(
                        "SELECT count(*) AS n FROM read_audit WHERE agent='smoke'"
                    ).fetchone()["n"]
                assert audited >= 10
                assert (
                    client.get(
                        "/v1/status", headers={"Authorization": "Bearer invalid"}
                    ).status_code
                    == 401
                )
        finally:
            child.terminate()
            try:
                stdout, stderr = child.communicate(timeout=10)
            except subprocess.TimeoutExpired:
                child.kill()
                stdout, stderr = child.communicate(timeout=10)
            assert token.encode() not in stdout + stderr
            assert cfg.bot_token.encode() not in stdout + stderr
    pruned = subprocess.check_output([*base, "audit", "prune", "--days", "180"], text=True)
    assert pruned == "pruned 0\n"
    exported = (cfg.export_root / "smoke/chat/2023-11-15.md").read_text()
    assert "oversized_not_downloaded" in exported and "no transcript text" in exported
    assert cfg.bot_token not in exported
    print(
        "PASS: localhost API + scoped reads + streamed file + oversized_not_downloaded"
        " + CLI migrate/export; synthetic cloud Bot API transport"
    )


if __name__ == "__main__":
    main()
