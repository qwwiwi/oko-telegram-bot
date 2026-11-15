import httpx
import pytest
from test_ingest import count, update

from archive.ingest import Ingestor
from archive.poller import poll_once
from archive.telegram import BotAPI, BotError, PollConflict


def test_identity_webhook_conflict_safe_errors(cfg, caplog):
    for result, code in [({"id": 1}, "bot_identity_mismatch"), ({"id": cfg.expected_bot_id}, None)]:

        def handler(req, result=result):
            if req.url.path.endswith("getMe"):
                return httpx.Response(200, json={"ok": True, "result": result})
            return httpx.Response(200, json={"ok": True, "result": {"url": ""}})

        with BotAPI(cfg, httpx.MockTransport(handler)) as bot:
            if code:
                with pytest.raises(BotError, match=code):
                    bot.preflight()
            else:
                bot.preflight()

    def bad(req):
        if req.url.path.endswith("getMe"):
            return httpx.Response(200, json={"ok": True, "result": {"id": cfg.expected_bot_id}})
        return httpx.Response(200, json={"ok": True, "result": {"url": "SENSITIVE"}})

    with BotAPI(cfg, httpx.MockTransport(bad)) as bot:
        with pytest.raises(BotError, match="webhook_present"):
            bot.preflight()
    with BotAPI(cfg, httpx.MockTransport(lambda r: httpx.Response(409, text="SENSITIVE"))) as bot:
        with pytest.raises(PollConflict, match="poll_conflict"):
            bot.updates(0)
    assert "SENSITIVE" not in caplog.text and cfg.bot_token not in caplog.text


def test_poll_contract_atomic_cursor(db, cfg):
    calls = []

    def transport(req):
        import json

        calls.append((req.url.path.rsplit("/", 1)[-1], json.loads(req.content)))
        return httpx.Response(200, json={"ok": True, "result": [update()]})

    with Ingestor(db, cfg) as ing, BotAPI(cfg, httpx.MockTransport(transport)) as bot:
        poll_once(ing, bot)
    assert calls[0][1]["offset"] == 0
    assert calls[0][1]["allowed_updates"] == [
        "message",
        "edited_message",
        "channel_post",
        "edited_channel_post",
        "my_chat_member",
        "callback_query",
    ]
    assert count(db, "messages") == 1
    assert db.execute("SELECT next_offset,last_poll FROM poll_state").fetchone()["last_poll"]


def test_get_file_returns_validated_relative_cloud_path(cfg):
    results = iter(
        [
            {"file_path": "documents/file_1.pdf", "file_size": 5},
            {"file_path": "/var/lib/telegram-bot-api/token/documents/file_1.pdf"},
            {"file_path": "documents/../../etc/passwd"},
            {"file_path": "https://example.invalid/file"},
            {"file_path": "documents//file"},
            {"file_path": "documents/file", "file_size": -1},
            {"file_path": "documents/file", "file_size": "5"},
        ]
    )

    def handler(req):
        assert req.url.scheme == "https" and req.url.host == "api.telegram.org"
        assert req.extensions["timeout"]["read"] <= 60
        return httpx.Response(200, json={"ok": True, "result": next(results)})

    with BotAPI(cfg, httpx.MockTransport(handler)) as bot:
        assert bot.get_file("synthetic") == ("documents/file_1.pdf", 5)
        for _ in range(6):
            with pytest.raises(BotError, match="invalid_file_result"):
                bot.get_file("synthetic")
