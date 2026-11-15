from dataclasses import replace
from datetime import UTC, datetime, timedelta

import httpx
import pytest
from fastapi.testclient import TestClient
from psycopg.types.json import Jsonb
from test_ingest import update
from test_transcription import setup_audio

import archive.db as database
from archive.api import create_app
from archive.db import enqueue, migrate
from archive.ingest import Ingestor
from archive.telegram import BotAPI, BotError
from archive.worker import Worker


def test_failed_leaves_cannot_exhaust_exit_capacity_and_kicked_needs_no_exit(db, cfg):
    cfg = replace(cfg, dsn=db.info.dsn, max_attempts=1)
    with Ingestor(db, cfg) as ing:
        for i in range(100):
            ing.ingest(update(i, chat=-9000 - i))

    class FailedBot:
        def call(self, *_):
            raise BotError("synthetic_transient_failure")

    worker = Worker(db, cfg, FailedBot())
    for _ in range(100):
        assert worker.once()
    with Ingestor(db, cfg) as ing:
        ing.ingest(update(100, chat=-20000))
        assert ing.offset == 101
        ing.ingest(
            {
                "update_id": 101,
                "my_chat_member": {
                    "chat": {"id": -30000, "type": "supergroup"},
                    "new_chat_member": {"status": "kicked"},
                },
            }
        )
    assert db.execute("SELECT 1 FROM jobs WHERE kind='leave' AND chat_id=-30000").fetchone() is None


def test_leave_rejection_is_distinct_terminal_result(cfg):
    def handler(req):
        if req.url.path.endswith("getMe"):
            return httpx.Response(200, json={"ok": True, "result": {"id": cfg.expected_bot_id}})
        return httpx.Response(403)

    with BotAPI(cfg, httpx.MockTransport(handler)) as bot:
        with pytest.raises(BotError) as caught:
            bot.call("leaveChat", {"chat_id": -9000})
        assert type(caught.value).__name__ == "LeaveRejected"


def test_coalescing_preserves_fresh_expiry_without_extending_old_identifier_ttl(db, cfg):
    cfg = replace(cfg, dsn=db.info.dsn)
    now = datetime.now(UTC)
    fresh_expiry = now + timedelta(hours=20)
    for key, identifier, expiry in (
        ("old", 1000, now + timedelta(minutes=10)),
        ("fresh", 2000, fresh_expiry),
    ):
        enqueue(
            db,
            key,
            "unknown_digest",
            payload={"ids": [identifier], "excess": 0},
            next_run=now - timedelta(hours=1),
            expires_at=expiry,
        )

    class UncertainBot:
        def call(self, *_):
            raise BotError("synthetic_uncertain_send")

    assert Worker(db, cfg, UncertainBot()).once()
    row = db.execute("SELECT * FROM jobs WHERE kind='unknown_digest'").fetchone()
    assert row["expires_at"] >= fresh_expiry
    assert row["payload"]["ids"] == [1000, 2000]
    for entry in row["payload"]["entries"]:
        if entry["id"] == 1000:
            entry["expires_at"] = (now - timedelta(seconds=1)).isoformat()
    db.execute("UPDATE jobs SET payload=%s WHERE id=%s", (Jsonb(row["payload"]), row["id"]))
    database.cleanup_unknown(db)
    remaining = db.execute("SELECT payload FROM jobs WHERE id=%s", (row["id"],)).fetchone()[
        "payload"
    ]
    assert remaining["ids"] == [2000]
    assert [entry["id"] for entry in remaining["entries"]] == [2000]


def test_upgrade_from_v1_adds_notification_state(db):
    db.execute("DROP TABLE notification_state")
    db.execute("DELETE FROM schema_version WHERE version>1")
    migrate(db)
    assert db.execute("SELECT count(*) AS n FROM notification_state").fetchone()["n"] == 1
    assert db.execute("SELECT max(version) AS v FROM schema_version").fetchone()["v"] >= 2


def test_unauthorized_request_does_not_open_database(cfg, monkeypatch):
    import archive.api as module

    def forbidden(*_):
        raise AssertionError("unauthenticated database access")

    monkeypatch.setattr(module, "connect", forbidden)
    assert TestClient(create_app(cfg)).get("/v1/status").status_code == 401


def test_local_missing_audio_is_not_uncertain_vendor_submission(db, cfg):
    cfg, worker = setup_audio(db, cfg)
    row = db.execute("SELECT relative_path FROM attachments").fetchone()
    (cfg.media_root / row["relative_path"]).unlink()
    worker.transcription_transport = httpx.MockTransport(
        lambda _: pytest.fail("no vendor request for a missing local file")
    )
    worker.once()
    worker.once()
    assert db.execute("SELECT status FROM transcripts").fetchone()["status"] == "failed"
