import json
from dataclasses import replace

import psycopg
import pytest

from archive.db import connect
from archive.ingest import Ingestor, MappingRetry, OwnershipLost, canonical


def update(uid=1, mid=1, chat=-10, text="synthetic", **extra):
    return {
        "update_id": uid,
        "message": {
            "chat": {"id": chat, "type": "supergroup"},
            "message_id": mid,
            "date": 1700000000,
            "from": {"id": 123},
            "text": text,
            **extra,
        },
    }


def count(db, table):
    return db.execute(f"SELECT count(*) AS n FROM {table}").fetchone()["n"]


def test_replay_revisions_idle_reset_and_epoch(db, cfg):
    with Ingestor(db, cfg) as ing:
        u = update()
        ing.ingest(u)
        ing.ingest(u)
        assert count(db, "events") == 1
        edit = update(2, text="new\x00text", edit_date=1700000200)
        edit["edited_message"] = edit.pop("message")
        ing.ingest(edit)
        ing.ingest(update(3, text="older"))
        assert db.execute("SELECT text FROM messages").fetchone()["text"] == "new�text"
        assert count(db, "revisions") == 3
        assert canonical(edit) == bytes(
            db.execute("SELECT raw FROM events WHERE update_id=2").fetchone()["raw"]
        )
        db.execute("UPDATE poll_state SET last_commit=now()-interval '7 days'")
        assert ing.fetch_offset() == 0
        ing.ingest(update(0, mid=2))
        assert ing.offset == 1
        ing.ingest(update(0, mid=3))
        assert count(db, "messages") == 3  # update_id collisions are not fingerprints
        db.execute("UPDATE poll_state SET epoch=epoch+1")
        with pytest.raises(OwnershipLost):
            ing.ingest(update(9))
    with Ingestor(db, cfg) as restored:
        # Cloud Bot API: a new owner fences the old epoch and resumes the durable offset.
        assert restored.epoch == ing.epoch + 2
        assert restored.fetch_offset() == 1


def test_single_poller_and_edit_before_original(db, cfg):
    with Ingestor(db, cfg) as ing, connect(db.info.dsn) as second:
        with pytest.raises(OwnershipLost):
            Ingestor(second, cfg).__enter__()
        ing.ingest(update(1, edit_date=1700000100, text="edit"))
        ing.ingest(update(2, text="original"))
        assert db.execute("SELECT text FROM messages").fetchone()["text"] == "edit"


def test_unknown_no_content_bounded_ttl_private_founder(db, cfg):
    with Ingestor(db, cfg) as ing:
        for i in range(150):
            unknown = update(i, chat=1000 + i, text="DO_NOT_RETAIN")
            unknown["message"]["chat"]["type"] = "private"
            ing.ingest(unknown)
        ing.ingest(update(151, chat=cfg.founder_id))
        assert count(db, "events") == 0
        assert count(db, "unknown_chats") == 100
        assert count(db, "jobs") == 1
        assert "DO_NOT_RETAIN" not in json.dumps(
            db.execute("SELECT * FROM jobs").fetchall(), default=str
        )
        db.execute("UPDATE unknown_chats SET expires_at=now()-interval '1 second'")
        db.execute("UPDATE jobs SET expires_at=now()-interval '1 second'")
        private = update(152, chat=501)
        private["message"]["chat"]["type"] = "private"
        ing.ingest(private)
        rows = db.execute("SELECT kind FROM jobs").fetchall()
        assert rows == [{"kind": "unknown_digest"}]


def test_permanent_quarantine_and_durable_failure_budget(db, cfg):
    def unique_violation(conn, event, body, uid):
        conn.execute("INSERT INTO mapping_failures VALUES('unique',1)")
        conn.execute("INSERT INTO mapping_failures VALUES('unique',1)")

    with Ingestor(db, cfg, mapper=unique_violation) as ing:
        ing.ingest(update(text="\x00raw"))
        assert count(db, "messages") == 0
        assert db.execute("SELECT kind FROM jobs").fetchall() == [{"kind": "founder_alert"}]
        raw = db.execute("SELECT raw,error_code FROM events").fetchone()
        assert b"\\u0000" in bytes(raw["raw"])
        assert raw["error_code"] == "sql_23"
        assert ing.offset == 2

    def broken(*args):
        raise ValueError("sensitive-do-not-log")

    for attempt in range(3):
        with Ingestor(db, cfg, mapper=broken) as ing:
            if attempt < 2:
                with pytest.raises(MappingRetry):
                    ing.ingest(update(2))
                assert ing.offset == 2
            else:
                ing.ingest(update(2))
                assert ing.offset == 3
    assert count(db, "mapping_failures") == 1
    with Ingestor(db, cfg) as ing:
        ing.ingest(update(3, mid=3))
        assert count(db, "messages") == 1


@pytest.mark.parametrize(
    "error",
    [
        psycopg.errors.SerializationFailure,
        psycopg.errors.DiskFull,
        psycopg.errors.AdminShutdown,
        psycopg.errors.ConnectionFailure,
    ],
)
def test_transient_never_ack(db, cfg, error):
    def fail(*args):
        raise error("synthetic")

    with Ingestor(db, cfg, mapper=fail) as ing:
        with pytest.raises(error):
            ing.ingest(update())
        assert ing.offset == 0 and count(db, "events") == 0
        assert count(db, "mapping_failures") == 0


def test_malformed_unknown_and_enrollment(db, cfg):
    with Ingestor(db, cfg) as ing:
        ing.ingest({"update_id": 1, "message": {"text": "not retained"}})
        assert db.execute("SELECT raw FROM events").fetchone()["raw"] is None
        broken = update(2)
        del broken["message"]["message_id"]
        ing.ingest(broken)
        assert db.execute("SELECT error_code FROM events WHERE update_id=2").fetchone()
        ing.ingest(update(3, migrate_to_chat_id=-30))
        assert count(db, "enrollment_hints") == 1
        # The owner's decision follows the chat to its supergroup (never left); agent
        # scopes are not widened: the founder is alerted to enroll the new ID.
        assert db.execute("SELECT status FROM chat_approvals WHERE chat_id=-30").fetchone() == {
            "status": "active"
        }
        alert = db.execute(
            "SELECT payload FROM jobs WHERE dedup_key='migration:-10:-30'"
        ).fetchone()
        assert alert["payload"] == {"code": "pending_enrollment", "old": -10, "new": -30}
        cfg2 = replace(cfg, chats={**cfg.chats, -10: replace(cfg.chats[-10], enabled=False)})
    with Ingestor(db, cfg2) as ing:
        ing.ingest(update(4, text="REVOKED"))
        assert count(db, "messages") == 1


def test_attachment_matrix_no_recursive_reply(db, cfg):
    media = {
        k: {
            "file_id": k,
            "file_unique_id": k,
            "file_size": 12,
            "thumbnail": {"file_id": k + "-thumb"},
        }
        for k in ("document", "voice", "audio", "video", "video_note", "animation", "sticker")
    }
    media["photo"] = [{"file_id": "small"}, {"file_id": "large"}]
    media["reply_to_message"] = update(mid=99)["message"]
    with Ingestor(db, cfg) as ing:
        ing.ingest(update(**media))
    assert count(db, "attachments") == 16
    assert count(db, "messages") == 1
    assert count(db, "jobs") == 16


def test_legacy_sidecar_cursor_is_never_sent_to_cloud(db, cfg):
    db.execute("UPDATE poll_state SET next_offset=900,sidecar_session='legacy-local-session'")
    with Ingestor(db, cfg) as ing:
        assert ing.fetch_offset() == 0
        ing.ingest(update(5))
    state = db.execute("SELECT next_offset,sidecar_session FROM poll_state").fetchone()
    assert state["next_offset"] == 6 and state["sidecar_session"] == "cloud-bot-api"
    with Ingestor(db, cfg) as resumed:
        assert resumed.fetch_offset() == 6
