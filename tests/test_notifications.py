from dataclasses import replace
from datetime import UTC, datetime, timedelta

import pytest
from test_config import SYNTHETIC_OWNER_ID
from test_ingest import update

from archive.approvals import activate
from archive.db import enqueue
from archive.ingest import Ingestor, MappingRetry
from archive.worker import Worker


class Bot:
    def __init__(self):
        self.calls = []

    def call(self, method, params):
        self.calls.append((method, params))
        return True


def test_notification_saturation_does_not_disable_bounded_group_exit(db, cfg):
    cfg = replace(cfg, dsn=db.info.dsn)
    bot = Bot()
    with Ingestor(db, cfg) as ing:
        for i in range(100):
            value = update(i, chat=1000 + i)
            value["message"]["chat"]["type"] = "private"
            ing.ingest(value)
        ing.ingest(update(100, chat=-9000))
        assert (
            db.execute("SELECT chat_id FROM jobs WHERE kind='leave'").fetchone()["chat_id"] == -9000
        )
        assert Worker(db, cfg, bot).once()
        assert bot.calls[-1] == ("leaveChat", {"chat_id": -9000})
        for i in range(100):
            ing.ingest(update(101 + i, chat=-10000 - i))
        offset = ing.offset
        with pytest.raises(MappingRetry):
            ing.ingest(update(201, chat=-20000))
        assert ing.offset == offset
        assert (
            db.execute("SELECT count(*) AS n FROM jobs WHERE kind='leave'").fetchone()["n"] == 100
        )
        assert Worker(db, cfg, bot).once()
        ing.ingest(update(201, chat=-20000))
        assert ing.offset == 202


def test_pending_leave_respects_new_enrollment(db, cfg):
    cfg = replace(cfg, dsn=db.info.dsn)
    with Ingestor(db, cfg) as ing:
        ing.ingest(update(chat=-9000))
    # The owner approved the chat (bot re-added) before the queued exit ran.
    activate(db, -9000, SYNTHETIC_OWNER_ID)
    bot = Bot()
    assert Worker(db, cfg, bot).once()
    assert not bot.calls


def test_backlogged_digests_share_durable_send_time_and_coalesce(db, cfg):
    cfg = replace(cfg, dsn=db.info.dsn)
    bot = Bot()
    now = datetime.now(UTC)
    for i in range(4):
        enqueue(
            db,
            f"digest:{i}",
            "unknown_digest",
            payload={"ids": [1000 + i], "excess": 0},
            next_run=now - timedelta(hours=4 - i),
            expires_at=now + timedelta(hours=20),
        )
    worker = Worker(db, cfg, bot)
    assert worker.once()
    for _ in range(5):
        worker.once()
    assert len(bot.calls) == 1
    assert all(str(1000 + i) in bot.calls[0][1]["text"] for i in range(4))
    enqueue(db, "digest:new", "unknown_digest", payload={"ids": [2000], "excess": 0})
    # A new worker/process cannot evade the durable send reservation.
    assert Worker(db, cfg, bot).once()
    assert len(bot.calls) == 1
    db.execute("UPDATE notification_state SET last_attempt=now()-interval '61 minutes'")
    db.execute("UPDATE jobs SET next_run=now() WHERE kind='unknown_digest'")
    assert Worker(db, cfg, bot).once()
    assert len(bot.calls) == 2
