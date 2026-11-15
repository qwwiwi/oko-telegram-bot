"""Owner decisions racing in-flight work: publication, migrated chats, implicit leaves."""

import os
import threading
import time
from contextlib import contextmanager
from dataclasses import replace

import psycopg
import pytest
from test_approvals import OWNER, Bot, confirmed, member, tap
from test_ingest import update
from test_migration import NEW, OLD, from_message, row, seed, to_message
from test_publish_fence import voice_cfg

from archive import approvals
from archive import worker as worker_module
from archive.db import connect
from archive.ingest import Ingestor
from archive.worker import Worker


class FileBot(Bot):
    """Owner-facing calls recorded like test_approvals.Bot, plus a cloud download."""

    def get_file(self, file_id):
        return "voice/" + file_id, None

    @contextmanager
    def download(self, path, maximum):
        yield iter([b"a" * 10, b"b" * 10])


def files(cfg):
    return [p for p in cfg.media_root.rglob("*") if p.is_file()]


def try_stop(dsn, chat):
    """The owner's Stop from the poller's connection; False if it must wait."""
    with connect(dsn) as other:
        other.execute("SET lock_timeout='300ms'")
        try:
            with other.transaction():
                approvals.decide(other, chat, "stop", OWNER)
        except psycopg.errors.LockNotAvailable:
            return False
    return True


def on_link(monkeypatch, action):
    real = os.link

    def link(*args, **kwargs):
        action()
        return real(*args, **kwargs)

    monkeypatch.setattr(os, "link", link)


def test_stop_waits_until_publication_is_referenced(db, cfg, monkeypatch):
    cfg = voice_cfg(db, cfg)
    with Ingestor(db, cfg) as ing:
        ing.ingest(update(voice={"file_id": "v"}))
    outcome = []
    on_link(monkeypatch, lambda: outcome.append(try_stop(db.info.dsn, -10)))
    assert Worker(db, cfg, FileBot()).once()
    # The approval row is held from the check through the hardlink and the commit.
    assert outcome == [False]
    row_ = db.execute("SELECT * FROM attachments").fetchone()
    assert row_["status"] == "ready" and (cfg.media_root / row_["relative_path"]).is_file()
    assert db.execute("SELECT status FROM jobs WHERE kind='download'").fetchone()["status"] == (
        "done"
    )
    assert try_stop(db.info.dsn, -10)


def test_failure_after_hardlink_leaves_no_unreferenced_file(db, cfg, monkeypatch):
    cfg = voice_cfg(db, cfg)
    with Ingestor(db, cfg) as ing:
        ing.ingest(update(voice={"file_id": "v"}))

    def crash(conn, job):
        raise RuntimeError("synthetic_crash_after_link")

    monkeypatch.setattr(worker_module, "finish", crash)
    assert Worker(db, cfg, FileBot()).once()
    assert not files(cfg)
    assert db.execute("SELECT status FROM attachments").fetchone()["status"] == "pending"


def migrated_voice(db, cfg):
    cfg = replace(cfg, dsn=db.info.dsn)
    seed(db, "active")
    with Ingestor(db, cfg) as ing:
        ing.ingest(update(1, mid=5, chat=OLD, text="", voice={"file_id": "v"}))
        ing.ingest(to_message(2))
        ing.ingest(from_message(3))
    assert row(db, NEW)["status"] == "active"
    return cfg


def run_download(db, cfg):
    worker = Worker(db, cfg, FileBot())
    job = db.execute("SELECT id FROM jobs WHERE kind='download'").fetchone()
    db.execute("UPDATE jobs SET next_run=now()+interval '1 hour' WHERE id<>%s", (job["id"],))
    assert worker.once()
    return db.execute("SELECT * FROM attachments").fetchone()


def test_stop_of_supergroup_revokes_queued_work_of_old_group(db, cfg):
    cfg = migrated_voice(db, cfg)
    with Ingestor(db, cfg) as ing:
        ing.ingest(tap(4, confirmed(db, NEW)))
    assert row(db, NEW)["status"] == "stopped"
    assert not approvals.collecting(db, cfg, OLD)
    attachment = run_download(db, cfg)
    assert (attachment["status"], attachment["error_code"]) == ("blocked", "chat_disabled")
    assert not files(cfg)


def test_queued_work_of_old_group_completes_while_supergroup_active(db, cfg):
    cfg = migrated_voice(db, cfg)
    assert approvals.collecting(db, cfg, OLD)
    assert run_download(db, cfg)["status"] == "ready"


def test_supergroup_stop_waits_for_old_group_publication(db, cfg, monkeypatch):
    cfg = migrated_voice(db, cfg)
    outcome = []
    on_link(monkeypatch, lambda: outcome.append(try_stop(db.info.dsn, NEW)))
    assert run_download(db, cfg)["status"] == "ready"
    assert outcome == [False]


def unknown_supergroup_leave(db, cfg):
    cfg = replace(cfg, dsn=db.info.dsn)
    seed(db, "active")
    with Ingestor(db, cfg) as ing:
        ing.ingest(update(1, chat=NEW, text="raced ahead of the migration"))
    assert db.execute("SELECT 1 FROM jobs WHERE kind='leave'").fetchone()
    return cfg


def test_leave_waits_for_concurrent_migration_and_rechecks(db, cfg, monkeypatch):
    cfg = unknown_supergroup_leave(db, cfg)
    bot = Bot()
    claimed, migrating = threading.Event(), threading.Event()
    real = worker_module.claim

    def claim(conn, cfg):
        job = real(conn, cfg)  # the leave is already running when the migration lands
        claimed.set()
        migrating.wait(10)
        return job

    monkeypatch.setattr(worker_module, "claim", claim)
    worker = threading.Thread(target=Worker(db, cfg, bot).once)
    worker.start()
    assert claimed.wait(10)
    with connect(db.info.dsn) as poller:
        with poller.transaction():
            assert approvals.migrate(poller, OLD, NEW, "New Super")
            migrating.set()
            time.sleep(0.5)
            assert worker.is_alive(), "leave ran without waiting for the migration"
        worker.join(10)
    assert not worker.is_alive()
    assert "leaveChat" not in bot.methods()
    assert row(db, NEW)["status"] == "active"
    assert not db.execute("SELECT 1 FROM jobs WHERE kind='leave'").fetchone()


def test_migration_waits_for_leave_in_flight(db, cfg):
    cfg = unknown_supergroup_leave(db, cfg)
    outcome = []

    class LeavingBot(Bot):
        def call(self, method, params=None):
            if method == "leaveChat":
                with connect(db.info.dsn) as poller:
                    poller.execute("SET lock_timeout='300ms'")
                    try:
                        with poller.transaction():
                            approvals.migrate(poller, OLD, NEW, "New Super")
                        outcome.append("migrated")
                    except psycopg.errors.LockNotAvailable:
                        outcome.append("waited")
            return super().call(method, params)

    assert Worker(db, cfg, LeavingBot()).once()
    assert outcome == ["waited"]


@pytest.mark.parametrize("status", ["active", "stopped", "declined"])
def test_unanswered_supergroup_request_takes_group_decision(db, cfg, status):
    cfg = replace(cfg, dsn=db.info.dsn)
    seed(db, status)
    with Ingestor(db, cfg) as ing:
        # The bot is seen in the new supergroup before either migration message.
        ing.ingest(member(1, chat=NEW, title="New Super"))
        assert row(db, NEW)["status"] == "pending"
        ing.ingest(to_message(2))
        ing.ingest(from_message(3))
    new = row(db, NEW)
    assert new["status"] == status
    if status in ("active", "stopped"):
        assert new["approved_by"] == OWNER and new["approved_at"] is not None


@pytest.mark.parametrize("decision", ["stopped", "declined"])
def test_explicit_supergroup_decision_is_kept_on_migration(db, cfg, decision):
    cfg = replace(cfg, dsn=db.info.dsn)
    seed(db, "active")
    with Ingestor(db, cfg) as ing:
        ing.ingest(member(1, chat=NEW, title="New Super"))
        ing.ingest(tap(2, f"p:no:{NEW}"))
        if decision == "stopped":
            ing.ingest(tap(3, f"p:yes:{NEW}"))
            ing.ingest(tap(4, confirmed(db, NEW)))
        assert row(db, NEW)["status"] == decision
        ing.ingest(to_message(5))
        ing.ingest(from_message(6))
    assert row(db, NEW)["status"] == decision
    assert not approvals.collecting(db, cfg, OLD)


class Interleaved:
    """Worker connection that runs ``after_discovery`` once the first link read returns."""

    def __init__(self, conn, after_discovery):
        self.conn, self.after_discovery, self.reads = conn, after_discovery, 0

    def __getattr__(self, name):
        return getattr(self.conn, name)

    def execute(self, query, params=None):
        result = self.conn.execute(query, params)
        if query.startswith("SELECT migrated_to"):
            self.reads += 1
            self.after_discovery(self.reads)
        return result


def waiting_or_done(db, pid, thread):
    for _ in range(100):
        if not thread.is_alive():
            return
        state = db.execute(
            "SELECT wait_event_type FROM pg_stat_activity WHERE pid=%s", (pid,)
        ).fetchone()
        if state and state["wait_event_type"] == "Lock":
            return
        time.sleep(0.05)
    raise AssertionError("duplicate migration neither waited nor finished")


def test_fence_retry_releases_locks_before_duplicate_migration(db, cfg):
    cfg = replace(cfg, dsn=db.info.dsn)
    seed(db, "active")
    duplicate, errors = connect(db.info.dsn), []

    def migrate_again():
        try:
            with duplicate.transaction():
                assert approvals.migrate(duplicate, OLD, NEW, "New Super")
        except Exception as exc:
            errors.append(exc)

    thread = threading.Thread(target=migrate_again)

    def after_discovery(reads):
        if reads == 1:
            # The migration commits between the link read and the locking read.
            with connect(db.info.dsn) as poller, poller.transaction():
                assert approvals.migrate(poller, OLD, NEW, "New Super")
        elif reads == 2:
            # The other half of the pair locks the supergroup row, then the old one.
            thread.start()
            waiting_or_done(db_check, duplicate.info.backend_pid, thread)

    with connect(db.info.dsn) as db_check, duplicate:
        worker = Interleaved(db, after_discovery)
        try:
            with db.transaction():
                assert approvals.fenced(worker, cfg, OLD)
        except Exception as exc:
            errors.append(exc)
        thread.join(10)
    assert not thread.is_alive()
    assert errors == []
    assert worker.reads == 2
    assert row(db, OLD)["migrated_to"] == NEW and row(db, NEW)["status"] == "active"
