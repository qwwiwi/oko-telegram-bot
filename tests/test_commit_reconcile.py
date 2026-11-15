"""Uncertain COMMIT after publication: a committed reference never points to a deleted file."""

import asyncio

import psycopg
import pytest
from test_approval_races import FileBot, files, on_link
from test_ingest import update
from test_publish_fence import voice_cfg

from archive import worker as worker_module
from archive.db import connect
from archive.ingest import Ingestor
from archive.storage import CommitUnknown, publish_stream
from archive.worker import Worker


def publish(tmp_path, failure, reconcile):
    def fence(size, checksum, link):
        link()
        raise failure

    with pytest.raises(type(failure)):
        publish_stream(tmp_path / "media", "c/file", iter([b"x"]), 10, fence=fence, **reconcile)
    return (tmp_path / "media" / "c" / "file").is_file()


def test_confirmed_rollback_removes_file(tmp_path):
    def reconcile():
        raise AssertionError("a confirmed rollback needs no reconciliation")

    assert not publish(tmp_path, RuntimeError("rolled_back"), {"reconcile": reconcile})


@pytest.mark.parametrize("referenced", [True, False])
def test_uncertain_commit_follows_committed_reference(tmp_path, referenced):
    kept = publish(tmp_path, CommitUnknown("commit_unknown"), {"reconcile": lambda: referenced})
    assert kept is referenced


def test_uncertain_commit_kept_when_reconciliation_fails(tmp_path, caplog):
    def reconcile():
        raise OSError("reconcile_unreachable")

    assert publish(tmp_path, CommitUnknown("commit_unknown"), {"reconcile": reconcile})
    assert "publish_reconcile_failed" in caplog.text


def test_uncertain_commit_without_reconciler_keeps_file(tmp_path):
    assert publish(tmp_path, CommitUnknown("commit_unknown"), {})


class LosingCommit:
    """Worker connection whose next outer COMMIT (after ``arm``) never acknowledges.

    commit=True: PostgreSQL commits, then the connection error reaches the client.
    commit=False: the transaction is rolled back, then the same error is raised.
    ``error`` replaces the connection error, e.g. an interruption while awaiting COMMIT.
    """

    def __init__(self, conn):
        self.conn, self.depth, self.mode, self.error = conn, 0, None, None

    def __getattr__(self, name):
        return getattr(self.conn, name)

    def arm(self, commit, error=None):
        self.mode, self.error = commit, error

    def transaction(self):
        return Block(self, self.conn.transaction())


class Block:
    def __init__(self, owner, real):
        self.owner, self.real = owner, real

    def __enter__(self):
        self.owner.depth += 1
        return self.real.__enter__()

    def __exit__(self, kind, value, tb):
        self.owner.depth -= 1
        mode, self.owner.mode = self.owner.mode, None
        if kind is not None or self.owner.depth or mode is None:
            self.owner.mode = mode
            return self.real.__exit__(kind, value, tb)
        if mode:
            self.real.__exit__(None, None, None)
        else:
            self.real.__exit__(psycopg.Rollback, psycopg.Rollback(), None)
        lost = psycopg.OperationalError("synthetic: connection lost during COMMIT")
        raise self.owner.error or lost


def download_losing_commit(db, cfg, monkeypatch, commit, after_link=lambda: None, error=None):
    cfg = voice_cfg(db, cfg)
    with Ingestor(db, cfg) as ing:
        ing.ingest(update(voice={"file_id": "v"}))
    conn = LosingCommit(db)

    def armed():
        conn.arm(commit, error)
        after_link()

    on_link(monkeypatch, armed)
    if error is None:
        assert Worker(conn, cfg, FileBot()).once()
    else:
        with pytest.raises(type(error)) as raised:
            Worker(conn, cfg, FileBot()).once()
        assert raised.value is error  # re-raised unchanged
    attachment = db.execute("SELECT * FROM attachments").fetchone()
    job = db.execute("SELECT * FROM jobs WHERE kind='download'").fetchone()
    return cfg, attachment, job


def test_uncertain_commit_with_committed_row_keeps_file(db, cfg, monkeypatch):
    cfg, attachment, job = download_losing_commit(db, cfg, monkeypatch, commit=True)
    assert attachment["status"] == "ready" and job["status"] == "done"
    assert (cfg.media_root / attachment["relative_path"]).is_file()


def test_uncertain_commit_without_committed_row_removes_file(db, cfg, monkeypatch):
    cfg, attachment, job = download_losing_commit(db, cfg, monkeypatch, commit=False)
    assert attachment["status"] == "pending" and attachment["relative_path"] is None
    assert (job["status"], job["error_code"]) == ("pending", "worker_error")
    assert not files(cfg)


@pytest.mark.parametrize("interruption", [KeyboardInterrupt, SystemExit, asyncio.CancelledError])
def test_interrupted_commit_keeps_referenced_file(db, cfg, monkeypatch, interruption):
    cfg, attachment, job = download_losing_commit(
        db, cfg, monkeypatch, commit=True, error=interruption()
    )
    assert attachment["status"] == "ready" and job["status"] == "done"
    assert (cfg.media_root / attachment["relative_path"]).is_file()


def test_interrupted_commit_never_reconciles(db, cfg, monkeypatch):
    def reconnect(dsn):
        raise AssertionError("no reconciliation while shutting down")

    cfg, attachment, _ = download_losing_commit(
        db,
        cfg,
        monkeypatch,
        commit=False,
        after_link=lambda: monkeypatch.setattr(worker_module, "connect", reconnect),
        error=KeyboardInterrupt(),
    )
    assert attachment["status"] == "pending"
    assert len(files(cfg)) == 1  # an orphan, never a dangling reference


def test_interruption_before_commit_removes_file(db, cfg, monkeypatch):
    cfg = voice_cfg(db, cfg)
    with Ingestor(db, cfg) as ing:
        ing.ingest(update(voice={"file_id": "v"}))
    interruption = SystemExit(1)

    def interrupted(conn, job):
        raise interruption  # after link(), before COMMIT is sent: a confirmed rollback

    monkeypatch.setattr(worker_module, "finish", interrupted)
    with pytest.raises(SystemExit) as raised:
        Worker(db, cfg, FileBot()).once()
    assert raised.value is interruption
    attachment = db.execute("SELECT * FROM attachments").fetchone()
    assert attachment["status"] == "pending" and attachment["relative_path"] is None
    assert not files(cfg)


def test_uncertain_commit_kept_when_reconcile_connection_fails(db, cfg, monkeypatch):
    def unreachable(dsn):
        raise psycopg.OperationalError("synthetic: database unreachable")

    def break_reconnect():
        # The heartbeat is already connected; only the reconciliation reconnects.
        monkeypatch.setattr(worker_module, "connect", unreachable)

    cfg, attachment, _ = download_losing_commit(
        db, cfg, monkeypatch, commit=False, after_link=break_reconnect
    )
    assert attachment["status"] == "pending"
    assert len(files(cfg)) == 1  # an orphan, never a dangling reference


def test_reconcile_keeps_file_while_old_backend_is_still_in_transaction(db, cfg):
    cfg = voice_cfg(db, cfg)
    with connect(db.info.dsn) as old, old.transaction():
        # The lost connection's backend has not finished its COMMIT yet.
        assert Worker(db, cfg, FileBot()).referenced(old.info.backend_pid, 1, "unused")
    assert not Worker(db, cfg, FileBot()).referenced(0, 1, "unused")
