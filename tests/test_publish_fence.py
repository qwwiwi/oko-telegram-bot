"""Approval fence at final publication: a chat stopped mid-flight keeps nothing new."""

import os
from contextlib import contextmanager
from dataclasses import replace

import httpx
import pytest
from test_ingest import update
from test_transcription import setup_audio

from archive import worker as worker_module
from archive.ingest import Ingestor
from archive.worker import Worker

REVOKED = ("stopped", "removed", "declined", "pending")


def revoke(db, status, chat=-10):
    db.execute("UPDATE chat_approvals SET status=%s WHERE chat_id=%s", (status, chat))


class RacingBot:
    """Cloud download whose chat is revoked by the owner while bytes are streaming."""

    def __init__(self, db, status):
        self.db, self.status = db, status

    def get_file(self, file_id):
        return "voice/" + file_id, None

    @contextmanager
    def download(self, path, maximum):
        def chunks():
            yield b"a" * 10
            revoke(self.db, self.status)
            yield b"b" * 10

        yield chunks()


def voice_cfg(db, cfg):
    return replace(
        cfg,
        dsn=db.info.dsn,
        transcription_enabled=True,
        chats={**cfg.chats, -10: replace(cfg.chats[-10], transcription=True)},
        groq_key="synthetic-groq",
    )


def assert_discarded(db, cfg):
    row = db.execute("SELECT * FROM attachments").fetchone()
    assert (row["status"], row["error_code"]) == ("blocked", "chat_disabled")
    assert row["relative_path"] is None and row["sha256"] is None
    job = db.execute("SELECT * FROM jobs WHERE kind='download'").fetchone()
    assert (job["status"], job["error_code"]) == ("done", "chat_disabled")
    assert not [p for p in cfg.media_root.rglob("*") if p.is_file()]
    assert not db.execute("SELECT 1 FROM transcripts").fetchone()
    assert not db.execute("SELECT 1 FROM jobs WHERE kind='transcribe'").fetchone()


@pytest.mark.parametrize("status", REVOKED)
def test_download_revoked_before_publication_is_discarded(db, cfg, status):
    cfg = voice_cfg(db, cfg)
    with Ingestor(db, cfg) as ing:
        ing.ingest(update(voice={"file_id": "v"}))
    assert Worker(db, cfg, RacingBot(db, status)).once()
    assert_discarded(db, cfg)


@pytest.mark.parametrize("status", REVOKED)
def test_download_revoked_right_before_publication_is_never_linked(db, cfg, status, monkeypatch):
    cfg = voice_cfg(db, cfg)
    real = worker_module.renew
    links = []

    renewals = []

    def renew_then_revoke(conn, cfg_, job):
        real(conn, cfg_, job)
        renewals.append(job["kind"])
        if renewals.count("download") == 2:
            # The fence's renewal (bytes complete), before the locked approval check.
            revoke(db, status)

    def link(*args, **kwargs):
        links.append(args)
        raise AssertionError("revoked file must never be linked")

    monkeypatch.setattr(worker_module, "renew", renew_then_revoke)
    monkeypatch.setattr(os, "link", link)
    with Ingestor(db, cfg) as ing:
        ing.ingest(update(voice={"file_id": "v"}))
    assert Worker(db, cfg, RacingBot(db, "active")).once()
    assert renewals.count("download") == 2 and not links
    assert_discarded(db, cfg)


def test_download_still_active_at_publication_is_kept(db, cfg):
    cfg = voice_cfg(db, cfg)
    with Ingestor(db, cfg) as ing:
        ing.ingest(update(voice={"file_id": "v"}))
    assert Worker(db, cfg, RacingBot(db, "active")).once()
    row = db.execute("SELECT * FROM attachments").fetchone()
    assert row["status"] == "ready" and (cfg.media_root / row["relative_path"]).is_file()


@pytest.mark.parametrize("status", REVOKED)
def test_groq_transcript_revoked_before_persistence_is_discarded(db, cfg, status):
    cfg, worker = setup_audio(db, cfg)

    def transport(request):
        revoke(db, status)
        return httpx.Response(200, json={"text": "DO_NOT_RETAIN"})

    worker.transcription_transport = httpx.MockTransport(transport)
    assert worker.once()
    row = db.execute("SELECT * FROM transcripts").fetchone()
    assert (row["status"], row["error_code"], row["text"]) == ("blocked", "chat_disabled", None)
    job = db.execute("SELECT * FROM jobs WHERE kind='transcribe'").fetchone()
    assert (job["status"], job["error_code"]) == ("done", "chat_disabled")


@pytest.mark.parametrize("status", REVOKED)
def test_assembly_transcript_revoked_before_persistence_is_discarded(db, cfg, status):
    cfg, worker = setup_audio(db, cfg, 20_000_001)

    def transport(request):
        if request.url.path.endswith("upload"):
            return httpx.Response(200, json={"upload_url": "https://cdn.assemblyai.com/u/f"})
        if request.method == "POST":
            return httpx.Response(200, json={"id": "synthetic-id", "status": "queued"})
        revoke(db, status)
        return httpx.Response(200, json={"status": "completed", "text": "DO_NOT_RETAIN"})

    worker.transcription_transport = httpx.MockTransport(transport)
    assert worker.once()
    db.execute("UPDATE jobs SET next_run=now() WHERE kind='transcript_poll'")
    assert worker.once()
    row = db.execute("SELECT * FROM transcripts").fetchone()
    assert (row["status"], row["error_code"], row["text"]) == ("blocked", "chat_disabled", None)
    job = db.execute("SELECT * FROM jobs WHERE kind='transcript_poll'").fetchone()
    assert (job["status"], job["error_code"]) == ("done", "chat_disabled")
