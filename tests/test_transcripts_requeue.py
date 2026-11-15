from dataclasses import replace

import httpx
import pytest
from cloud import CloudFiles
from test_ingest import update
from test_transcription import setup_audio

from archive import cli, transcripts
from archive.db import enqueue_transcription
from archive.ingest import Ingestor
from archive.transcripts import RequeueCounts, requeue
from archive.worker import Worker


def archived(db, cfg, chats=(-10,)):
    cfg = replace(cfg, dsn=db.info.dsn)
    cloud = CloudFiles(cfg, {"voice": b"synthetic audio"})
    with Ingestor(db, cfg) as ing:
        for uid, chat in enumerate(chats, 1):
            ing.ingest(update(uid, uid, chat, voice={"file_id": "voice"}))
    worker = Worker(db, cfg, cloud.bot())
    for _ in chats:
        assert worker.once()
    return cfg, worker


def consent(cfg):
    return replace(
        cfg,
        transcription_enabled=True,
        chats={chat: replace(value, transcription=True) for chat, value in cfg.chats.items()},
        groq_key="synthetic-groq",
    )


def snapshot(db):
    return [
        db.execute(f"SELECT * FROM {table} ORDER BY 1").fetchall()
        for table in ("transcripts", "jobs", "attachments", "chat_approvals")
    ]


@pytest.mark.parametrize("global_consent,chat_consent", [(False, True), (True, False)])
def test_consent_off_changes_nothing(db, cfg, global_consent, chat_consent):
    cfg, _ = archived(db, cfg)
    cfg = replace(
        consent(cfg),
        transcription_enabled=global_consent,
        chats={-10: replace(cfg.chats[-10], transcription=chat_consent)},
    )
    before = snapshot(db)
    assert requeue(db, cfg) == RequeueCounts(requeued=0)
    assert snapshot(db) == before


def test_consent_on_requeues_and_transcribes_idempotently(db, cfg):
    cfg, worker = archived(db, cfg)
    worker.cfg = cfg = consent(cfg)
    assert requeue(db, cfg) == RequeueCounts(requeued=1)
    assert db.execute("SELECT status,error_code FROM transcripts").fetchone() == {
        "status": "pending",
        "error_code": None,
    }
    before = snapshot(db)
    assert requeue(db, cfg) == RequeueCounts(requeued=0)
    assert snapshot(db) == before
    calls = []

    def provider(request):
        calls.append(request)
        return httpx.Response(200, json={"text": "synthetic recovered transcript"})

    worker.transcription_transport = httpx.MockTransport(provider)
    assert worker.once()
    assert not worker.once()
    assert db.execute("SELECT status,text FROM transcripts").fetchone() == {
        "status": "ready",
        "text": "synthetic recovered transcript",
    }
    assert len(calls) == 1
    assert requeue(db, cfg) == RequeueCounts(requeued=0)


def test_key_required_renews_existing_job_instead_of_duplicate(db, cfg):
    cfg, worker = setup_audio(db, cfg)
    worker.cfg = replace(cfg, groq_key="")
    assert worker.once()
    assert db.execute("SELECT error_code FROM transcripts").fetchone() == {
        "error_code": "key_required"
    }
    job = db.execute("SELECT * FROM jobs WHERE kind='transcribe'").fetchone()
    db.execute(
        """UPDATE jobs SET attempts=5,created_at=now()-interval '2 days',
        next_run=now()+interval '1 day',error_code='old_error' WHERE id=%s""",
        (job["id"],),
    )
    assert requeue(db, cfg) == RequeueCounts(requeued=1)
    retried = db.execute("SELECT * FROM jobs WHERE kind='transcribe'").fetchone()
    assert retried["id"] == job["id"]
    assert retried["status"] == "pending" and retried["attempts"] == 0
    assert retried["error_code"] is retried["lease_token"] is retried["lease_until"] is None
    assert retried["created_at"] >= job["created_at"]
    worker.cfg = cfg
    worker.transcription_transport = httpx.MockTransport(
        lambda request: httpx.Response(200, json={"text": "recovered key"})
    )
    assert worker.once()
    assert db.execute("SELECT status FROM transcripts").fetchone()["status"] == "ready"
    assert requeue(db, cfg) == RequeueCounts(requeued=0)


@pytest.mark.parametrize("blocked_by", ["key", "consent"])
def test_submitted_then_blocked_is_never_submitted_again(db, cfg, blocked_by):
    cfg, worker = setup_audio(db, cfg, 20_000_001)
    calls = []

    def provider(request):
        calls.append((request.method, request.url.path))
        if request.url.path == "/v2/upload":
            return httpx.Response(200, json={"upload_url": "https://cdn.assemblyai.com/upload/f"})
        assert request.method == "POST" and request.url.path == "/v2/transcript"
        return httpx.Response(200, json={"id": "synthetic-id", "status": "queued"})

    worker.transcription_transport = httpx.MockTransport(provider)
    assert worker.once()
    assert db.execute("SELECT submission_id FROM transcripts").fetchone() == {
        "submission_id": "synthetic-id"
    }
    worker.cfg = (
        replace(cfg, assemblyai_key="")
        if blocked_by == "key"
        else replace(cfg, transcription_enabled=False)
    )
    db.execute("UPDATE jobs SET next_run=now() WHERE kind='transcript_poll'")
    assert worker.once()
    assert db.execute("SELECT status,error_code FROM transcripts").fetchone() == {
        "status": "blocked",
        "error_code": "key_required" if blocked_by == "key" else "consent_required",
    }
    worker.cfg = cfg
    before = snapshot(db)
    for dry_run in (True, False):
        assert requeue(db, cfg, dry_run=dry_run) == RequeueCounts(skipped_submitted=1)
        assert snapshot(db) == before
    assert not worker.once()
    assert calls == [("POST", "/v2/upload"), ("POST", "/v2/transcript")]


@pytest.mark.parametrize("field", ["submission_id", "uploaded_url", "provider"])
@pytest.mark.parametrize("value", ["synthetic-metadata", ""])
def test_any_submission_metadata_prevents_requeue(db, cfg, field, value):
    cfg, _ = archived(db, cfg)
    db.execute(f"UPDATE transcripts SET {field}=%s WHERE chat_id=-10", (value,))
    before = snapshot(db)
    for dry_run in (True, False):
        assert requeue(db, consent(cfg), dry_run=dry_run) == RequeueCounts(skipped_submitted=1)
        assert snapshot(db) == before


@pytest.mark.parametrize(
    "field,value", [("kind", "download"), ("attachment_id", None), ("chat_id", -20)]
)
@pytest.mark.parametrize("job_status", ["done", "failed"])
def test_mismatched_job_is_untouched(db, cfg, field, value, job_status):
    cfg, worker = setup_audio(db, cfg)
    worker.cfg = replace(cfg, groq_key="")
    assert worker.once()
    job = db.execute("SELECT * FROM jobs WHERE kind='transcribe'").fetchone()
    db.execute(f"UPDATE jobs SET {field}=%s,status=%s WHERE id=%s", (value, job_status, job["id"]))
    before = snapshot(db)
    for dry_run in (True, False):
        assert requeue(db, cfg, dry_run=dry_run) == RequeueCounts(skipped_mismatch=1)
        assert snapshot(db) == before
    assert not enqueue_transcription(db, job["attachment_id"], job["chat_id"], retry=True)
    assert snapshot(db) == before


@pytest.mark.parametrize(
    "status,code",
    [
        ("blocked", "unsupported_format"),
        ("blocked", "chat_disabled"),
        ("blocked", "submission_uncertain"),
        ("ready", None),
        ("ready", "consent_required"),
        ("reconciliation_required", "submission_uncertain"),
        ("reconciliation_required", "key_required"),
        ("failed", "consent_required"),
    ],
)
def test_other_states_untouched(db, cfg, status, code):
    cfg, _ = archived(db, cfg)
    db.execute("UPDATE transcripts SET status=%s,error_code=%s WHERE chat_id=-10", (status, code))
    before = snapshot(db)
    assert requeue(db, consent(cfg)) == RequeueCounts(requeued=0)
    assert snapshot(db) == before


@pytest.mark.parametrize("status", ["pending", "stopped", "declined", "removed", "expired"])
def test_non_collecting_chats_untouched(db, cfg, status):
    cfg, _ = archived(db, cfg)
    db.execute("UPDATE chat_approvals SET status=%s WHERE chat_id=-10", (status,))
    before = snapshot(db)
    assert requeue(db, consent(cfg)) == RequeueCounts(requeued=0)
    assert snapshot(db) == before


def test_config_disabled_chat_untouched(db, cfg):
    cfg, _ = archived(db, cfg)
    cfg = consent(cfg)
    cfg = replace(cfg, chats={-10: replace(cfg.chats[-10], enabled=False)})
    before = snapshot(db)
    assert requeue(db, cfg) == RequeueCounts(requeued=0)
    assert snapshot(db) == before


@pytest.mark.parametrize("path", [None, "missing.ogg", "../outside.ogg", "symlink.ogg"])
def test_missing_or_unsafe_attachment_untouched(db, cfg, path):
    cfg, _ = archived(db, cfg)
    (cfg.media_root / "symlink.ogg").symlink_to("missing.ogg")
    db.execute("UPDATE attachments SET relative_path=%s WHERE chat_id=-10", (path,))
    before = snapshot(db)
    assert requeue(db, consent(cfg)) == RequeueCounts(requeued=0)
    assert snapshot(db) == before


@pytest.mark.parametrize("job_status", ["pending", "running"])
def test_existing_active_job_untouched(db, cfg, job_status):
    cfg, worker = setup_audio(db, cfg)
    worker.cfg = replace(cfg, groq_key="")
    assert worker.once()
    db.execute("UPDATE jobs SET status=%s WHERE kind='transcribe'", (job_status,))
    before = snapshot(db)
    assert requeue(db, cfg) == RequeueCounts(requeued=0)
    assert snapshot(db) == before


def test_cli_chat_filter_dry_run_and_counts_only(db, cfg, monkeypatch, capsys):
    cfg, _ = archived(db, cfg, (-10, -20))
    cfg = consent(cfg)
    monkeypatch.setattr(cli, "load", lambda path: cfg)

    def run(*args):
        monkeypatch.setattr(
            "sys.argv",
            ["oko", "--config", "synthetic.toml", "transcripts", "requeue", *args],
        )
        cli.main()
        output = capsys.readouterr()
        assert output.err == ""
        return output.out

    before = snapshot(db)
    assert run("--dry-run") == "requeued 2, skipped_submitted 0, skipped_mismatch 0\n"
    assert (
        run("--chat", "-10", "--dry-run") == "requeued 1, skipped_submitted 0, skipped_mismatch 0\n"
    )
    assert snapshot(db) == before
    assert run("--chat", "-999") == "requeued 0, skipped_submitted 0, skipped_mismatch 0\n"
    assert run("--chat", "-10") == "requeued 1, skipped_submitted 0, skipped_mismatch 0\n"
    assert db.execute("SELECT status FROM transcripts WHERE chat_id=-20").fetchone() == {
        "status": "blocked"
    }
    assert run("--chat", "-10") == "requeued 0, skipped_submitted 0, skipped_mismatch 0\n"
    assert run() == "requeued 1, skipped_submitted 0, skipped_mismatch 0\n"
    assert run() == "requeued 0, skipped_submitted 0, skipped_mismatch 0\n"


def test_requeue_rolls_back_all_candidates_on_failure(db, cfg, monkeypatch):
    cfg, _ = archived(db, cfg, (-10, -20))
    before = snapshot(db)
    enqueue = transcripts.enqueue_transcription
    calls = []

    def fail_second(*args, **kwargs):
        calls.append(1)
        result = enqueue(*args, **kwargs)
        if len(calls) == 2:
            raise RuntimeError("synthetic transaction failure")
        return result

    monkeypatch.setattr(transcripts, "enqueue_transcription", fail_second)
    with pytest.raises(RuntimeError, match="synthetic transaction failure"):
        requeue(db, consent(cfg))
    assert len(calls) == 2
    assert snapshot(db) == before


def test_cli_reports_both_skip_counts(db, cfg, monkeypatch, capsys):
    cfg, _ = archived(db, cfg, (-10, -20))
    cfg = consent(cfg)
    db.execute("UPDATE transcripts SET provider='assemblyai' WHERE chat_id=-10")
    attachment = db.execute("SELECT id FROM attachments WHERE chat_id=-20").fetchone()["id"]
    db.execute(
        """INSERT INTO jobs(dedup_key,kind,attachment_id,chat_id,status)
        VALUES(%s,'download',%s,-20,'done')""",
        (f"transcribe:{attachment}", attachment),
    )
    monkeypatch.setattr(cli, "load", lambda path: cfg)
    before = snapshot(db)
    for args in (["--dry-run"], []):
        monkeypatch.setattr(
            "sys.argv",
            ["oko", "--config", "synthetic.toml", "transcripts", "requeue", *args],
        )
        cli.main()
        output = capsys.readouterr()
        assert output.err == ""
        assert output.out == "requeued 0, skipped_submitted 1, skipped_mismatch 1\n"
        assert snapshot(db) == before
