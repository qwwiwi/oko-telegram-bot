import json
from dataclasses import replace

import httpx
from cloud import CloudFiles
from test_ingest import update

from archive.ingest import Ingestor
from archive.worker import Worker


def setup_audio(db, cfg, size=8):
    cfg = replace(
        cfg,
        dsn=db.info.dsn,
        transcription_enabled=True,
        chats={**cfg.chats, -10: replace(cfg.chats[-10], transcription=True)},
        groq_key="synthetic-groq",
        assemblyai_key="synthetic-assembly",
    )
    cloud = CloudFiles(cfg, {"voice": bytes(size)})
    with Ingestor(db, cfg) as ing:
        ing.ingest(update(voice={"file_id": "voice", "file_size": size}))
    worker = Worker(db, cfg, cloud.bot())
    worker.once()
    return cfg, worker


def test_groq_contract_and_no_fallback(db, cfg, caplog):
    cfg, worker = setup_audio(db, cfg)
    calls = []

    def transport(req):
        calls.append(req)
        assert req.url == "https://api.groq.com/openai/v1/audio/transcriptions"
        assert req.headers["authorization"] == "Bearer synthetic-groq"
        assert "multipart/form-data" in req.headers["content-type"]
        assert b"whisper-large-v3" in req.content and b'name="file"' in req.content
        assert b'filename="audio.ogg"' in req.content and b"audio/ogg" in req.content
        return httpx.Response(200, json={"text": "synthetic transcript"})

    worker.transcription_transport = httpx.MockTransport(transport)
    assert worker.once()
    row = db.execute("SELECT * FROM transcripts").fetchone()
    assert row["status"] == "ready" and row["text"] == "synthetic transcript"
    assert len(calls) == 1
    assert "synthetic-groq" not in caplog.text


def test_assembly_upload_submit_durable_poll(db, cfg):
    cfg, worker = setup_audio(db, cfg, 20_000_001)
    calls = []

    def transport(req):
        calls.append(req.url.path)
        assert req.headers["authorization"] == "synthetic-assembly"
        if req.url.path.endswith("upload"):
            assert len(req.content) == 20_000_001
            assert req.headers["content-type"] == "application/octet-stream"
            return httpx.Response(200, json={"upload_url": "https://cdn.assemblyai.com/upload/f"})
        if req.method == "POST":
            assert json.loads(req.content)["audio_url"] == "https://cdn.assemblyai.com/upload/f"
            assert json.loads(req.content)["language_detection"] is True
            assert json.loads(req.content)["speech_models"] == ["universal-2"]
            return httpx.Response(200, json={"id": "synthetic-id", "status": "queued"})
        assert req.url.path.endswith("/synthetic-id")
        return httpx.Response(200, json={"status": "completed", "text": "large transcript"})

    worker.transcription_transport = httpx.MockTransport(transport)
    worker.once()
    row = db.execute("SELECT * FROM transcripts").fetchone()
    assert row["submission_id"] == "synthetic-id" and row["status"] == "polling"
    assert calls == ["/v2/upload", "/v2/transcript"]
    db.execute("UPDATE jobs SET next_run=now() WHERE kind='transcript_poll'")
    worker.once()
    assert db.execute("SELECT text FROM transcripts").fetchone()["text"] == "large transcript"
    assert calls == ["/v2/upload", "/v2/transcript", "/v2/transcript/synthetic-id"]


def test_uncertain_submission_never_auto_rebill(db, cfg):
    cfg, worker = setup_audio(db, cfg)
    calls = []

    def timeout(req):
        calls.append(req)
        raise httpx.ReadTimeout("SENSITIVE_TOKEN_BODY_PATH")

    worker.transcription_transport = httpx.MockTransport(timeout)
    worker.once()
    worker.once()
    assert len(calls) == 1
    row = db.execute("SELECT status,error_code FROM transcripts").fetchone()
    assert row == {"status": "reconciliation_required", "error_code": "submission_uncertain"}


def test_revoke_and_missing_key_block_egress(db, cfg):
    cfg, worker = setup_audio(db, cfg)
    worker.cfg = replace(cfg, groq_key="")

    def forbidden(req):
        raise AssertionError("egress must remain off")

    worker.transcription_transport = httpx.MockTransport(forbidden)
    worker.once()
    assert db.execute("SELECT status,error_code FROM transcripts").fetchone() == {
        "status": "blocked",
        "error_code": "key_required",
    }
