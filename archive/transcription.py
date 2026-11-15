"""Synthetic-contract-tested adapters, not live vendor E2E. No vendor fallback."""

import re
from datetime import UTC, datetime, timedelta

import httpx

from archive import approvals
from archive.ingest import text
from archive.storage import open_under
from archive.worker import finish, guard

GROQ = "https://api.groq.com/openai/v1/audio/transcriptions"
ASSEMBLY = "https://api.assemblyai.com/v2"
SMALL = 20_000_000


def upload_type(row):
    if row["kind"] == "voice":
        return "audio.ogg", "audio/ogg"
    if row["kind"] in {"video", "video_note"}:
        return "video.mp4", "video/mp4"
    types = {
        "audio/mpeg": "mp3",
        "audio/mp3": "mp3",
        "audio/mp4": "m4a",
        "audio/x-m4a": "m4a",
        "audio/ogg": "ogg",
        "audio/wav": "wav",
        "audio/x-wav": "wav",
        "audio/flac": "flac",
        "audio/webm": "webm",
    }
    extension = types.get(row["mime"])
    return ("audio." + extension, row["mime"]) if extension else None


class ProviderError(RuntimeError):
    pass


def response_json(response):
    if response.status_code != 200:
        raise ProviderError("provider_http_error")
    try:
        result = response.json()
        if not isinstance(result, dict):
            raise ValueError()
        return result
    except ValueError:
        raise ProviderError("provider_response_error") from None


def state(conn, job, status, code=None, **values):
    allowed = {"provider", "submission_id", "uploaded_url", "text"}
    if set(values) - allowed:
        raise ValueError("invalid_transcript_columns")
    with conn.transaction():
        guard(conn, job)
        assignments = ["status=%s", "error_code=%s"] + [k + "=%s" for k in values]
        conn.execute(
            "UPDATE transcripts SET " + ",".join(assignments) + " WHERE attachment_id=%s",
            (status, code, *values.values(), job["attachment_id"]),
        )


def complete(conn, cfg, job, row, value):
    """Persist transcript text only if the chat still collects at commit.

    The approval row is held (approvals.fenced) in the same transaction, so a Stop,
    removal, decline or re-pending committed while the provider worked discards the
    text: the transcript ends blocked/chat_disabled and the job completes.
    """
    chat = row["chat_id"]
    with conn.transaction():
        guard(conn, job)
        if (
            approvals.fenced(conn, cfg, chat)
            and cfg.transcription_enabled
            and cfg.transcription_consent(chat)
        ):
            conn.execute(
                "UPDATE transcripts SET status='ready',error_code=NULL,text=%s "
                "WHERE attachment_id=%s",
                (value, job["attachment_id"]),
            )
        else:
            conn.execute(
                "UPDATE transcripts SET status='blocked',error_code='chat_disabled',text=NULL "
                "WHERE attachment_id=%s",
                (job["attachment_id"],),
            )
            conn.execute("UPDATE jobs SET error_code='chat_disabled' WHERE id=%s", (job["id"],))
        finish(conn, job)


def defer_poll(conn, job):
    with conn.transaction():
        guard(conn, job)
        conn.execute(
            """UPDATE jobs SET kind='transcript_poll',status='pending',attempts=0,
            next_run=now()+interval '30 seconds',lease_until=NULL WHERE id=%s""",
            (job["id"],),
        )


def process(conn, cfg, job, heartbeat, transport=None):
    row = conn.execute(
        """SELECT a.*,t.status AS transcript_status,t.submission_id,t.uploaded_url
        FROM attachments a JOIN transcripts t ON t.attachment_id=a.id WHERE a.id=%s""",
        (job["attachment_id"],),
    ).fetchone()
    if not (
        approvals.collecting(conn, cfg, row["chat_id"])
        and cfg.transcription_enabled
        and cfg.transcription_consent(row["chat_id"])
    ):
        state(conn, job, "blocked", "consent_required")
        finish(conn, job)
        return
    small = row["size"] <= SMALL
    key = cfg.groq_key if small else cfg.assemblyai_key
    if not key:
        state(conn, job, "blocked", "key_required")
        finish(conn, job)
        return
    if row["transcript_status"] in {"submitting", "reconciliation_required"}:
        state(conn, job, "reconciliation_required", "submission_uncertain")
        finish(conn, job)
        return
    if row["transcript_status"] == "ready":
        finish(conn, job)
        return
    if datetime.now(UTC) - job["created_at"] >= timedelta(hours=24):
        state(conn, job, "failed", "provider_deadline")
        finish(conn, job)
        return
    try:
        with httpx.Client(
            transport=transport,
            trust_env=False,
            follow_redirects=False,
            timeout=httpx.Timeout(60, connect=5),
        ) as client:
            if small:
                media_type = upload_type(row)
                if media_type is None:
                    state(conn, job, "blocked", "unsupported_format")
                    finish(conn, job)
                    return
                with open_under(cfg.media_root, str(cfg.media_root / row["relative_path"])) as file:
                    state(conn, job, "submitting", provider="groq")
                    result = response_json(
                        client.post(
                            GROQ,
                            headers={"Authorization": "Bearer " + key},
                            data={"model": "whisper-large-v3", "response_format": "json"},
                            files={"file": (media_type[0], file, media_type[1])},
                        )
                    )
                heartbeat.check()
                complete(conn, cfg, job, row, text(result["text"]))
                return
            headers = {"Authorization": key}
            submission = row["submission_id"]
            if not submission:
                uploaded = row["uploaded_url"]
                if not uploaded:
                    with open_under(
                        cfg.media_root, str(cfg.media_root / row["relative_path"])
                    ) as file:
                        result = response_json(
                            client.post(
                                ASSEMBLY + "/upload",
                                headers={**headers, "Content-Type": "application/octet-stream"},
                                content=chunks(file, heartbeat),
                            )
                        )
                    uploaded = result["upload_url"]
                    if not isinstance(uploaded, str) or not uploaded.startswith(
                        "https://cdn.assemblyai.com/"
                    ):
                        raise ProviderError("provider_response_error")
                    state(conn, job, "uploaded", provider="assemblyai", uploaded_url=uploaded)
                # Persist before request: uncertainty requires reconciliation, never resubmission.
                state(conn, job, "submitting", provider="assemblyai")
                result = response_json(
                    client.post(
                        ASSEMBLY + "/transcript",
                        headers=headers,
                        json={
                            "audio_url": uploaded,
                            "speech_models": ["universal-2"],
                            "language_detection": True,
                        },
                    )
                )
                submission = result["id"]
                if not isinstance(submission, str) or not re.fullmatch(
                    r"[a-zA-Z0-9-]{1,128}", submission
                ):
                    raise ProviderError("provider_response_error")
                state(conn, job, "polling", submission_id=submission)
                defer_poll(conn, job)
                return
            result = response_json(
                client.get(ASSEMBLY + "/transcript/" + submission, headers=headers)
            )
            heartbeat.check()
            if result.get("status") == "completed":
                complete(conn, cfg, job, row, text(result["text"]))
            elif result.get("status") == "error":
                state(conn, job, "failed", "provider_failed")
                finish(conn, job)
            elif result.get("status") in {"queued", "processing"}:
                defer_poll(conn, job)
            else:
                raise ProviderError("provider_response_error")
    except (httpx.HTTPError, ProviderError, ValueError, KeyError, TypeError):
        current = conn.execute(
            "SELECT status FROM transcripts WHERE attachment_id=%s", (job["attachment_id"],)
        ).fetchone()["status"]
        if current == "submitting":
            state(conn, job, "reconciliation_required", "submission_uncertain")
            finish(conn, job)
        elif row["submission_id"]:
            # Safe retry of an existing submission ID, never re-submit/fallback.
            with conn.transaction():
                guard(conn, job)
                if job["attempts"] >= cfg.max_attempts:
                    state(conn, job, "failed", "provider_poll_failed")
                    finish(conn, job)
                else:
                    conn.execute(
                        """UPDATE jobs SET status='pending',lease_until=NULL,
                        next_run=now()+interval '60 seconds',error_code='provider_poll_error'
                        WHERE id=%s""",
                        (job["id"],),
                    )
        else:
            state(conn, job, "failed", "provider_error")
            finish(conn, job)


def chunks(file, heartbeat):
    while chunk := file.read(1024 * 1024):
        heartbeat.check()
        yield chunk
