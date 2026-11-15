"""Explicit operator recovery of transcripts blocked before provider submission."""

from dataclasses import dataclass

from archive import approvals
from archive.db import enqueue_transcription
from archive.storage import StorageError, open_under

REQUEUE_CODES = ("consent_required", "key_required")


@dataclass
class RequeueCounts:
    requeued: int = 0
    skipped_submitted: int = 0
    skipped_mismatch: int = 0


def job_matches(job, attachment_id, chat_id):
    return (
        job["kind"] == "transcribe"
        and job["attachment_id"] == attachment_id
        and job["chat_id"] == chat_id
    )


def requeue(conn, cfg, chat=None, dry_run=False):
    """Return recovery/skip counts, atomically restoring pending state and its job.

    Lock jobs before transcripts, like the worker. Pending/running jobs are left
    alone: a worker may have blocked its transcript but not yet finished its job.
    Approval locks hold the collecting decision through commit, including migrated
    chats. Dry runs take the same locks and perform no data writes.
    """
    counts = RequeueCounts()
    with conn.transaction():
        if not cfg.transcription_enabled:
            return counts
        candidates = conn.execute(
            """SELECT attachment_id,chat_id FROM transcripts
            WHERE status='blocked' AND error_code=ANY(%s)
            AND (%s::bigint IS NULL OR chat_id=%s) ORDER BY attachment_id""",
            (list(REQUEUE_CODES), chat, chat),
        ).fetchall()
        for candidate in candidates:
            attachment, chat_id = candidate["attachment_id"], candidate["chat_id"]
            if not cfg.transcription_consent(chat_id):
                continue
            job = conn.execute(
                """SELECT status,kind,attachment_id,chat_id FROM jobs
                WHERE dedup_key=%s FOR UPDATE""",
                (f"transcribe:{attachment}",),
            ).fetchone()
            if not approvals.fenced(conn, cfg, chat_id):
                continue
            row = conn.execute(
                """SELECT t.status,t.error_code,t.submission_id,t.uploaded_url,t.provider,
                a.status AS attachment_status,a.relative_path
                FROM transcripts t JOIN attachments a ON a.id=t.attachment_id
                WHERE t.attachment_id=%s FOR UPDATE OF t,a""",
                (attachment,),
            ).fetchone()
            if not row or row["status"] != "blocked" or row["error_code"] not in REQUEUE_CODES:
                continue
            if any(
                row[field] is not None for field in ("submission_id", "uploaded_url", "provider")
            ):
                counts.skipped_submitted += 1
                continue
            if job and not job_matches(job, attachment, chat_id):
                counts.skipped_mismatch += 1
                continue
            if job and job["status"] in {"pending", "running"}:
                continue
            if row["attachment_status"] != "ready" or not row["relative_path"]:
                continue
            try:
                with open_under(cfg.media_root, str(cfg.media_root / row["relative_path"])):
                    pass
            except StorageError:
                continue
            if not dry_run:
                if not enqueue_transcription(conn, attachment, chat_id, retry=True):
                    # A conflicting job may have been inserted since the first lookup.
                    job = conn.execute(
                        """SELECT kind,attachment_id,chat_id FROM jobs
                        WHERE dedup_key=%s FOR UPDATE""",
                        (f"transcribe:{attachment}",),
                    ).fetchone()
                    if job and not job_matches(job, attachment, chat_id):
                        counts.skipped_mismatch += 1
                    continue
                conn.execute(
                    """UPDATE transcripts SET status='pending',error_code=NULL,text=NULL
                    WHERE attachment_id=%s""",
                    (attachment,),
                )
            counts.requeued += 1
    return counts
