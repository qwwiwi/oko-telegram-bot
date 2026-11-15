"""Explicit transactions; no silent reconnect or externally supplied test DB."""

from pathlib import Path

import psycopg
from psycopg.rows import dict_row

LOCK = 4120519907318


def connect(dsn):
    return psycopg.connect(
        dsn,
        autocommit=True,
        row_factory=dict_row,
        connect_timeout=5,
        options="-c statement_timeout=30000 -c lock_timeout=5000",
    )


def migrate(conn):
    source = Path(__file__).parent / "migrations"
    if not source.exists():
        source = Path(__file__).parent.parent / "migrations"
    with conn.transaction():
        conn.execute("SELECT pg_advisory_xact_lock(%s)", (LOCK,))
        conn.execute("CREATE TABLE IF NOT EXISTS schema_version(version integer PRIMARY KEY)")
        versions = {r["version"] for r in conn.execute("SELECT version FROM schema_version")}
        for path in sorted(source.glob("*.sql")):
            if int(path.name.split("_")[0]) not in versions:
                conn.execute(path.read_text())


def enqueue(
    conn, key, kind, chat_id=None, attachment_id=None, payload=None, expires_at=None, next_run=None
):
    from psycopg.types.json import Jsonb

    conn.execute(
        """INSERT INTO jobs(dedup_key,kind,chat_id,attachment_id,payload,expires_at,next_run)
           VALUES(%s,%s,%s,%s,%s,%s,COALESCE(%s,now())) ON CONFLICT(dedup_key) DO NOTHING""",
        (key, kind, chat_id, attachment_id, Jsonb(payload or {}), expires_at, next_run),
    )


def enqueue_transcription(conn, attachment_id, chat_id, retry=False):
    """Use the ingest dedup key; retries return whether a matching job was queued."""
    key = f"transcribe:{attachment_id}"
    if retry:
        row = conn.execute(
            """INSERT INTO jobs(dedup_key,kind,chat_id,attachment_id)
            VALUES(%s,'transcribe',%s,%s) ON CONFLICT(dedup_key) DO UPDATE
            SET status='pending',attempts=0,
            next_run=now(),created_at=now(),lease_token=NULL,lease_until=NULL,
            error_code=NULL,expires_at=NULL,payload='{}'::jsonb
            WHERE jobs.kind='transcribe'
            AND jobs.attachment_id=EXCLUDED.attachment_id AND jobs.chat_id=EXCLUDED.chat_id
            AND jobs.status IN ('done','failed') RETURNING id""",
            (key, chat_id, attachment_id),
        ).fetchone()
        return row is not None
    enqueue(conn, key, "transcribe", chat_id, attachment_id)


def cleanup_unknown(conn):
    conn.execute("DELETE FROM jobs WHERE expires_at<=now()")
    conn.execute("DELETE FROM jobs WHERE kind='leave' AND status IN ('failed','done')")
    conn.execute("DELETE FROM unknown_chats WHERE expires_at<=now()")
    conn.execute("""UPDATE jobs SET payload=jsonb_set(payload,'{entries}',COALESCE(
        (SELECT jsonb_agg(entry) FROM jsonb_array_elements(payload->'entries') entry
         WHERE (entry->>'expires_at')::timestamptz>now()),'[]'::jsonb))
        WHERE kind='unknown_digest' AND payload ? 'entries'""")
    conn.execute("""UPDATE jobs SET payload=jsonb_set(payload,'{ids}',COALESCE(
        (SELECT jsonb_agg(entry->'id') FROM jsonb_array_elements(payload->'entries') entry),
        '[]'::jsonb)) WHERE kind='unknown_digest' AND payload ? 'entries'""")
