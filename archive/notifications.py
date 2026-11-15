"""Durable notification reservation and bounded backlog coalescing."""

from datetime import datetime, timedelta

from psycopg.types.json import Jsonb

from archive.worker import finish, guard


def send_digest(conn, cfg, bot, job):
    with conn.transaction():
        guard(conn, job)
        state = conn.execute(
            """SELECT last_attempt,now() AS clock,
            last_attempt>now()-interval '1 hour' AS throttled
            FROM notification_state FOR UPDATE"""
        ).fetchone()
        if state["throttled"]:
            conn.execute(
                """UPDATE jobs SET status='pending',lease_until=NULL,
                next_run=%s+interval '1 hour',attempts=GREATEST(0,attempts-1)
                WHERE id=%s""",
                (state["last_attempt"], job["id"]),
            )
            return
        rows = conn.execute(
            """SELECT id,payload,expires_at FROM jobs WHERE kind='unknown_digest'
            AND (id=%s OR (status='pending' AND next_run<=now()))
            FOR UPDATE SKIP LOCKED""",
            (job["id"],),
        ).fetchall()
        expirations = {}
        deadlines = []
        for row in rows:
            deadline = row["expires_at"] or state["clock"] + timedelta(hours=24)
            deadlines.append(deadline)
            entries = row["payload"].get("entries")
            if entries is None:
                entries = [
                    {"id": i, "expires_at": deadline.isoformat()}
                    for i in row["payload"].get("ids", [])
                ]
            for entry in entries:
                identifier = int(entry["id"])
                expiry = datetime.fromisoformat(entry["expires_at"])
                if expiry > state["clock"]:
                    expirations[identifier] = max(expirations.get(identifier, expiry), expiry)
        identifiers = sorted(expirations)
        payload = {
            "ids": identifiers[:100],
            "entries": [
                {"id": i, "expires_at": expirations[i].isoformat()} for i in identifiers[:100]
            ],
            "excess": sum(int(row["payload"].get("excess", 0)) for row in rows)
            + max(0, len(identifiers) - 100),
        }
        conn.execute(
            "UPDATE jobs SET payload=%s,expires_at=%s WHERE id=%s",
            (Jsonb(payload), max(deadlines), job["id"]),
        )
        conn.execute(
            "DELETE FROM jobs WHERE id=ANY(%s) AND id<>%s AND status='pending'",
            ([row["id"] for row in rows], job["id"]),
        )
        # Reserve before HTTP; uncertain attempts wait an hour but retain merged payload.
        conn.execute("UPDATE notification_state SET last_attempt=now()")
    message = "Unknown chats (numeric IDs): " + ", ".join(str(i) for i in payload["ids"])
    message += "; excess=" + str(payload["excess"])
    bot.call("sendMessage", {"chat_id": cfg.founder_id, "text": message})
    finish(conn, job)
