"""Local operator-only status; no tokens, paths, chat identities or client text."""


def status(conn):
    poller = conn.execute("SELECT last_poll,last_commit,failures FROM poll_state").fetchone()
    jobs = conn.execute(
        "SELECT status,count(*) AS count FROM jobs GROUP BY status ORDER BY status"
    ).fetchall()
    oldest = conn.execute(
        "SELECT min(created_at) AS oldest FROM jobs WHERE status IN ('pending','running')"
    ).fetchone()["oldest"]
    files = conn.execute(
        "SELECT status,count(*) AS count FROM attachments GROUP BY status ORDER BY status"
    ).fetchall()
    # Approval-gate counts only: no chat IDs or titles leave the database here.
    chats = conn.execute(
        "SELECT status,count(*) AS count FROM chat_approvals GROUP BY status ORDER BY status"
    ).fetchall()
    return {
        "poller": poller,
        "jobs": jobs,
        "files": files,
        "chats": chats,
        "oldest_pending_job": oldest,
    }
