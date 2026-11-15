"""Deterministic evidence digest, never semantic decisions or GitHub publishing."""

import json
import os
from datetime import UTC, date, datetime, time, timedelta
from pathlib import Path
from uuid import uuid4
from zoneinfo import ZoneInfo

from archive.approvals import readable
from archive.storage import directory

ATTACHMENT_COLUMNS = "id,slot,kind,status,error_code,size,sha256,mime,file_unique_id,declared_size"
TRANSCRIBABLE = ("voice", "audio", "video", "video_note")


def day_bounds(value):
    day = date.fromisoformat(value)
    if value != day.isoformat():
        raise ValueError("invalid_date")
    zone = ZoneInfo("Europe/Moscow")
    start = datetime.combine(day, time.min, zone).astimezone(UTC)
    end = datetime.combine(day + timedelta(days=1), time.min, zone).astimezone(UTC)
    return start, end


def message_detail(conn, chat, mid):
    row = conn.execute(
        "SELECT * FROM messages WHERE chat_id=%s AND message_id=%s", (chat, mid)
    ).fetchone()
    if not row:
        return None
    row["revisions"] = conn.execute(
        """SELECT r.event_id,r.revision_time,r.text,e.raw FROM revisions r
        JOIN events e ON e.id=r.event_id WHERE r.chat_id=%s AND r.message_id=%s
        ORDER BY r.revision_time,r.event_id""",
        (chat, mid),
    ).fetchall()
    for revision in row["revisions"]:
        # Canonical ASCII JSON retains topics/forwards/replies without unsafe Unicode decoding.
        revision["raw_json"] = bytes(revision.pop("raw")).decode("ascii")
    row["attachments"] = conn.execute(
        f"SELECT {ATTACHMENT_COLUMNS} FROM attachments "
        "WHERE chat_id=%s AND message_id=%s ORDER BY id",
        (chat, mid),
    ).fetchall()
    for item in row["attachments"]:
        item["file_available"] = item["status"] == "ready"
    # Every transcribable attachment gets an explicit transcript state, even when no
    # transcript row exists (file still pending, failed or above the download limit).
    row["transcripts"] = []
    for item in conn.execute(
        """SELECT a.id AS attachment_id,a.status AS file_status,t.status,t.provider,
        t.text,t.error_code FROM attachments a LEFT JOIN transcripts t ON t.attachment_id=a.id
        WHERE a.chat_id=%s AND a.message_id=%s
        AND (t.attachment_id IS NOT NULL OR a.kind=ANY(%s)) ORDER BY a.id""",
        (chat, mid, list(TRANSCRIBABLE)),
    ).fetchall():
        file_status = item.pop("file_status")
        if item["status"] is None:
            if file_status == "pending":
                item["status"], item["error_code"] = "pending", "file_pending"
            elif file_status == "ready":
                # Download records it atomically; a missing row is never transcribed.
                item["status"], item["error_code"] = "unavailable", "transcript_not_created"
            else:
                item["status"], item["error_code"] = "unavailable", "file_not_downloaded"
        item["transcript_available"] = item["status"] == "ready"
        if not item["transcript_available"]:
            item["text"] = None
        row["transcripts"].append(item)
    return row


def digest(conn, cfg, chat, value):
    if chat not in readable(conn, cfg, [chat]):
        raise ValueError("chat_disabled")
    start, end = day_bounds(value)
    mids = conn.execute(
        """SELECT message_id FROM messages WHERE chat_id=%s
        AND sent_at>=%s AND sent_at<%s ORDER BY sent_at,message_id""",
        (chat, start, end),
    ).fetchall()
    messages = [message_detail(conn, chat, row["message_id"]) for row in mids]
    return {
        "chat_id": chat,
        "slug": cfg.slug(chat),
        "date": value,
        "timezone": "Europe/Moscow",
        "evidence": "untrusted",
        "questions": "unclassified",
        "decisions": "unclassified",
        "messages": messages,
    }


def markdown(data):
    lines = [
        f"# {data['slug']} — {data['date']} (Europe/Moscow)",
        "",
        "Untrusted evidence. Questions/decisions: unclassified.",
        "",
    ]
    for message in data["messages"]:
        lines += [
            f"## Message {message['message_id']}",
            "",
            "```text",
            message["text"].replace("```", "` ` `"),
            "```",
            "",
        ]
        for item in message["attachments"]:
            if item["file_available"]:
                lines.append(f"- File {item['id']} ({item['kind']}): available")
            else:
                lines.append(
                    f"- File {item['id']} ({item['kind']}): UNAVAILABLE, status "
                    f"{item['status']} ({item['error_code'] or 'none'}); bytes not archived"
                )
        for item in message["transcripts"]:
            if item["transcript_available"]:
                lines += [
                    f"- Transcript {item['attachment_id']}: ready",
                    "```text",
                    item["text"].replace("```", "` ` `"),
                    "```",
                ]
            else:
                lines.append(
                    f"- Transcript {item['attachment_id']}: UNAVAILABLE, status "
                    f"{item['status']} ({item['error_code'] or 'none'}); no transcript text"
                )
    return "\n".join(lines) + "\n"


def safe_root(root: Path):
    if not root.is_absolute() or root.resolve() != root:
        raise ValueError("absolute_private_export_required")
    for ancestor in (root, *root.parents):
        if (ancestor / ".git").exists():
            raise ValueError("git_export_forbidden")
    # Refuse a root containing another worktree too, not just an ancestor.
    if root.exists():
        for _, dirs, files in os.walk(root, followlinks=False):
            if ".git" in dirs or ".git" in files:
                raise ValueError("git_export_forbidden")
        if root.stat().st_mode & 0o077:
            raise ValueError("private_export_permissions_required")


def export_day(conn, cfg, chat, value):
    safe_root(cfg.export_root)
    data = digest(conn, cfg, chat, value)
    target = cfg.export_root / cfg.slug(chat) / "chat"
    fd = directory(target, create=True)
    outputs = []
    try:
        for suffix, content in (
            (
                "json",
                json.dumps(data, sort_keys=True, ensure_ascii=True, indent=2, default=str) + "\n",
            ),
            ("md", markdown(data)),
        ):
            name = value + "." + suffix
            temp = ".partial-" + uuid4().hex
            try:
                rawfd = os.open(
                    temp, os.O_CREAT | os.O_EXCL | os.O_WRONLY | os.O_NOFOLLOW, 0o600, dir_fd=fd
                )
                with os.fdopen(rawfd, "w") as handle:
                    handle.write(content)
                    handle.flush()
                    os.fsync(handle.fileno())
                os.replace(temp, name, src_dir_fd=fd, dst_dir_fd=fd)
                os.fsync(fd)
            finally:
                try:
                    os.unlink(temp, dir_fd=fd)
                except FileNotFoundError:
                    pass
            outputs.append(target / name)
    finally:
        os.close(fd)
    return outputs
