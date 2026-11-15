"""Transactional update mapping with durable poison accounting and fenced cursor."""

import hashlib
import json
import logging
from datetime import UTC, datetime, timedelta

import psycopg

from archive import approvals
from archive.db import LOCK, cleanup_unknown, enqueue

KINDS = (
    "message",
    "edited_message",
    "channel_post",
    "edited_channel_post",
    "my_chat_member",
    "callback_query",
)
# Legacy poll_state.sidecar_session column now records the cursor's transport.
CLOUD_CURSOR = "cloud-bot-api"
MEDIA = ("document", "voice", "audio", "video", "video_note", "animation", "sticker")
# Deterministic data errors: 22 data exception, 23 integrity, 54 program limit exceeded.
DETERMINISTIC = {"22", "23", "54"}

log = logging.getLogger(__name__)


class OwnershipLost(RuntimeError):
    pass


class Malformed(ValueError):
    pass


class MappingRetry(RuntimeError):
    pass


def canonical(update):
    return json.dumps(update, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode()


def text(value):
    if not isinstance(value, str):
        raise ValueError("invalid_text")
    return value.replace("\x00", "�").encode("utf-8", "replace").decode()


def integer(value):
    if type(value) is not int or not -(2**63) <= value < 2**63:
        raise ValueError("invalid_integer")
    return value


def body_of(update):
    for key in KINDS:
        if key in update:
            body = update[key]
            return key, body if isinstance(body, dict) else {}
    return None, {}


def chat_of(body):
    chat = body.get("chat")
    if isinstance(chat, dict) and type(chat.get("id")) is int:
        try:
            return integer(chat["id"])
        except ValueError:
            pass
    return None


class Ingestor:
    def __init__(self, conn, cfg, mapper=None):
        self.conn, self.cfg = conn, cfg
        self.mapper = mapper or self.map_message

    def __enter__(self):
        if not self.conn.execute("SELECT pg_try_advisory_lock(%s) AS ok", (LOCK,)).fetchone()["ok"]:
            raise OwnershipLost("poller_already_owned")
        with self.conn.transaction():
            # Cloud Bot API keeps one server-side update queue per bot: a new owner
            # fences the previous epoch and resumes from the durable committed offset.
            state = self.conn.execute("SELECT * FROM poll_state FOR UPDATE").fetchone()
            self.epoch = state["epoch"] + 1
            # One-time switch: a cursor written by a local Bot API server numbers updates
            # independently and must never be sent to the cloud queue (it would confirm
            # unseen updates). offset=0 re-reads everything the cloud queue still holds;
            # updates already archived through the local server may come back under
            # different update_ids, hence different fingerprints, and are stored again as
            # duplicate events/revisions/attachments (messages upsert per chat/message ID).
            # Runbook: "Cloud cursor switch duplicates".
            cloud = state["sidecar_session"] == CLOUD_CURSOR
            self.offset = state["next_offset"] if cloud else 0
            self.conn.execute(
                "UPDATE poll_state SET epoch=%s,next_offset=%s,sidecar_session=%s",
                (self.epoch, self.offset, CLOUD_CURSOR),
            )
        return self

    def __exit__(self, *args):
        if not self.conn.closed:
            self.conn.execute("SELECT pg_advisory_unlock(%s)", (LOCK,))

    def guard(self):
        self.conn.execute("SELECT pg_advisory_xact_lock(%s)", (LOCK,))
        state = self.conn.execute("SELECT * FROM poll_state FOR UPDATE").fetchone()
        if state["epoch"] != self.epoch or state["next_offset"] != self.offset:
            raise OwnershipLost("stale_cursor")
        return state

    def fetch_offset(self):
        state = self.conn.execute("SELECT * FROM poll_state").fetchone()
        if state["epoch"] != self.epoch:
            raise OwnershipLost("stale_cursor")
        if state["last_commit"] and datetime.now(UTC) - state["last_commit"] >= timedelta(days=6):
            return 0
        return self.offset

    def advance(self, uid):
        row = self.conn.execute(
            """UPDATE poll_state SET next_offset=%s,last_commit=now(),
             failures=0 WHERE epoch=%s AND next_offset=%s RETURNING next_offset""",
            (uid + 1, self.epoch, self.offset),
        ).fetchone()
        if not row:
            raise OwnershipLost("stale_cursor")

    def ingest(self, update):
        uid = integer(update["update_id"])
        raw = canonical(update)
        fp = hashlib.sha256(raw).hexdigest()
        kind, body = body_of(update)
        chat = chat_of(body)
        self.prepare(kind, body, chat)
        # Read outside the update transaction: the poller is the only approval writer
        # and processes updates in order, so this cannot change underneath us.
        state = approvals.status(self.conn, chat)
        approved = state == approvals.ACTIVE and self.cfg.allows(chat)
        try:
            with self.conn.transaction():
                self.guard()
                self.cleanup_unknown()
                if kind == "callback_query":
                    approvals.callback(self.conn, self.cfg, body)
                elif chat in (self.cfg.founder_id, self.cfg.owner_tg_id):
                    if kind == "message" and chat == self.cfg.owner_tg_id:
                        approvals.command(self.conn, self.cfg, body, "chats-list:" + fp)
                elif kind == "my_chat_member" and not approved and self.group(body):
                    self.membership_change(chat, body, state, fp)
                elif chat is not None and state is not None and not approved:
                    # Awaiting the owner, declined, stopped or removed: ack and drop.
                    approvals.dropped(self.conn, chat)
                elif chat is not None and not approved:
                    departed = kind == "my_chat_member" and body.get("new_chat_member", {}).get(
                        "status"
                    ) in {"left", "kicked"}
                    info = body.get("chat", {})
                    if departed:
                        pass
                    elif chat in self.cfg.chats and info.get("type") in approvals.GROUPS:
                        # Configured before the approval flow existed (upgrade path): ask
                        # the owner instead of leaving. Nothing from this update is stored.
                        approvals.request(
                            self.conn,
                            chat,
                            info.get("title"),
                            info.get("type"),
                            "approval-configured:" + str(chat),
                            bounded=False,
                        )
                    else:
                        self.unknown(chat, info.get("type"))
                elif not kind:
                    pass
                elif not approved:
                    minimal = hashlib.sha256(
                        canonical({"update_id": uid, "error_code": "missing_chat"})
                    ).hexdigest()
                    self.quarantine(minimal, uid, None, None, "missing_chat")
                else:
                    event = self.conn.execute(
                        """INSERT INTO events(fingerprint,update_id,chat_id,raw)
                        VALUES(%s,%s,%s,%s) ON CONFLICT(fingerprint) DO NOTHING RETURNING id""",
                        (fp, uid, chat, raw),
                    ).fetchone()
                    if event:
                        if kind == "my_chat_member":
                            self.membership(chat, body, fp)
                            if body.get("new_chat_member", {}).get("status") in approvals.DEPARTED:
                                approvals.removed(self.conn, chat)
                        else:
                            if any(type(body.get(k)) is not int for k in ("message_id", "date")):
                                raise Malformed("message_identity")
                            self.mapper(self.conn, event["id"], body, uid)
                self.advance(uid)
        except Malformed:
            self.fallback(fp, uid, chat, raw, "malformed")
        except psycopg.Error as exc:
            cls = (exc.sqlstate or "")[:2]
            if cls not in DETERMINISTIC or not approved:
                raise  # unknown SQL is fail-closed, not poison-budget consumption
            if cls == "54":
                # A limit may depend on server settings: consume the bounded budget first.
                self.budget(fp)
            self.fallback(fp, uid, chat, raw, "sql_" + cls)
        except (OwnershipLost, MappingRetry, MemoryError, OSError):
            raise
        except Exception:
            if not approved:
                raise MappingRetry("unapproved_mapping_error") from None
            self.budget(fp)
            self.fallback(fp, uid, chat, raw, "mapping_permanent")
        self.offset = uid + 1

    def prepare(self, kind, body, chat):
        """Approval bookkeeping that must precede reading this update's chat status.

        Expires stale pending chats and carries a migrating group's decision to its
        supergroup, so the update (and any re-add) is judged on the current state and
        the new supergroup is never treated as unknown and left. Idempotent: a retried
        update repeats it harmlessly; nothing from the update itself is stored here.
        """
        pair = migration(kind, body, chat)
        if pair and chat in (self.cfg.founder_id, self.cfg.owner_tg_id):
            pair = None
        with self.conn.transaction():
            self.guard()
            approvals.expire(self.conn, self.cfg)
            if pair:
                approvals.migrate(self.conn, *pair, body["chat"].get("title"))

    def budget(self, fp):
        """Durable three-attempt poison budget; raises MappingRetry until exhausted."""
        # A separate minimal transaction survives the failed message transaction/restart.
        with self.conn.transaction():
            self.guard()
            attempts = self.conn.execute(
                """INSERT INTO mapping_failures VALUES(%s,1)
                ON CONFLICT(fingerprint) DO UPDATE
                SET attempts=LEAST(3,mapping_failures.attempts+1)
                RETURNING attempts""",
                (fp,),
            ).fetchone()["attempts"]
        if attempts < 3:
            raise MappingRetry("mapping_retry") from None

    def fallback(self, fp, uid, chat, raw, code):
        try:
            with self.conn.transaction():
                self.guard()
                self.quarantine(fp, uid, chat, raw, code)
                self.advance(uid)
        except psycopg.Error as exc:
            if raw is None or (exc.sqlstate or "")[:2] != "54":
                raise  # a failed fallback never acknowledges
            # The raw evidence itself exceeds a server limit: same bounded budget, then
            # a minimal record without the payload so ingestion cannot block forever.
            self.budget(fp)
            log.warning("quarantine_raw_omitted")
            with self.conn.transaction():
                self.guard()
                self.quarantine(fp, uid, chat, None, code + "_raw_omitted")
                self.advance(uid)

    def quarantine(self, fp, uid, chat, raw, code):
        self.conn.execute(
            """INSERT INTO events(fingerprint,update_id,chat_id,raw,error_code)
            VALUES(%s,%s,%s,%s,%s) ON CONFLICT(fingerprint) DO NOTHING""",
            (fp, uid, chat, raw, code),
        )
        if approvals.collecting(self.conn, self.cfg, chat):
            enqueue(
                self.conn,
                "quarantine:" + fp,
                "founder_alert",
                chat,
                payload={"code": "quarantine", "chat_id": chat},
            )

    def cleanup_unknown(self):
        cleanup_unknown(self.conn)

    def unknown(self, chat, kind):
        now = datetime.now(UTC)
        hour = now.replace(minute=0, second=0, microsecond=0)
        expiry = now + timedelta(hours=24)
        key = "unknown-digest:" + hour.isoformat()
        if kind != "private":
            leave_key = "leave:" + str(chat)
            existing = self.conn.execute(
                "SELECT 1 FROM jobs WHERE dedup_key=%s AND status IN ('pending','running')",
                (leave_key,),
            ).fetchone()
            if not existing:
                pending = self.conn.execute(
                    "SELECT count(*) AS n FROM jobs "
                    "WHERE kind='leave' AND status IN ('pending','running')"
                ).fetchone()["n"]
                if pending >= 100:
                    # Never acknowledge an exit we could not queue. Completion frees slots.
                    raise MappingRetry("unknown_leave_backpressure")
                enqueue(self.conn, leave_key, "leave", chat, expires_at=expiry)
        if self.conn.execute("SELECT 1 FROM unknown_chats WHERE chat_id=%s", (chat,)).fetchone():
            return
        n = self.conn.execute("SELECT count(*) AS n FROM unknown_chats").fetchone()["n"]
        enqueue(
            self.conn,
            key,
            "unknown_digest",
            expires_at=expiry,
            payload={"ids": [], "excess": 0},
            next_run=hour + timedelta(hours=1),
        )
        if n >= 100:
            self.conn.execute(
                """UPDATE jobs SET payload=jsonb_set(payload,'{excess}',
                to_jsonb(LEAST(2147483647,(payload->>'excess')::bigint+1)))
                WHERE dedup_key=%s AND status='pending'""",
                (key,),
            )
            return
        self.conn.execute(
            "INSERT INTO unknown_chats VALUES(%s,%s,%s)",
            (chat, "private" if kind == "private" else "group", expiry),
        )
        appended = self.conn.execute(
            """UPDATE jobs SET payload=jsonb_set(payload,'{ids}',
            (payload->'ids') || to_jsonb(%s::bigint)) WHERE dedup_key=%s AND status='pending'
            AND jsonb_array_length(payload->'ids')<100""",
            (chat, key),
        )
        if not appended.rowcount:
            self.conn.execute(
                """UPDATE jobs SET payload=jsonb_set(payload,'{excess}',
                to_jsonb(LEAST(2147483647,(payload->>'excess')::bigint+1)))
                WHERE dedup_key=%s AND status='pending'""",
                (key,),
            )

    @staticmethod
    def group(body):
        chat = body.get("chat")
        return isinstance(chat, dict) and chat.get("type") in approvals.GROUPS

    def membership_change(self, chat, body, state, fp):
        """Bot added/removed in a chat that is not collecting. Stores no event."""
        member = body.get("new_chat_member")
        new = member.get("status") if isinstance(member, dict) else None
        info = body["chat"]
        if new in approvals.ADDED and state in approvals.GONE:
            key = "approval-prompt:" + fp
            if approvals.request(self.conn, chat, info.get("title"), info.get("type"), key):
                return
            self.unknown(chat, info.get("type"))  # bounded backlog: unknown-chat policy
        elif new in approvals.DEPARTED:
            if state is not None:
                approvals.removed(self.conn, chat)
        elif state is None:
            self.unknown(chat, info.get("type"))

    def membership(self, chat, body, fp):
        member = body.get("new_chat_member", {})
        if member.get("status") in {"left", "kicked", "restricted", "member"}:
            enqueue(
                self.conn,
                "coverage:" + fp,
                "founder_alert",
                chat,
                payload={"code": "coverage_check", "chat_id": chat},
            )

    def map_message(self, conn, event, body, uid):
        chat = integer(body["chat"]["id"])
        mid = integer(body["message_id"])
        date = integer(body["date"])
        rev = integer(body.get("edit_date", date))
        content = text(body.get("text", body.get("caption", "")))
        author = body.get("from", {}).get("id")
        author = integer(author) if author is not None else None
        reply = body.get("reply_to_message", {}).get("message_id")
        reply = integer(reply) if reply is not None else None
        album = body.get("media_group_id")
        album = text(album) if album is not None else None
        conn.execute(
            "INSERT INTO revisions VALUES(%s,%s,%s,%s,%s)", (event, chat, mid, rev, content)
        )
        conn.execute(
            """INSERT INTO messages VALUES(%s,%s,%s,%s,to_timestamp(%s),%s,%s,%s,%s)
            ON CONFLICT(chat_id,message_id) DO UPDATE SET event_id=EXCLUDED.event_id,
            revision_time=EXCLUDED.revision_time,sent_at=EXCLUDED.sent_at,
            author_id=EXCLUDED.author_id,text=EXCLUDED.text,
            media_group_id=EXCLUDED.media_group_id,reply_to_id=EXCLUDED.reply_to_id
            WHERE (EXCLUDED.revision_time,EXCLUDED.event_id) >
                  (messages.revision_time,messages.event_id)""",
            (chat, mid, event, rev, date, author, content, album, reply),
        )
        for slot, item, media_kind in attachments(body):
            aid = conn.execute(
                """INSERT INTO attachments(event_id,chat_id,message_id,slot,
                file_id,file_unique_id,declared_size,mime,kind) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s)
                RETURNING id""",
                (
                    event,
                    chat,
                    mid,
                    slot,
                    text(item["file_id"]),
                    text(item["file_unique_id"]) if "file_unique_id" in item else None,
                    integer(item["file_size"]) if "file_size" in item else None,
                    text(item["mime_type"]) if "mime_type" in item else None,
                    media_kind,
                ),
            ).fetchone()["id"]
            enqueue(conn, f"download:{aid}", "download", chat, aid)


def migration(kind, body, chat):
    """(old group, new supergroup) from either half of a Telegram migration, else None."""
    if kind != "message" or chat is None:
        return None
    for field, pair in (
        ("migrate_to_chat_id", lambda other: (chat, other)),
        ("migrate_from_chat_id", lambda other: (other, chat)),
    ):
        other = body.get(field)
        if type(other) is int and other != 0 and other != chat:
            try:
                return pair(integer(other))
            except ValueError:
                return None
    return None


def attachments(body):
    for i, photo in enumerate(body.get("photo", [])):
        yield f"photo/{i}", photo, "photo"
    for kind in MEDIA:
        if kind in body:
            item = body[kind]
            yield kind, item, kind
            for field in ("thumbnail", "thumb", "cover"):
                child = item.get(field)
                if isinstance(child, dict):
                    yield kind + "/" + field, child, "thumbnail"
                elif isinstance(child, list):
                    for i, value in enumerate(child):
                        yield f"{kind}/{field}/{i}", value, "thumbnail"
