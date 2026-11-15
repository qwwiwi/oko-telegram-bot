"""Owner-managed chat approval gate: nothing from a chat is archived before «Да».

Only the poller writes chat_approvals (one ingestion owner, updates processed in
order); the worker, API and export only read it. Statuses:
pending (asked, waiting), active (collecting), declined («Нет»), stopped («Стоп»),
removed (bot left/kicked), expired (no answer within PENDING_TTL; the bot leaves).
A row with migrated_to set is a group that became that supergroup: its decision was
carried to the new ID and it is no longer listed or decidable. Rows hold numeric IDs,
the Telegram chat title for the owner's DM and counters; never message content.
"""

import json
import logging
import re
import unicodedata
from datetime import UTC, datetime, timedelta

import psycopg

from archive.db import enqueue

PENDING, ACTIVE, DECLINED, STOPPED, REMOVED, EXPIRED = (
    "pending",
    "active",
    "declined",
    "stopped",
    "removed",
    "expired",
)
# Statuses after which the bot is (being) out of the chat: a fresh add asks again.
GONE = (None, REMOVED, EXPIRED)
GROUPS = ("group", "supergroup", "channel")
ADDED = ("member", "administrator")
DEPARTED = ("left", "kicked")
LABELS = {
    ACTIVE: "собираю",
    PENDING: "ждёт ответа",
    DECLINED: "выключен",
    STOPPED: "выключен",
    REMOVED: "бот удалён",
    EXPIRED: "нет ответа, бот выходит",
}
# action -> (allowed source statuses, target status)
TRANSITIONS = {
    "yes": ((PENDING, DECLINED, STOPPED), ACTIVE),
    "no": ((PENDING,), DECLINED),
    "stop": ((ACTIVE,), STOPPED),
}
ANSWERS = {
    ACTIVE: "Собираю сообщения",
    DECLINED: "Не собираю",
    STOPPED: "Сбор остановлен",
    "unchanged": "Уже применено",
    "stale": "Кнопка устарела",
    "invalid": "Неизвестная команда",
    "refused": "Недоступно",
    "page": "Список обновлён",
    "outdated": "Список устарел, отправьте /chats",
    "changed": "Состояние изменилось, откройте карточку заново",
    # Card navigation (open, Stop confirmation) needs no toast.
    "open": "",
    "ask": "",
}
# origin p = approval prompt, l = /chats list page, c = per-chat card (with the list page
# to return to). Telegram caps callback_data at 64 bytes; the longest form here (Stop
# confirm with its state version) is 53.
PAGE_NUMBER = r"(0|[1-9][0-9]{0,4})"
CHAT_ID = r"(-?[1-9][0-9]{0,18})"
VERSION = r"([1-9][0-9]{0,18})"
CALLBACK = re.compile(r"p:(yes|no):" + CHAT_ID)
# The list used to carry Stop/Enable buttons; taps on such old messages change nothing.
OUTDATED = re.compile(r"l:(?:yes|no|stop):" + CHAT_ID + "(?::" + PAGE_NUMBER + ")?")
# open = show the card, ask = Stop confirmation, yes/no = owner decision.
CARD = re.compile(r"c:(open|ask|yes|no):" + CHAT_ID + ":" + PAGE_NUMBER)
# The confirmed Stop carries the state version the confirmation was shown for: a tap
# after any later change of the chat (e.g. Stop then «Включить») changes nothing.
STOP = re.compile(r"c:(stop):" + CHAT_ID + ":" + PAGE_NUMBER + ":" + VERSION)
# Row version: updated_at in epoch microseconds, bumped by every status change.
STATE_VERSION = "(extract(epoch FROM updated_at)*1000000)::bigint AS version"
PAGE = re.compile(r"l:page:" + PAGE_NUMBER)
# Contact-like substrings in user-controlled names/titles shown to the owner: URLs,
# t.me and tg:// deep links first (they may hold @ and digits), then emails, @handles,
# and 7+ digit runs (phones) ignoring spaces, dashes, brackets and a leading +.
CONTACTS = re.compile(
    r"(?:https?://|www\.|tg://)\S+|(?:\b|@)(?:t|telegram)\.me/\S*"
    r"|[\w.+-]+@[\w-]+(?:\.[\w-]+)+"
    r"|@\w+"
    r"|\+?\(?\d(?:[\s()\-]*\d){6,}\)?",
    re.IGNORECASE,
)
CALLBACK_ID = re.compile(r"[0-9A-Za-z_-]{1,64}")
MAX_PENDING = 100
PENDING_TTL = timedelta(days=7)
# Rows per /chats page: one button each plus navigation stays under Telegram's
# 100-button keyboard limit.
PAGE_SIZE = 40
# Short list marks; label() stays the full state shown on the card.
MARKS = {
    ACTIVE: "собираю",
    PENDING: "ждёт решения",
    DECLINED: "на паузе",
    STOPPED: "на паузе",
    REMOVED: "бот удалён",
    EXPIRED: "нет ответа",
}
CLIENT_NAMES = 3
INTERACTION_TTL = timedelta(minutes=10)
UNKNOWN_EXIT_TTL = timedelta(hours=24)

log = logging.getLogger(__name__)


def status(conn, chat):
    if chat is None:
        return None
    row = conn.execute("SELECT status FROM chat_approvals WHERE chat_id=%s", (chat,)).fetchone()
    return row["status"] if row else None


def lock(conn, *chats):
    """Per-chat transaction lock serializing implicit leaves with approval creation.

    Taken by request(), migrate() and the worker's leave job (recheck + leaveChat),
    also when no approval row exists yet. Keys are hashed into their own namespace
    (never the poller's LOCK) and taken in a fixed order to avoid deadlocks.
    """
    for chat in sorted(set(chats)):
        conn.execute(
            "SELECT pg_advisory_xact_lock(hashtextextended('chat-approval:' || %s::text, 0))",
            (chat,),
        )


def effective(conn, chat, share=False):
    """Approval status governing work of ``chat``, following a supergroup migration.

    Owner decisions about a migrated group are made on the new ID, so queued work of
    the old ID is allowed only while both rows are active. With share=True both rows
    are held FOR SHARE (in chat_id order, like migrate()) until the transaction ends.
    """
    for _ in range(3):
        # Each attempt runs in a savepoint: a retry first rolls back to it, which drops
        # the FOR SHARE locks of the failed attempt, so a concurrent migrate() locking
        # the supergroup row and then this one never deadlocks with the next attempt.
        with conn.transaction() as attempt:
            link = conn.execute(
                "SELECT migrated_to FROM chat_approvals WHERE chat_id=%s", (chat,)
            ).fetchone()
            if link is None:
                return None
            ids = [chat] if link["migrated_to"] is None else [chat, link["migrated_to"]]
            rows = {
                row["chat_id"]: row
                for row in conn.execute(
                    "SELECT chat_id,status,migrated_to FROM chat_approvals WHERE "
                    "chat_id=ANY(%s) ORDER BY chat_id" + (" FOR SHARE" if share else ""),
                    (ids,),
                ).fetchall()
            }
            own = rows.get(chat)
            if own is None or own["migrated_to"] != link["migrated_to"]:
                # Migrated or re-requested in between: release and read again.
                raise psycopg.Rollback(attempt)
            if own["migrated_to"] is None:
                return own["status"]
            if own["status"] != ACTIVE:
                return own["status"]
            successor = rows.get(own["migrated_to"])
            return successor["status"] if successor else None
    return None  # keeps changing under us: fail closed


def collecting(conn, cfg, chat):
    """New content may be stored, downloaded or transcribed only while active."""
    return chat is not None and cfg.allows(chat) and effective(conn, chat) == ACTIVE


def fenced(conn, cfg, chat):
    """collecting(), holding the approval row(s) until the caller's transaction ends.

    Call inside the transaction that publishes a worker result: owner decisions
    (FOR UPDATE / UPDATE by the poller) wait for it, so a result is committed only
    if the chat is still active at commit, and a Stop committed first is seen here.
    For a migrated group the supergroup's row is held too: its Stop also revokes
    work queued under the old ID.
    """
    return cfg.allows(chat) and effective(conn, chat, share=True) == ACTIVE


def readable(conn, cfg, chats):
    """Chats an agent may read: owner-approved at least once and not revoked in config.

    Archived evidence of a once-approved chat stays readable whatever the current
    status (Stop, removal, a re-add awaiting the owner); only enabled=false revokes.
    A never-approved chat has no archived content and is never visible.
    """
    rows = conn.execute(
        """SELECT chat_id FROM chat_approvals WHERE chat_id=ANY(%s)
        AND approved_at IS NOT NULL""",
        (list(chats),),
    ).fetchall()
    return sorted(row["chat_id"] for row in rows if cfg.allows(row["chat_id"]))


def clean_title(value):
    if not isinstance(value, str):
        return ""
    printable = "".join(c for c in value if unicodedata.category(c)[0] != "C")
    return " ".join(printable.split())[:128]


def redact(value):
    """Replace contacts (URLs, emails, @handles, phone-like numbers) with «…»."""
    return CONTACTS.sub("…", value)


def shown(chat, title):
    """Chat title as shown to the owner; the numeric ID fallback is not redacted."""
    return redact(title) if title else str(chat)


def activate(conn, chat, approver):
    """Operator/test seeding of an already approved chat (same state as owner «Да»)."""
    conn.execute(
        """INSERT INTO chat_approvals(chat_id,status,approved_at,approved_by,decided_by)
        VALUES(%s,'active',now(),%s,%s) ON CONFLICT(chat_id) DO UPDATE
        SET status='active',approved_at=now(),approved_by=EXCLUDED.approved_by,
        decided_by=EXCLUDED.decided_by,updated_at=now()""",
        (chat, approver, approver),
    )


def request(conn, chat, title, kind, key, bounded=True):
    """Record a chat as pending and queue the owner's «Да»/«Нет» prompt.

    Returns False when the bounded pending backlog is full (caller applies the
    unknown-chat policy instead of recording attacker-controlled fanout). Operator-
    vouched sources (migration of an active chat, configured chats) pass
    bounded=False so they are never left because of unrelated pending adds.
    """
    lock(conn, chat)
    current = status(conn, chat)
    if current not in GONE:
        conn.execute(
            "UPDATE chat_approvals SET title=%s WHERE chat_id=%s AND %s<>''",
            (clean_title(title), chat, clean_title(title)),
        )
        return True
    waiting = conn.execute(
        "SELECT count(*) AS n FROM chat_approvals WHERE status='pending' AND migrated_to IS NULL"
    ).fetchone()["n"]
    if bounded and waiting >= MAX_PENDING:
        return False
    conn.execute(
        """INSERT INTO chat_approvals(chat_id,status,title,kind) VALUES(%s,'pending',%s,%s)
        ON CONFLICT(chat_id) DO UPDATE SET status='pending',title=EXCLUDED.title,
        kind=EXCLUDED.kind,requested_at=now(),updated_at=now(),decided_by=NULL,
        migrated_to=NULL""",
        (chat, clean_title(title), kind if kind in GROUPS else None),
    )
    enqueue(conn, key, "approval_prompt", chat, payload={"chat_id": chat})
    log.info("chat_approval_requested")
    return True


def removed(conn, chat):
    conn.execute(
        "UPDATE chat_approvals SET status='removed',updated_at=now() WHERE chat_id=%s",
        (chat,),
    )


def expire(conn, cfg):
    """Pending past PENDING_TTL → expired, and the bot leaves (a re-add asks again).

    Bounds how long spam adds can hold the MAX_PENDING slots. Operator-configured
    chats are never left implicitly, so they keep waiting for the owner.
    """
    rows = conn.execute(
        """UPDATE chat_approvals SET status='expired',updated_at=now()
        WHERE status='pending' AND migrated_to IS NULL AND requested_at<=now()-%s
        AND NOT chat_id=ANY(%s) RETURNING chat_id""",
        (PENDING_TTL, list(cfg.chats)),
    ).fetchall()
    for row in rows:
        enqueue(
            conn,
            "leave:" + str(row["chat_id"]),
            "leave",
            row["chat_id"],
            expires_at=datetime.now(UTC) + UNKNOWN_EXIT_TTL,
        )
    if rows:
        log.info("chat_approval_expired count=%d", len(rows))


def migrate(conn, old, new, title):
    """Carry the owner's decision from a group to the supergroup it became.

    Telegram sends migrate_to_chat_id in the old group and migrate_from_chat_id in
    the new supergroup, in either order; both call this with the same (old, new)
    and the first wins. The new ID gets the old status (active stays active,
    pending is asked again under the new ID); the old row keeps its status so
    queued work from before the migration completes while the new ID stays active
    (see effective()), but it is no longer listed or decidable. An unanswered
    request for the new ID is not an owner decision and takes the old one. Returns
    False when there is no decision to carry (unknown group).
    """
    lock(conn, old, new)
    rows = {
        row["chat_id"]: row
        for row in conn.execute(
            "SELECT * FROM chat_approvals WHERE chat_id IN (%s,%s) ORDER BY chat_id FOR UPDATE",
            (old, new),
        ).fetchall()
    }
    source, target = rows.get(old), rows.get(new)
    if source is None or source["status"] in GONE:
        return False
    if source["migrated_to"] is not None:
        return True  # already carried by the other half of the pair or a re-delivery
    conn.execute(
        "UPDATE chat_approvals SET migrated_to=%s,updated_at=now() WHERE chat_id=%s",
        (new, old),
    )
    conn.execute("INSERT INTO enrollment_hints VALUES(%s,%s) ON CONFLICT DO NOTHING", (old, new))
    if source["approved_at"] is not None:
        # Agent scopes are never widened automatically: the operator enrolls the new ID.
        enqueue(
            conn,
            f"migration:{old}:{new}",
            "founder_alert",
            old,
            payload={"code": "pending_enrollment", "old": old, "new": new},
        )
    # An update from the new supergroup that raced ahead of both halves may have
    # queued an unknown-chat exit; the chat is known now (the worker re-checks too).
    conn.execute("DELETE FROM jobs WHERE dedup_key=%s AND status='pending'", ("leave:" + str(new),))
    conn.execute("DELETE FROM unknown_chats WHERE chat_id=%s", (new,))
    if target is not None and target["status"] not in GONE:
        # The bot may have been seen in the supergroup (and asked about it) before the
        # migration message: that pending request yields to the group's decision.
        if target["status"] != PENDING or source["status"] == PENDING:
            return True  # the new ID already has its own owner decision or prompt
    conn.execute(
        """INSERT INTO chat_approvals(chat_id,status,title,kind,requested_at,approved_at,
        approved_by,decided_by) VALUES(%s,%s,%s,'supergroup',now(),%s,%s,%s)
        ON CONFLICT(chat_id) DO UPDATE SET status=EXCLUDED.status,title=EXCLUDED.title,
        kind=EXCLUDED.kind,requested_at=now(),approved_at=EXCLUDED.approved_at,
        approved_by=EXCLUDED.approved_by,decided_by=EXCLUDED.decided_by,
        updated_at=now(),migrated_to=NULL""",
        (
            new,
            source["status"],
            clean_title(title) or source["title"],
            source["approved_at"],
            source["approved_by"],
            source["decided_by"],
        ),
    )
    if source["status"] == PENDING:
        enqueue(
            conn,
            f"approval-migration:{old}:{new}",
            "approval_prompt",
            new,
            payload={"chat_id": new},
        )
    log.info("chat_approval_migrated")
    return True


def dropped(conn, chat):
    """Acknowledge-and-drop accounting for a non-collecting chat: a count, no content."""
    total = conn.execute(
        """UPDATE chat_approvals SET dropped=LEAST(9223372036854775806,dropped)+1
        WHERE chat_id=%s RETURNING dropped""",
        (chat,),
    ).fetchone()
    log.info("unapproved_update_dropped total=%d", total["dropped"] if total else 0)


def decide(conn, chat, action, owner, version=None):
    """Apply an owner action; idempotent for repeats and stale buttons.

    With version set (confirmed Stop) the action applies only if the chat is still in
    the state the confirmation was shown for; otherwise nothing changes ("changed").
    """
    sources, target = TRANSITIONS[action]
    row = conn.execute(
        "SELECT status,migrated_to," + STATE_VERSION + " FROM chat_approvals "
        "WHERE chat_id=%s FOR UPDATE",
        (chat,),
    ).fetchone()
    if row["migrated_to"] is not None:
        return "stale"
    if version is not None and row["version"] != version:
        return "changed"
    if row["status"] == target:
        return "unchanged"
    if row["status"] not in sources:
        return "stale"
    if target == ACTIVE:
        conn.execute(
            """UPDATE chat_approvals SET status='active',approved_at=now(),approved_by=%s,
            decided_by=%s,updated_at=now() WHERE chat_id=%s""",
            (owner, owner, chat),
        )
    else:
        conn.execute(
            """UPDATE chat_approvals SET status=%s,decided_by=%s,updated_at=now()
            WHERE chat_id=%s""",
            (target, owner, chat),
        )
    log.info("chat_approval_%s", target)
    return target


def callback(conn, cfg, body):
    """Validate and apply an inline-button tap. Never stores or logs callback data."""
    qid = body.get("id")
    if not isinstance(qid, str) or not CALLBACK_ID.fullmatch(qid):
        log.warning("callback_invalid_id")
        return
    message = body.get("message") if isinstance(body.get("message"), dict) else {}
    origin_chat = (
        message.get("chat", {}).get("id") if isinstance(message.get("chat"), dict) else None
    )
    mid = message.get("message_id")
    sender = body.get("from", {}).get("id") if isinstance(body.get("from"), dict) else None
    payload = {"callback_id": qid, "answer": "refused"}
    if type(sender) is not int or sender != cfg.owner_tg_id or origin_chat != cfg.owner_tg_id:
        log.warning("callback_refused")
    else:
        data = body.get("data")
        data = data if isinstance(data, str) else ""
        prompted, opened, stop, page = (
            CALLBACK.fullmatch(data),
            CARD.fullmatch(data),
            STOP.fullmatch(data),
            PAGE.fullmatch(data),
        )
        found = prompted or opened or stop
        chat = int(found.group(2)) if found else None
        if page:
            payload.update(answer="page", origin="l", page=int(page.group(1)))
        elif OUTDATED.fullmatch(data):
            payload["answer"] = "outdated"
        elif found is None or status(conn, chat) is None:
            log.warning("callback_invalid")
            payload["answer"] = "invalid"
        elif prompted:
            payload.update(answer=decide(conn, chat, prompted.group(1), sender), origin="p")
        elif stop:
            answer = decide(conn, chat, "stop", sender, int(stop.group(4)))
            payload.update(answer=answer, origin="c", chat=chat, page=int(stop.group(3)))
        else:
            action = opened.group(1)
            answer = action if action in ("open", "ask") else decide(conn, chat, action, sender)
            payload.update(
                answer=answer,
                origin="c",
                chat=chat,
                page=int(opened.group(3)),
                confirm=action == "ask",
            )
        if "origin" in payload and type(mid) is int:
            payload["message_id"] = mid
    enqueue(
        conn,
        "callback:" + qid,
        "owner_callback",
        payload=payload,
        expires_at=datetime.now(UTC) + INTERACTION_TTL,
    )


def command(conn, cfg, body, key):
    """Owner DM commands. Only /chats exists; anything else is ignored, not stored."""
    sender = body.get("from", {}).get("id") if isinstance(body.get("from"), dict) else None
    text = body.get("text")
    if sender != cfg.owner_tg_id or not isinstance(text, str):
        return
    words = text.split()
    if words and words[0].split("@", 1)[0] == "/chats":
        enqueue(conn, key, "chats_list", expires_at=datetime.now(UTC) + INTERACTION_TTL)


def prompt(chat, title):
    text = f"Меня добавили в чат «{shown(chat, title)}». Собирать сообщения?"
    keyboard = [
        [
            {"text": "Да", "callback_data": f"p:yes:{chat}"},
            {"text": "Нет", "callback_data": f"p:no:{chat}"},
        ]
    ]
    return text, {"inline_keyboard": keyboard}


def label(cfg, chat, value):
    """Effective state: the operator kill switch (enabled = false) wins over the bot."""
    if cfg.allows(chat):
        return LABELS[value]
    return "выключен в конфиге" if value == ACTIVE else LABELS[value] + ", выключен в конфиге"


def mark(cfg, chat, value):
    return MARKS[value] if cfg.allows(chat) else "выключен в конфиге"


def render(conn, cfg, page=0):
    """One /chats page: a button per chat (title and state) that opens its card.

    No decisions on the list itself, so a stray tap never stops a chat. Every row is
    reachable through «Назад»/«Далее»; a page past the end (rows removed since the
    list was sent) shows the last page.
    """
    total = conn.execute(
        "SELECT count(*) AS n FROM chat_approvals WHERE migrated_to IS NULL"
    ).fetchone()["n"]
    pages = max(1, -(-total // PAGE_SIZE))
    page = min(max(0, page), pages - 1)
    rows = conn.execute(
        """SELECT chat_id,status,title FROM chat_approvals WHERE migrated_to IS NULL ORDER BY
        CASE status WHEN 'active' THEN 0 WHEN 'pending' THEN 1 WHEN 'stopped' THEN 2
        WHEN 'declined' THEN 3 ELSE 4 END,requested_at,chat_id LIMIT %s OFFSET %s""",
        (PAGE_SIZE, page * PAGE_SIZE),
    ).fetchall()
    lines = ["Чаты:" if pages == 1 else f"Чаты (страница {page + 1} из {pages}):"]
    keyboard = []
    for row in rows:
        chat = row["chat_id"]
        title = shown(chat, row["title"])
        keyboard.append(
            [
                {
                    "text": f"{title[:48]} · {mark(cfg, chat, row['status'])}",
                    "callback_data": f"c:open:{chat}:{page}",
                }
            ]
        )
    lines.append("Нажмите на чат, чтобы открыть карточку." if rows else "пока нет")
    navigation = []
    if page > 0:
        navigation.append({"text": "Назад", "callback_data": f"l:page:{page - 1}"})
    if page < pages - 1:
        navigation.append({"text": "Далее", "callback_data": f"l:page:{page + 1}"})
    if navigation:
        keyboard.append(navigation)
    return "\n".join(lines), {"inline_keyboard": keyboard}


def link(chat, message):
    """t.me/c deep link for a supergroup/channel (-100 prefix); None otherwise."""
    text = str(chat)
    # Supergroup/channel IDs are -100 followed by at least 10 digits; a basic group's
    # ID may also start with -100 but is shorter.
    if chat > -1000000000000 or not text.startswith("-100") or text[4] == "0":
        return None
    return f"https://t.me/c/{text[4:]}/{message or 1}"


def person(raw):
    """First and last name of the archived message author; never username or phone."""
    try:
        update = json.loads(bytes(raw))
    except (TypeError, ValueError, RecursionError):
        return ""
    for body in update.values() if isinstance(update, dict) else ():
        sender = body.get("from") if isinstance(body, dict) else None
        if isinstance(sender, dict):
            if sender.get("is_bot") is True:
                return ""
            parts = (sender.get("first_name"), sender.get("last_name"))
            # Redact before truncating so a cut never leaves part of a contact behind.
            return redact(clean_title(" ".join(p for p in parts if isinstance(p, str))))[:64]
    return ""


def clients(conn, cfg, chats):
    """Names of chat members other than the owner, staff and bots, latest first."""
    staff = [cfg.owner_tg_id, cfg.founder_id, *cfg.staff]
    rows = conn.execute(
        """SELECT raw FROM (SELECT DISTINCT ON (m.author_id) m.sent_at,e.raw
        FROM messages m JOIN events e ON e.id=m.event_id
        WHERE m.chat_id=ANY(%s) AND m.author_id IS NOT NULL AND m.author_id<>ALL(%s)
        ORDER BY m.author_id,m.sent_at DESC,m.message_id DESC) latest
        ORDER BY sent_at DESC LIMIT 20""",
        (chats, staff),
    ).fetchall()
    names = []
    for row in rows:
        name = person(row["raw"])
        if name and name not in names:
            names.append(name)
    shown = ", ".join(names[:CLIENT_NAMES])
    return shown + (f" и ещё {len(names) - CLIENT_NAMES}" if len(names) > CLIENT_NAMES else "")


def card(conn, cfg, chat, page=0, confirm=False):
    """Owner-facing card of one chat: state, client, archive counters, link, actions.

    With confirm=True shows the Stop confirmation instead. A chat that is gone from
    the list (migrated or unknown) shows the list page instead.
    """
    row = conn.execute(
        "SELECT status,title,migrated_to," + STATE_VERSION + " FROM chat_approvals "
        "WHERE chat_id=%s",
        (chat,),
    ).fetchone()
    if row is None or row["migrated_to"] is not None:
        return render(conn, cfg, page)
    title = shown(chat, row["title"])
    back = {"text": "← К списку", "callback_data": f"l:page:{page}"}
    if confirm and row["status"] == ACTIVE:
        return f"Остановить сбор в «{title}»?", {
            "inline_keyboard": [
                [
                    {
                        "text": "Да, остановить",
                        "callback_data": f"c:stop:{chat}:{page}:{row['version']}",
                    },
                    {"text": "Отмена", "callback_data": f"c:open:{chat}:{page}"},
                ]
            ]
        }
    # Messages of a group that became this supergroup belong to the same conversation.
    chats = [chat] + [
        r["chat_id"]
        for r in conn.execute(
            "SELECT chat_id FROM chat_approvals WHERE migrated_to=%s", (chat,)
        ).fetchall()
    ]
    stats = conn.execute(
        """SELECT count(*) AS total,
        count(*) FILTER (WHERE sent_at>=date_trunc('day',now() AT TIME ZONE 'Europe/Moscow')
            AT TIME ZONE 'Europe/Moscow') AS today,
        to_char(max(sent_at) AT TIME ZONE 'Europe/Moscow','DD.MM.YYYY HH24:MI') AS last,
        max(message_id) FILTER (WHERE chat_id=%s) AS last_id
        FROM messages WHERE chat_id=ANY(%s)""",
        (chat, chats),
    ).fetchone()
    files = conn.execute(
        "SELECT count(*) AS n FROM attachments WHERE chat_id=ANY(%s)", (chats,)
    ).fetchone()["n"]
    lines = [
        f"«{title}»",
        f"Состояние: {label(cfg, chat, row['status'])}",
        f"Клиент: {clients(conn, cfg, chats) or '—'}",
        f"Сообщений: {stats['total']} (сегодня {stats['today']})",
        f"Вложений: {files}",
        "Последнее сообщение: " + (f"{stats['last']} МСК" if stats["last"] else "—"),
    ]
    keyboard = []
    url = link(chat, stats["last_id"])
    if url:
        keyboard.append([{"text": "Открыть чат", "url": url}])
    actions = {
        ACTIVE: [("Стоп", "ask")],
        STOPPED: [("Включить", "yes")],
        DECLINED: [("Включить", "yes")],
        PENDING: [("Да", "yes"), ("Нет", "no")],
    }.get(row["status"], [])
    keyboard.append(
        [{"text": t, "callback_data": f"c:{a}:{chat}:{page}"} for t, a in actions] + [back]
    )
    return "\n".join(lines), {"inline_keyboard": keyboard}
