"""Group → supergroup migration carries the owner's decision; the bot never leaves."""

from dataclasses import replace

import pytest
from test_approvals import OWNER, Bot, drain, tap
from test_ingest import count, update

from archive import approvals
from archive.ingest import Ingestor

OLD = -500
NEW = -1005000000099


def to_message(uid):
    value = update(uid, mid=uid, chat=OLD, text="", migrate_to_chat_id=NEW)
    value["message"]["chat"].update(type="group", title="Old Group")
    return value


def from_message(uid):
    value = update(uid, mid=1, chat=NEW, text="", migrate_from_chat_id=OLD)
    value["message"]["chat"]["title"] = "New Super"
    return value


def seed(db, status):
    db.execute(
        """INSERT INTO chat_approvals(chat_id,status,title,kind,approved_at,approved_by)
        VALUES(%s,%s,'Old Group','group',
        CASE WHEN %s IN ('active','stopped') THEN now() END,
        CASE WHEN %s IN ('active','stopped') THEN %s END)""",
        (OLD, status, status, status, OWNER),
    )


def row(db, chat):
    return db.execute("SELECT * FROM chat_approvals WHERE chat_id=%s", (chat,)).fetchone()


@pytest.mark.parametrize("order", ["to_first", "from_first"])
@pytest.mark.parametrize("status", ["active", "pending", "stopped", "declined"])
def test_migration_carries_approval_state_in_either_order(db, cfg, status, order):
    cfg = replace(cfg, dsn=db.info.dsn)
    seed(db, status)
    first, second = (
        (to_message, from_message) if order == "to_first" else (from_message, to_message)
    )
    with Ingestor(db, cfg) as ing:
        ing.ingest(first(1))
        ing.ingest(second(2))
        ing.ingest(update(3, mid=2, chat=NEW, text="after migration"))
        ing.ingest(second(4))  # Telegram re-delivery under another update_id
        assert ing.offset == 5
    new = row(db, NEW)
    assert new["status"] == status
    assert row(db, OLD)["migrated_to"] == NEW
    assert count(db, "enrollment_hints") == 1
    stored = db.execute("SELECT count(*) AS n FROM messages WHERE chat_id=%s", (NEW,)).fetchone()
    assert (stored["n"] > 0) == (status == "active")
    assert not db.execute("SELECT 1 FROM jobs WHERE kind='leave'").fetchone()
    assert not db.execute("SELECT 1 FROM unknown_chats").fetchone()
    prompts = db.execute(
        "SELECT chat_id FROM jobs WHERE kind='approval_prompt' ORDER BY id"
    ).fetchall()
    assert [p["chat_id"] for p in prompts] == ([NEW] if status == "pending" else [])
    if status in ("active", "stopped"):
        assert new["approved_at"] is not None and new["approved_by"] == OWNER
    bot = Bot()
    drain(db, cfg, bot)
    assert "leaveChat" not in bot.methods()
    sent = [p for m, p in bot.calls if m == "sendMessage" and p["chat_id"] == OWNER]
    if status == "pending":
        assert sent[0]["reply_markup"]["inline_keyboard"][0][0]["callback_data"] == f"p:yes:{NEW}"


def test_migrated_old_chat_is_hidden_and_its_buttons_are_stale(db, cfg):
    cfg = replace(cfg, dsn=db.info.dsn)
    seed(db, "pending")
    with Ingestor(db, cfg) as ing:
        ing.ingest(to_message(1))
        ing.ingest(tap(2, f"p:yes:{OLD}"))
        assert row(db, OLD)["status"] == "pending" and row(db, NEW)["status"] == "pending"
        ing.ingest(tap(3, f"p:yes:{NEW}"))
    assert row(db, NEW)["status"] == "active"
    answers = db.execute(
        "SELECT payload FROM jobs WHERE kind='owner_callback' ORDER BY id"
    ).fetchall()
    assert [a["payload"]["answer"] for a in answers] == ["stale", "active"]
    # The new chat inherits the old title; the dead old ID is not listed at all.
    _, markup = approvals.render(db, cfg)
    texts = [b["text"] for r in markup["inline_keyboard"] for b in r]
    assert sum(t.startswith("Old Group ·") for t in texts) == 1
    buttons = [b["callback_data"] for r in markup["inline_keyboard"] for b in r]
    assert f"c:open:{NEW}:0" in buttons and not any(str(OLD) + ":" in b for b in buttons)


def test_unknown_group_migration_still_follows_unknown_policy(db, cfg):
    cfg = replace(cfg, dsn=db.info.dsn)
    with Ingestor(db, cfg) as ing:
        ing.ingest(from_message(1))
    assert row(db, NEW) is None and row(db, OLD) is None
    assert db.execute("SELECT 1 FROM jobs WHERE kind='leave'").fetchone()


def test_pending_cap_ignores_migrated_rows(db, cfg):
    cfg = replace(cfg, dsn=db.info.dsn)
    seed(db, "pending")
    with Ingestor(db, cfg) as ing:
        ing.ingest(to_message(1))
    waiting = db.execute(
        "SELECT count(*) AS n FROM chat_approvals WHERE status='pending' AND migrated_to IS NULL"
    ).fetchone()["n"]
    assert waiting == 1
