import hashlib
import json
import logging
from dataclasses import replace

import pytest
from fastapi.testclient import TestClient
from test_config import SYNTHETIC_BOT_ID, SYNTHETIC_OWNER_ID
from test_ingest import count, update

from archive import approvals
from archive.api import create_app
from archive.approvals import command, render
from archive.config import Agent
from archive.ingest import Ingestor
from archive.worker import Worker

OWNER = SYNTHETIC_OWNER_ID
CLIENT = -1005000000001


def confirmed(conn, chat, page=0):
    """«Да, остановить» callback data for the chat's current state."""
    row = conn.execute(
        "SELECT " + approvals.STATE_VERSION + " FROM chat_approvals WHERE chat_id=%s", (chat,)
    ).fetchone()
    return f"c:stop:{chat}:{page}:{row['version']}"


class Bot:
    def __init__(self):
        self.calls = []

    def call(self, method, params=None):
        self.calls.append((method, params))
        return {"message_id": 1} if method == "sendMessage" else True

    def methods(self):
        return [method for method, _ in self.calls]


def member(uid, chat=CLIENT, status="member", title="Synthetic Client", kind="supergroup"):
    return {
        "update_id": uid,
        "my_chat_member": {
            "chat": {"id": chat, "type": kind, "title": title},
            "from": {"id": 42},
            "date": 1700000000,
            "old_chat_member": {"status": "left", "user": {"id": SYNTHETIC_BOT_ID}},
            "new_chat_member": {"status": status, "user": {"id": SYNTHETIC_BOT_ID}},
        },
    }


def tap(uid, data, sender=OWNER, qid=None, mid=77):
    return {
        "update_id": uid,
        "callback_query": {
            "id": qid or str(9000 + uid),
            "from": {"id": sender},
            "data": data,
            "message": {
                "message_id": mid,
                "date": 1700000000,
                "chat": {"id": sender, "type": "private"},
            },
        },
    }


def dm(uid, text, sender=OWNER):
    value = update(uid, mid=uid, chat=sender, text=text)
    value["message"]["chat"]["type"] = "private"
    value["message"]["from"] = {"id": sender}
    return value


def client_message(uid, text="DO_NOT_RETAIN", **extra):
    return update(uid, mid=uid, chat=CLIENT, text=text, **extra)


def state(db, chat=CLIENT):
    return db.execute("SELECT * FROM chat_approvals WHERE chat_id=%s", (chat,)).fetchone()


def stored(db):
    return {
        "events": db.execute(
            "SELECT count(*) AS n FROM events WHERE chat_id=%s", (CLIENT,)
        ).fetchone()["n"],
        "messages": db.execute(
            "SELECT count(*) AS n FROM messages WHERE chat_id=%s", (CLIENT,)
        ).fetchone()["n"],
        "attachments": db.execute(
            "SELECT count(*) AS n FROM attachments WHERE chat_id=%s", (CLIENT,)
        ).fetchone()["n"],
        "downloads": db.execute(
            "SELECT count(*) AS n FROM jobs WHERE kind='download' AND chat_id=%s", (CLIENT,)
        ).fetchone()["n"],
    }


NOTHING = {"events": 0, "messages": 0, "attachments": 0, "downloads": 0}


def drain(db, cfg, bot):
    worker = Worker(db, cfg, bot)
    while worker.once():
        pass


def test_pending_chat_stores_nothing_and_prompts_owner(db, cfg, caplog):
    cfg = replace(cfg, dsn=db.info.dsn)
    caplog.set_level(logging.DEBUG)
    with Ingestor(db, cfg) as ing:
        ing.ingest(member(1))
        assert state(db)["status"] == "pending"
        ing.ingest(client_message(2, document={"file_id": "DO_NOT_RETAIN_FILE"}))
        ing.ingest(client_message(3, photo=[{"file_id": "DO_NOT_RETAIN_PHOTO"}]))
        assert ing.offset == 4
    assert stored(db) == NOTHING
    assert state(db)["dropped"] == 2
    kinds = [row["kind"] for row in db.execute("SELECT kind FROM jobs").fetchall()]
    assert kinds == ["approval_prompt"]  # never a leave job for a chat awaiting the owner
    dump = json.dumps(
        [
            db.execute("SELECT * FROM jobs").fetchall(),
            db.execute("SELECT * FROM chat_approvals").fetchall(),
        ],
        default=str,
    )
    assert "DO_NOT_RETAIN" not in dump and "DO_NOT_RETAIN" not in caplog.text
    bot = Bot()
    drain(db, cfg, bot)
    assert bot.calls == [
        (
            "sendMessage",
            {
                "chat_id": OWNER,
                "text": "Меня добавили в чат «Synthetic Client». Собирать сообщения?",
                "reply_markup": {
                    "inline_keyboard": [
                        [
                            {"text": "Да", "callback_data": f"p:yes:{CLIENT}"},
                            {"text": "Нет", "callback_data": f"p:no:{CLIENT}"},
                        ]
                    ]
                },
            },
        )
    ]


def test_yes_then_messages_stored_without_backfill(db, cfg):
    cfg = replace(cfg, dsn=db.info.dsn)
    with Ingestor(db, cfg) as ing:
        ing.ingest(member(1, status="administrator"))
        ing.ingest(client_message(2, text="before approval"))
        ing.ingest(tap(3, f"p:yes:{CLIENT}"))
        row = state(db)
        assert row["status"] == "active" and row["approved_by"] == OWNER
        assert row["approved_at"] is not None
        ing.ingest(client_message(4, text="after approval", document={"file_id": "f"}))
    assert stored(db) == {"events": 1, "messages": 1, "attachments": 1, "downloads": 1}
    assert db.execute("SELECT message_id FROM messages").fetchall()[-1]["message_id"] == 4
    bot = Bot()
    worker = Worker(db, cfg, bot)
    while worker.once() and "answerCallbackQuery" not in bot.methods():
        pass
    answer = next(params for method, params in bot.calls if method == "answerCallbackQuery")
    assert answer["callback_query_id"] == "9003" and answer["text"]
    assert (
        "editMessageReplyMarkup",
        {
            "chat_id": OWNER,
            "message_id": 77,
            "reply_markup": {"inline_keyboard": []},
        },
    ) in bot.calls


def test_no_stop_and_removed_store_nothing_new(db, cfg):
    cfg = replace(cfg, dsn=db.info.dsn)
    with Ingestor(db, cfg) as ing:
        ing.ingest(member(1))
        ing.ingest(tap(2, f"p:no:{CLIENT}"))
        assert state(db)["status"] == "declined"
        ing.ingest(client_message(3))
        assert stored(db) == NOTHING

        ing.ingest(tap(4, f"c:yes:{CLIENT}:0"))
        ing.ingest(client_message(5, text="kept"))
        assert stored(db)["messages"] == 1
        ing.ingest(tap(6, confirmed(db, CLIENT)))
        assert state(db)["status"] == "stopped"
        ing.ingest(client_message(7, document={"file_id": "new"}))
        assert stored(db) == {"events": 1, "messages": 1, "attachments": 0, "downloads": 0}

        ing.ingest(tap(8, f"c:yes:{CLIENT}:0"))
        ing.ingest(member(9, status="kicked"))
        assert state(db)["status"] == "removed"
        ing.ingest(client_message(10))
        assert stored(db)["messages"] == 1
        # Removed chats cannot be re-enabled by a stale button; only a fresh add asks again.
        ing.ingest(tap(11, f"c:yes:{CLIENT}:0"))
        assert state(db)["status"] == "removed"
        ing.ingest(member(12))
        assert state(db)["status"] == "pending"
    assert db.execute("SELECT text FROM messages").fetchone()["text"] == "kept"
    assert not db.execute("SELECT 1 FROM jobs WHERE kind='leave'").fetchall()


def test_non_owner_callback_refused_without_content(db, cfg, caplog):
    cfg = replace(cfg, dsn=db.info.dsn)
    caplog.set_level(logging.DEBUG)
    with Ingestor(db, cfg) as ing:
        ing.ingest(member(1))
        ing.ingest(tap(2, f"p:yes:{CLIENT}", sender=4242))
        forwarded = tap(3, f"p:yes:{CLIENT}")
        forwarded["callback_query"]["message"]["chat"]["id"] = -77  # owner, foreign message
        ing.ingest(forwarded)
        assert ing.offset == 4
    assert state(db)["status"] == "pending"
    assert "callback_refused" in caplog.text and f"p:yes:{CLIENT}" not in caplog.text
    bot = Bot()
    drain(db, cfg, bot)
    answers = [p for m, p in bot.calls if m == "answerCallbackQuery"]
    assert [a["callback_query_id"] for a in answers] == ["9002", "9003"]
    assert all(a["text"] and a["show_alert"] for a in answers)
    assert not {"editMessageReplyMarkup", "editMessageText"} & set(bot.methods())


@pytest.mark.parametrize(
    "data",
    [
        "p:yes:-1009999999999",  # chat not in the approval table
        f"p:drop:{CLIENT}",
        f"x:yes:{CLIENT}",
        f"p:yes:{CLIENT};stop",
        f"p:yes:0{abs(CLIENT)}",
        "p:yes:" + "9" * 70,
        123,
        None,
    ],
)
def test_forged_callback_data_refused(db, cfg, data):
    cfg = replace(cfg, dsn=db.info.dsn)
    with Ingestor(db, cfg) as ing:
        ing.ingest(member(1))
        ing.ingest(tap(2, data))
        assert ing.offset == 3
    assert state(db)["status"] == "pending"
    # Only the seeded chats and the pending client: a forged chat ID creates no row.
    assert db.execute("SELECT count(*) AS n FROM chat_approvals").fetchone()["n"] == 3
    bot = Bot()
    drain(db, cfg, bot)
    assert bot.methods().count("answerCallbackQuery") == 1
    assert not {"editMessageReplyMarkup", "editMessageText"} & set(bot.methods())


def test_chats_command_lists_statuses_owner_only(db, cfg):
    cfg = replace(cfg, dsn=db.info.dsn)
    chats = {-1001: "Active One", -1002: "Waiting Two", -1003: "Off Three", -1004: "Gone Four"}
    with Ingestor(db, cfg) as ing:
        for uid, (chat, title) in enumerate(chats.items(), start=1):
            ing.ingest(member(uid, chat=chat, title=title))
        ing.ingest(tap(10, "p:yes:-1001"))
        ing.ingest(tap(11, "p:no:-1003"))
        ing.ingest(member(12, chat=-1004, status="left"))
        ing.ingest(dm(13, "/chats", sender=4242))  # not the owner: no list
        ing.ingest(dm(14, "/chats"))
    assert count(db, "events") == 0
    lists = db.execute("SELECT count(*) AS n FROM jobs WHERE kind='chats_list'").fetchone()
    assert lists["n"] == 1
    bot = Bot()
    drain(db, cfg, bot)
    listing = [p for m, p in bot.calls if m == "sendMessage" and p["text"].startswith("Чаты")]
    assert len(listing) == 1 and listing[0]["chat_id"] == OWNER
    buttons = {
        button["callback_data"]: button["text"]
        for row in listing[0]["reply_markup"]["inline_keyboard"]
        for button in row
    }
    # The list only opens cards; decisions live on the card.
    assert buttons == {
        "c:open:-1001:0": "Active One · собираю",
        "c:open:-1002:0": "Waiting Two · ждёт решения",
        "c:open:-1003:0": "Off Three · на паузе",
        "c:open:-1004:0": "Gone Four · бот удалён",
        "c:open:-10:0": "-10 · собираю",
        "c:open:-20:0": "-20 · собираю",
    }

    with Ingestor(db, cfg) as ing:
        ing.ingest(tap(20, confirmed(db, -1001), mid=500))
    bot = Bot()
    drain(db, cfg, bot)
    edit = next(p for m, p in bot.calls if m == "editMessageText")
    assert edit["message_id"] == 500 and "Состояние: выключен" in edit["text"]


def test_duplicate_and_stale_taps_are_idempotent(db, cfg):
    cfg = replace(cfg, dsn=db.info.dsn)
    with Ingestor(db, cfg) as ing:
        ing.ingest(member(1))
        first = tap(2, f"p:yes:{CLIENT}")
        ing.ingest(first)
        approved = state(db)["approved_at"]
        ing.ingest(first)  # Telegram re-delivery of the same update
        ing.ingest(tap(3, f"p:yes:{CLIENT}"))  # double tap, new callback id
        ing.ingest(tap(4, f"p:no:{CLIENT}"))  # stale prompt button after approval
        ing.ingest(member(5, status="administrator"))  # promotion keeps the decision
        row = state(db)
    assert row["status"] == "active" and row["approved_at"] == approved
    callbacks = db.execute(
        "SELECT payload FROM jobs WHERE kind='owner_callback' ORDER BY id"
    ).fetchall()
    assert [c["payload"]["answer"] for c in callbacks] == ["active", "unchanged", "stale"]
    assert (
        db.execute("SELECT count(*) AS n FROM jobs WHERE kind='approval_prompt'").fetchone()["n"]
        == 1
    )


def test_unapproved_chat_never_visible_through_api(db, cfg):
    token = "y" * 32
    cfg = replace(
        cfg,
        dsn=db.info.dsn,
        agents={"reader": Agent(hashlib.sha256(token.encode()).hexdigest(), (-10, CLIENT))},
    )
    client = TestClient(create_app(cfg), headers={"Authorization": "Bearer " + token})
    with Ingestor(db, cfg) as ing:
        ing.ingest(member(1))
        ing.ingest(client_message(2))
    assert client.get("/v1/chats").json() == [{"chat_id": -10, "slug": "alpha"}]
    assert client.get(f"/v1/chats/{CLIENT}/messages").status_code == 404
    with Ingestor(db, cfg) as ing:
        ing.ingest(tap(3, f"p:yes:{CLIENT}"))
        ing.ingest(client_message(4, text="visible"))
    slug = "chat" + str(CLIENT)
    assert {"chat_id": CLIENT, "slug": slug} in client.get("/v1/chats").json()
    items = client.get(f"/v1/chats/{CLIENT}/messages").json()["items"]
    assert [i["text"] for i in items] == ["visible"]
    with Ingestor(db, cfg) as ing:
        ing.ingest(tap(5, confirmed(db, CLIENT)))
    # Stop halts collection; already archived evidence stays readable to scoped agents.
    assert client.get(f"/v1/chats/{CLIENT}/messages").status_code == 200


def test_configured_chat_without_row_asks_owner_instead_of_leaving(db, cfg):
    # Upgrade path: a chat configured before the approval flow has no approval row.
    cfg = replace(cfg, dsn=db.info.dsn)
    db.execute("DELETE FROM chat_approvals WHERE chat_id=-20")
    db.execute(
        "INSERT INTO chat_approvals(chat_id,status) "
        "SELECT -2000-g,'pending' FROM generate_series(1,100) g"
    )
    with Ingestor(db, cfg) as ing:
        ing.ingest(update(1, chat=-20, text="DO_NOT_RETAIN"))
        ing.ingest(update(2, chat=-20, text="DO_NOT_RETAIN"))
    assert state(db, -20)["status"] == "pending"
    assert count(db, "events") == 0
    kinds = [r["kind"] for r in db.execute("SELECT kind FROM jobs").fetchall()]
    assert kinds == ["approval_prompt"]


def test_readded_chat_keeps_archived_evidence_readable(db, cfg):
    token = "y" * 32
    cfg = replace(
        cfg,
        dsn=db.info.dsn,
        agents={"reader": Agent(hashlib.sha256(token.encode()).hexdigest(), (CLIENT,))},
    )
    client = TestClient(create_app(cfg), headers={"Authorization": "Bearer " + token})
    with Ingestor(db, cfg) as ing:
        ing.ingest(member(1))
        ing.ingest(tap(2, f"p:yes:{CLIENT}"))
        ing.ingest(client_message(3, text="archived"))
        ing.ingest(member(4, status="kicked"))
        ing.ingest(member(5))
        assert state(db)["status"] == "pending"
        ing.ingest(tap(6, f"p:no:{CLIENT}"))
        ing.ingest(client_message(7, text="never stored"))
    items = client.get(f"/v1/chats/{CLIENT}/messages").json()["items"]
    assert [i["text"] for i in items] == ["archived"]


def seed_pending(db, n, age="0 days", start=1):
    db.execute(
        """INSERT INTO chat_approvals(chat_id,status,title,requested_at)
        SELECT -7000-g,'pending','Spam '||g,now()-%s::interval-g*interval '1 second'
        FROM generate_series(%s::int,%s::int) g""",
        (age, start, start + n - 1),
    )


def listed(markup):
    return [b["callback_data"] for row in markup["inline_keyboard"] for b in row][
        : None if not nav(markup) else -len(nav(markup))
    ]


def nav(markup):
    return {
        b["text"]: b["callback_data"]
        for row in markup["inline_keyboard"]
        for b in row
        if b["callback_data"].startswith("l:page:")
    }


def test_chats_list_paginates_every_row(db, cfg):
    seed_pending(db, 95)
    total = db.execute("SELECT count(*) AS n FROM chat_approvals").fetchone()["n"]
    seen, page, pages = [], 0, []
    while True:
        text, markup = render(db, cfg, page)
        pages.append(nav(markup))
        seen += listed(markup)
        buttons = sum(len(r) for r in markup["inline_keyboard"])
        assert buttons <= 100 and len(text) <= 4096
        if "Далее" not in pages[-1]:
            break
        assert pages[-1]["Далее"] == f"l:page:{page + 1}"
        page += 1
    assert len(seen) == total == 97 and len(set(seen)) == total
    assert "Назад" not in pages[0] and pages[-1]["Назад"] == f"l:page:{page - 1}"
    assert all(set(p) == {"Назад", "Далее"} for p in pages[1:-1])
    # A page past the end (rows removed meanwhile) shows the last page, not an empty one.
    assert listed(render(db, cfg, 999)[1]) == listed(render(db, cfg, page)[1])


def test_chats_page_tap_edits_list_and_card_keeps_page(db, cfg):
    cfg = replace(cfg, dsn=db.info.dsn)
    seed_pending(db, 95)
    with Ingestor(db, cfg) as ing:
        ing.ingest(tap(1, "l:page:2", mid=600))
    bot = Bot()
    drain(db, cfg, bot)
    edit = next(p for m, p in bot.calls if m == "editMessageText")
    assert edit["message_id"] == 600 and edit["reply_markup"] == render(db, cfg, 2)[1]
    opened = listed(render(db, cfg, 2)[1])[0]
    assert opened.startswith("c:open:") and opened.endswith(":2")
    with Ingestor(db, cfg) as ing:
        ing.ingest(tap(2, opened.replace("c:open:", "c:no:"), mid=600))
    bot = Bot()
    drain(db, cfg, bot)
    edit = next(p for m, p in bot.calls if m == "editMessageText")
    assert "Состояние: выключен" in edit["text"]
    back = edit["reply_markup"]["inline_keyboard"][-1][-1]
    assert back == {"text": "← К списку", "callback_data": "l:page:2"}


@pytest.mark.parametrize(
    "data", ["l:page:-1", "l:page:01", "p:page:1", "l:page:100000", "l:page:", "l:page:1:2"]
)
def test_forged_page_data_refused(db, cfg, data):
    cfg = replace(cfg, dsn=db.info.dsn)
    with Ingestor(db, cfg) as ing:
        ing.ingest(tap(1, data))
    answers = db.execute("SELECT payload FROM jobs WHERE kind='owner_callback'").fetchall()
    assert [a["payload"]["answer"] for a in answers] == ["invalid"]
    bot = Bot()
    drain(db, cfg, bot)
    assert not {"editMessageText", "sendMessage"} & set(bot.methods())


def test_chats_list_shows_config_disabled_chat_as_off(db, cfg):
    cfg = replace(cfg, chats={**cfg.chats, -10: replace(cfg.chats[-10], enabled=False)})
    _, markup = render(db, cfg)
    texts = [b["text"] for row in markup["inline_keyboard"] for b in row]
    assert "-10 · выключен в конфиге" in texts
    assert "-20 · собираю" in texts


def test_stale_pending_expires_leaves_and_readd_asks_again(db, cfg):
    cfg = replace(cfg, dsn=db.info.dsn)
    # A full backlog of week-old spam adds must not block a fresh add.
    seed_pending(db, 100, age="7 days")
    with Ingestor(db, cfg) as ing:
        ing.ingest(member(1))
    assert state(db)["status"] == "pending"
    expired = db.execute(
        "SELECT count(*) AS n FROM chat_approvals WHERE status='expired'"
    ).fetchone()["n"]
    leaves = db.execute("SELECT count(*) AS n FROM jobs WHERE kind='leave'").fetchone()["n"]
    assert expired == leaves == 100
    bot = Bot()
    drain(db, cfg, bot)
    assert bot.methods().count("leaveChat") == 100
    db.execute(
        "UPDATE chat_approvals SET requested_at=now()-interval '8 days' WHERE chat_id=%s", (CLIENT,)
    )
    with Ingestor(db, cfg) as ing:
        ing.ingest(client_message(2))
        assert state(db)["status"] == "expired"
        ing.ingest(tap(3, f"p:yes:{CLIENT}"))  # a week-old prompt button is stale
        assert state(db)["status"] == "expired"
        ing.ingest(member(4, status="administrator"))
    assert state(db)["status"] == "pending"
    assert stored(db) == NOTHING
    prompts = db.execute(
        "SELECT count(*) AS n FROM jobs WHERE kind='approval_prompt' AND chat_id=%s", (CLIENT,)
    ).fetchone()["n"]
    assert prompts == 2


def test_configured_pending_chat_never_expires(db, cfg):
    cfg = replace(cfg, dsn=db.info.dsn)
    db.execute(
        "UPDATE chat_approvals SET status='pending',requested_at=now()-interval '30 days' "
        "WHERE chat_id=-20"
    )
    with Ingestor(db, cfg) as ing:
        ing.ingest(member(1))
    assert state(db, -20)["status"] == "pending"
    assert not db.execute("SELECT 1 FROM jobs WHERE kind='leave'").fetchone()


def test_non_owner_in_owner_dm_and_command_refused(db, cfg):
    cfg = replace(cfg, dsn=db.info.dsn)
    intruder = dm(1, "/chats", sender=OWNER)
    intruder["message"]["from"] = {"id": 4242}  # someone else's message in the owner DM
    owner_text = dm(2, "DO_NOT_RETAIN ordinary owner text")
    with Ingestor(db, cfg) as ing:
        ing.ingest(intruder)
        ing.ingest(owner_text)
        assert ing.offset == 3
    for body in (
        {"from": {"id": 4242}, "text": "/chats"},
        {"from": 4242, "text": "/chats"},
        {"from": {"id": str(OWNER)}, "text": "/chats"},
        {"text": "/chats"},
    ):
        command(db, cfg, body, "chats-list:forged")
    assert not db.execute("SELECT 1 FROM jobs").fetchone()
    assert count(db, "events") == 0
    command(db, cfg, {"from": {"id": OWNER}, "text": "/chats@oko_archive"}, "chats-list:ok")
    assert [r["kind"] for r in db.execute("SELECT kind FROM jobs").fetchall()] == ["chats_list"]
