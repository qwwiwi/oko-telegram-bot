"""/chats list opens a per-chat card; Stop lives only inside the card, behind a confirm."""

import itertools
import time
from dataclasses import replace

import pytest
from test_approvals import (
    CLIENT,
    OWNER,
    Bot,
    client_message,
    confirmed,
    drain,
    member,
    state,
    tap,
)

from archive import approvals
from archive.approvals import card, render
from archive.ingest import Ingestor

TITLE = "AI сопровождение Tagir"
# Each tap is a fresh Telegram update with its own callback id.
UPDATES = itertools.count(100)


def buttons(markup):
    return [b for row in markup["inline_keyboard"] for b in row]


def data(markup):
    return {b["text"]: b.get("callback_data") for b in buttons(markup)}


def approved(db, cfg, *messages):
    with Ingestor(db, cfg) as ing:
        ing.ingest(member(1, title=TITLE))
        ing.ingest(tap(2, f"p:yes:{CLIENT}"))
        for message in messages:
            ing.ingest(message)


def taps(db, cfg, *values, mid=500):
    with Ingestor(db, cfg) as ing:
        for value in values:
            ing.ingest(tap(next(UPDATES), value, mid=mid))
    bot = Bot()
    drain(db, cfg, bot)
    return bot


def edits(bot):
    return [p for m, p in bot.calls if m == "editMessageText"]


def client(uid, first, last=None, author=777, **extra):
    person = {"id": author, "first_name": first, "username": "client_login"}
    if last:
        person["last_name"] = last
    return client_message(uid, text="hello", **{"from": person, **extra})


def test_list_has_one_open_button_per_chat_and_no_stop(db, cfg):
    cfg = replace(cfg, dsn=db.info.dsn)
    with Ingestor(db, cfg) as ing:
        for uid, (chat, title) in enumerate(
            {-1001: "Active One", -1002: "Waiting Two", -1003: "Off Three"}.items(), start=1
        ):
            ing.ingest(member(uid, chat=chat, title=title))
        ing.ingest(tap(10, "p:yes:-1001"))
        ing.ingest(tap(11, "p:no:-1003"))
    text, markup = render(db, cfg)
    assert text.startswith("Чаты")
    labels = data(markup)
    assert labels["Active One · собираю"] == "c:open:-1001:0"
    assert labels["Waiting Two · ждёт решения"] == "c:open:-1002:0"
    assert labels["Off Three · на паузе"] == "c:open:-1003:0"
    for row in markup["inline_keyboard"]:
        assert len(row) == 1
    for b in buttons(markup):
        assert not b["text"].startswith(("Стоп", "Включить", "Да", "Нет"))
        assert not b["callback_data"].startswith(("c:stop", "c:yes", "c:no", "l:stop", "l:yes"))


def test_card_shows_state_client_counts_link_and_stop(db, cfg):
    cfg = replace(cfg, dsn=db.info.dsn)
    now = int(time.time())
    approved(
        db,
        cfg,
        client(3, "Tagir", "Ivanov", date=now),
        client(4, "Tagir", "Ivanov", date=now, document={"file_id": "f"}),
        client_message(5, text="owner", date=now, **{"from": {"id": OWNER, "first_name": "Owner"}}),
        client_message(
            6, text="bot", date=now, **{"from": {"id": 5, "first_name": "Helper", "is_bot": True}}
        ),
        client(7, "Old", author=778, date=1700000000),
    )
    text, markup = card(db, cfg, CLIENT, 3)
    lines = text.splitlines()
    assert lines[0] == f"«{TITLE}»"
    assert "Состояние: собираю" in lines
    assert "Клиент: Tagir Ivanov, Old" in lines
    assert "Сообщений: 5 (сегодня 4)" in lines
    assert "Вложений: 1" in lines
    assert any(line.startswith("Последнее сообщение: ") and line.endswith("МСК") for line in lines)
    assert "Owner" not in text and "Helper" not in text and "client_login" not in text
    assert "777" not in text
    rows = markup["inline_keyboard"]
    assert rows[0] == [{"text": "Открыть чат", "url": "https://t.me/c/5000000001/7"}]
    assert rows[-1] == [
        {"text": "Стоп", "callback_data": f"c:ask:{CLIENT}:3"},
        {"text": "← К списку", "callback_data": "l:page:3"},
    ]


def test_card_without_messages_and_non_supergroup_id_has_no_link(db, cfg):
    cfg = replace(cfg, dsn=db.info.dsn)
    with Ingestor(db, cfg) as ing:
        ing.ingest(member(1, chat=-4242, title="Small Group", kind="group"))
    text, markup = card(db, cfg, -4242, 0)
    assert "Состояние: ждёт ответа" in text and "Сообщений: 0 (сегодня 0)" in text
    assert "Клиент: —" in text and "Последнее сообщение: —" in text
    assert not any("url" in b for b in buttons(markup))
    assert markup["inline_keyboard"][-1] == [
        {"text": "Да", "callback_data": "c:yes:-4242:0"},
        {"text": "Нет", "callback_data": "c:no:-4242:0"},
        {"text": "← К списку", "callback_data": "l:page:0"},
    ]


def test_card_link_points_to_first_message_when_nothing_archived(db, cfg):
    cfg = replace(cfg, dsn=db.info.dsn)
    approved(db, cfg)
    _, markup = card(db, cfg, CLIENT, 0)
    assert markup["inline_keyboard"][0][0]["url"] == "https://t.me/c/5000000001/1"


def test_open_stop_confirm_and_enable_flow(db, cfg):
    cfg = replace(cfg, dsn=db.info.dsn)
    approved(db, cfg)
    bot = taps(db, cfg, f"c:open:{CLIENT}:0")
    (edit,) = edits(bot)
    assert edit["message_id"] == 500 and edit["text"].startswith(f"«{TITLE}»")
    assert state(db)["status"] == "active"

    bot = taps(db, cfg, f"c:ask:{CLIENT}:0")
    (edit,) = edits(bot)
    assert edit["text"] == f"Остановить сбор в «{TITLE}»?"
    stop = confirmed(db, CLIENT)
    assert data(edit["reply_markup"]) == {
        "Да, остановить": stop,
        "Отмена": f"c:open:{CLIENT}:0",
    }
    assert state(db)["status"] == "active"  # asking changes nothing

    bot = taps(db, cfg, f"c:open:{CLIENT}:0")  # «Отмена»
    assert state(db)["status"] == "active" and "Состояние: собираю" in edits(bot)[0]["text"]

    bot = taps(db, cfg, stop)
    assert state(db)["status"] == "stopped"
    (edit,) = edits(bot)
    assert "Состояние: выключен" in edit["text"]
    assert edit["reply_markup"]["inline_keyboard"][-1][0] == {
        "text": "Включить",
        "callback_data": f"c:yes:{CLIENT}:0",
    }
    # A repeated confirm (double tap) changes nothing: the state it was shown for is gone.
    taps(db, cfg, stop)
    assert state(db)["status"] == "stopped"

    bot = taps(db, cfg, f"c:yes:{CLIENT}:0")  # enable needs no confirmation
    assert state(db)["status"] == "active"
    answers = db.execute(
        "SELECT payload FROM jobs WHERE kind='owner_callback' ORDER BY id"
    ).fetchall()
    assert [a["payload"]["answer"] for a in answers][-3:] == ["stopped", "changed", "active"]


def test_back_to_list_keeps_page(db, cfg):
    cfg = replace(cfg, dsn=db.info.dsn)
    approved(db, cfg)
    bot = taps(db, cfg, "l:page:0")
    (edit,) = edits(bot)
    assert edit["text"] == render(db, cfg, 0)[0]


@pytest.mark.parametrize("value", ["l:stop:{c}", "l:stop:{c}:0", "l:yes:{c}:2", "l:no:{c}"])
def test_old_list_buttons_change_nothing(db, cfg, value):
    cfg = replace(cfg, dsn=db.info.dsn)
    approved(db, cfg)
    if value.startswith("l:yes"):
        taps(db, cfg, confirmed(db, CLIENT))
    before = state(db)["status"]
    bot = taps(db, cfg, value.format(c=CLIENT))
    assert state(db)["status"] == before
    answer = [p for m, p in bot.calls if m == "answerCallbackQuery"][-1]
    assert answer["text"] == "Список устарел, отправьте /chats"
    assert not edits(bot)


@pytest.mark.parametrize(
    "value",
    [
        "c:stop:-1009999999999:0:1",  # chat not in the approval table
        f"c:drop:{CLIENT}:0",
        f"c:stop:{CLIENT}",
        f"c:stop:{CLIENT}:0",  # no state version: never shown by a confirmation
        f"c:stop:{CLIENT}:0:",
        f"c:stop:{CLIENT}:0:01",
        f"c:stop:{CLIENT}:0:1:2",
        f"c:stop:{CLIENT}:01",
        f"c:open:{CLIENT}:0:1",
        f"c:ask:0{abs(CLIENT)}:0",
    ],
)
def test_forged_card_data_refused(db, cfg, value):
    cfg = replace(cfg, dsn=db.info.dsn)
    approved(db, cfg)
    bot = taps(db, cfg, value)
    assert state(db)["status"] == "active"
    assert [p["text"] for m, p in bot.calls if m == "answerCallbackQuery"][-1] == (
        "Неизвестная команда"
    )
    assert not edits(bot)


def test_non_owner_card_taps_ignored(db, cfg):
    cfg = replace(cfg, dsn=db.info.dsn)
    approved(db, cfg)
    with Ingestor(db, cfg) as ing:
        ing.ingest(tap(50, f"c:stop:{CLIENT}:0", sender=4242))
        ing.ingest(tap(51, f"c:open:{CLIENT}:0", sender=4242))
    assert state(db)["status"] == "active"
    bot = Bot()
    drain(db, cfg, bot)
    assert not edits(bot)
    refused = [p for m, p in bot.calls if m == "answerCallbackQuery" and p["show_alert"]]
    assert sorted(p["callback_query_id"] for p in refused) == ["9050", "9051"]


def test_config_disabled_chat_card_shows_kill_switch(db, cfg):
    cfg = replace(cfg, chats={**cfg.chats, -10: replace(cfg.chats[-10], enabled=False)})
    text, _ = card(db, cfg, -10, 0)
    assert "Состояние: выключен в конфиге" in text
    _, markup = render(db, cfg)
    assert "-10 · выключен в конфиге" in data(markup)


def test_migrated_chat_card_falls_back_to_list(db, cfg):
    db.execute("UPDATE chat_approvals SET migrated_to=-1007 WHERE chat_id=-20")
    assert card(db, cfg, -20, 0) == render(db, cfg, 0)


def test_every_callback_data_fits_telegram_limit(db, cfg):
    huge = -1009223372036854775
    db.execute(
        "INSERT INTO chat_approvals(chat_id,status,title) VALUES(%s,'active',%s)",
        (huge, "Ж" * 128),
    )
    db.execute("UPDATE chat_approvals SET status='pending' WHERE chat_id=-20")
    db.execute("UPDATE chat_approvals SET status='stopped' WHERE chat_id=-10")
    page = 99999
    values = [b.get("callback_data") for b in buttons(render(db, cfg, page)[1])]
    for chat in (huge, -10, -20):
        values += [b.get("callback_data") for b in buttons(card(db, cfg, chat, page)[1])]
        values += [b.get("callback_data") for b in buttons(card(db, cfg, chat, page, True)[1])]
    values = [v for v in values if v is not None]
    assert values and all(len(v.encode()) <= 64 for v in values)
    assert all(
        approvals.CARD.fullmatch(v) or approvals.STOP.fullmatch(v) or approvals.PAGE.fullmatch(v)
        for v in values
    )
    assert any(approvals.STOP.fullmatch(v) for v in values)


def test_link_only_for_supergroup_ids():
    assert approvals.link(-1005000000001, None) == "https://t.me/c/5000000001/1"
    assert approvals.link(-1005000000001, 9) == "https://t.me/c/5000000001/9"
    for chat in (-1001234567, -4242, -100, 42, -1000123456789):
        assert approvals.link(chat, 5) is None


def answer_text(bot):
    return [p.get("text") for m, p in bot.calls if m == "answerCallbackQuery"][-1]


def test_stop_with_wrong_state_version_changes_nothing(db, cfg):
    cfg = replace(cfg, dsn=db.info.dsn)
    approved(db, cfg)
    version = int(confirmed(db, CLIENT).rsplit(":", 1)[1])
    for forged in (version + 1, version - 1, 1):
        bot = taps(db, cfg, f"c:stop:{CLIENT}:0:{forged}")
        assert state(db)["status"] == "active"
        assert answer_text(bot) == "Состояние изменилось, откройте карточку заново"
        (edit,) = edits(bot)  # the current card, not the confirmation
        assert "Состояние: собираю" in edit["text"]


def test_stale_confirm_after_enable_changes_nothing(db, cfg):
    cfg = replace(cfg, dsn=db.info.dsn)
    approved(db, cfg)
    bot = taps(db, cfg, f"c:ask:{CLIENT}:0")
    old = data(edits(bot)[0]["reply_markup"])["Да, остановить"]
    taps(db, cfg, old)
    assert state(db)["status"] == "stopped"
    taps(db, cfg, f"c:yes:{CLIENT}:0")
    assert state(db)["status"] == "active"
    # The confirmation shown before the re-enable must not stop the chat again.
    bot = taps(db, cfg, old)
    assert state(db)["status"] == "active"
    assert answer_text(bot) == "Состояние изменилось, откройте карточку заново"
    # A fresh confirmation works.
    bot = taps(db, cfg, f"c:ask:{CLIENT}:0")
    fresh = data(edits(bot)[0]["reply_markup"])["Да, остановить"]
    assert fresh != old
    bot = taps(db, cfg, fresh)
    assert state(db)["status"] == "stopped"
    assert answer_text(bot) == "Сбор остановлен"


def test_confirm_made_while_stopped_cannot_stop_after_enable(db, cfg):
    cfg = replace(cfg, dsn=db.info.dsn)
    approved(db, cfg)
    taps(db, cfg, confirmed(db, CLIENT))
    stopped = confirmed(db, CLIENT)  # version of the stopped state
    taps(db, cfg, f"c:yes:{CLIENT}:0")
    taps(db, cfg, stopped)
    assert state(db)["status"] == "active"


@pytest.mark.parametrize(
    "raw, shown",
    [
        ("Tagir +7 (916) 123-45-67", "Tagir …"),
        ("Tagir 89161234567", "Tagir …"),
        ("Tagir 8-916-123 45 67", "Tagir …"),
        ("Call (916)1234567 now", "Call … now"),
        ("Ivan ivan.petrov+work@mail.example.ru", "Ivan …"),
        ("Ivan @ivan_petrov", "Ivan …"),
        ("Ivan https://example.com/path?a=1", "Ivan …"),
        ("Ivan www.example.com", "Ivan …"),
        ("Ivan t.me/ivan_petrov", "Ivan …"),
        ("Ivan https://t.me/+AbCdEf", "Ivan …"),
        ("Ivan telegram.me/ivan", "Ivan …"),
        ("Ivan tg://resolve?domain=ivan_petrov", "Ivan …"),
        ("Ivan TG://Resolve?Domain=ivan_petrov", "Ivan …"),
        ("Ivan tg://user?id=42 hi", "Ivan … hi"),
        ("Ivan(tg://resolve?domain=ivan)", "Ivan(…"),
        ("Ivan 2024", "Ivan 2024"),
        ("Room 123-456", "Room 123-456"),
        ("AI сопровождение Tagir", "AI сопровождение Tagir"),
    ],
)
def test_redact_contacts(raw, shown):
    assert approvals.redact(raw) == shown


def test_card_redacts_contacts_in_client_names(db, cfg):
    cfg = replace(cfg, dsn=db.info.dsn)
    approved(
        db,
        cfg,
        client(3, "Tagir", "+7 916 123-45-67", author=777, date=1700000001),
        client(4, "Anna mail@example.com", "@anna_k", author=778, date=1700000002),
        client(5, "Boris", "https://t.me/boris", author=779, date=1700000003),
    )
    text, _ = card(db, cfg, CLIENT, 0)
    line = next(line for line in text.splitlines() if line.startswith("Клиент: "))
    assert line == "Клиент: Boris …, Anna … …, Tagir …"
    for leaked in ("916", "4567", "mail@", "example", "@anna", "t.me", "boris"):
        assert leaked not in text


def test_card_redacts_tg_link_in_client_name(db, cfg):
    cfg = replace(cfg, dsn=db.info.dsn)
    approved(db, cfg, client(3, "Ivan", "tg://resolve?domain=ivan_petrov", author=777))
    text, _ = card(db, cfg, CLIENT, 0)
    assert "Клиент: Ivan …" in text.splitlines()
    for leaked in ("tg:", "resolve", "ivan_petrov"):
        assert leaked not in text


def test_card_redacts_tg_link_in_title(db, cfg):
    cfg = replace(cfg, dsn=db.info.dsn)
    with Ingestor(db, cfg) as ing:
        ing.ingest(member(1, title="Ivan TG://resolve?domain=ivan_petrov"))
        ing.ingest(tap(2, f"p:yes:{CLIENT}"))
    text, _ = card(db, cfg, CLIENT, 0)
    assert text.splitlines()[0] == "«Ivan …»"
    assert "ivan_petrov" not in text


def test_card_and_list_redact_contacts_in_title(db, cfg):
    cfg = replace(cfg, dsn=db.info.dsn)
    title = "Tagir +7 916 123 45 67 tagir@example.com @tagir t.me/tagir"
    with Ingestor(db, cfg) as ing:
        ing.ingest(member(1, title=title))
        ing.ingest(tap(2, f"p:yes:{CLIENT}"))
    shown = "Tagir … … … …"
    text, _ = card(db, cfg, CLIENT, 0)
    assert text.splitlines()[0] == f"«{shown}»"
    assert card(db, cfg, CLIENT, 0, True)[0] == f"Остановить сбор в «{shown}»?"
    _, markup = render(db, cfg)
    assert f"{shown} · собираю" in data(markup)
    assert (
        approvals.prompt(CLIENT, title)[0] == f"Меня добавили в чат «{shown}». Собирать сообщения?"
    )
    # The numeric ID fallback of an untitled chat is not a contact.
    assert approvals.prompt(CLIENT, "")[0].startswith(f"Меня добавили в чат «{CLIENT}»")
