from dataclasses import replace
from types import SimpleNamespace

import httpx
import pytest
from test_config import SYNTHETIC_BOT_ID, settings

from archive.telegram import BotAPI, BotError

OTHER_ID = 2000000002


@pytest.mark.parametrize("expected_id", [OTHER_ID, 1, 0, -5])
def test_token_prefix_must_equal_pinned_bot_id(tmp_path, expected_id):
    with pytest.raises(ValueError, match="archive_bot_(token_)?required"):
        replace(settings(tmp_path), expected_bot_id=expected_id)


def test_pinned_bot_id_must_be_an_integer(tmp_path):
    with pytest.raises(ValueError, match="archive_bot_required"):
        replace(settings(tmp_path), expected_bot_id=str(SYNTHETIC_BOT_ID))


@pytest.mark.parametrize(
    ("actual_id", "operation", "prior_preflight"),
    [
        (OTHER_ID, "preflight", False),
        (OTHER_ID, "leaveChat", False),
        (OTHER_ID, "leaveChat", True),
        (SYNTHETIC_BOT_ID, "leaveChat", False),
        (SYNTHETIC_BOT_ID, "leaveChat", True),
        (SYNTHETIC_BOT_ID, "preflight", False),
    ],
)
def test_bot_boundary_checks_actual_bot_identity(tmp_path, actual_id, operation, prior_preflight):
    # Bypass Settings validation to independently exercise the transport boundary.
    cfg = SimpleNamespace(**vars(settings(tmp_path)))
    calls = []
    current_id = SYNTHETIC_BOT_ID if prior_preflight else actual_id

    def handler(req):
        method = req.url.path.rsplit("/", 1)[-1]
        calls.append(method)
        result = {"getMe": {"id": current_id}, "getWebhookInfo": {"url": ""}}.get(method, True)
        return httpx.Response(200, json={"ok": True, "result": result})

    with BotAPI(cfg, httpx.MockTransport(handler)) as bot:
        if prior_preflight:
            bot.preflight()
            calls.clear()
            cfg.bot_token = "synthetic-replaced-credential"
            current_id = actual_id

        def invoke():
            if operation == "preflight":
                bot.preflight()
            else:
                bot.call("leaveChat", {"chat_id": -1000000000001})

        if actual_id == SYNTHETIC_BOT_ID:
            invoke()
            assert calls == ["getMe", "getWebhookInfo" if operation == "preflight" else operation]
        else:
            with pytest.raises(BotError, match="bot_identity_mismatch"):
                invoke()
            assert "leaveChat" not in calls and "getWebhookInfo" not in calls
            assert set(calls) <= {"getMe"}
