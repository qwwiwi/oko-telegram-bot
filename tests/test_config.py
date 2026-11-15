import hashlib
from dataclasses import replace

import pytest

from archive.config import CLOUD_DOWNLOAD_LIMIT, Agent, Chat, Settings, load

# Obviously synthetic Telegram IDs: no real bot or user.
SYNTHETIC_BOT_ID = 1234567890
SYNTHETIC_OWNER_ID = 1000000001
# Built by concatenation so the source never contains a token-shaped literal.
SYNTHETIC_TOKEN = str(SYNTHETIC_BOT_ID) + ":" + "synthetic_local_only_" + "x" * 14


def settings(tmp_path):
    return Settings(
        dsn="synthetic",
        bot_token=SYNTHETIC_TOKEN,
        expected_bot_id=SYNTHETIC_BOT_ID,
        owner_tg_id=SYNTHETIC_OWNER_ID,
        founder_id=999,
        media_root=tmp_path / "media",
        export_root=tmp_path / "output",
        chats={-10: Chat("alpha"), -20: Chat("beta")},
        agents={"reader": Agent(hashlib.sha256(b"x" * 32).hexdigest(), (-10,))},
    )


def test_fail_closed_configuration(tmp_path):
    cfg = settings(tmp_path)
    assert not cfg.transcription_enabled and not cfg.chats[-10].transcription
    assert cfg.scopes("x" * 32) == (-10,)
    assert cfg.scopes("bad") == ()
    assert (
        replace(cfg, chats={**cfg.chats, -10: Chat("alpha", enabled=False)}).scopes("x" * 32) == ()
    )
    for changes in [
        {"agents": {"r": Agent("0" * 64, ())}},
        {"agents": {"r": Agent("placeholder", (-10,))}},
        {"chats": {-10: Chat("same"), -20: Chat("same")}},
        {"chats": {-10: Chat("../escape")}},
        {"expected_bot_id": 0},
        {"bot_token": "CHANGE_ME"},
        {"bot_token": "synthetic-local-only"},
        # A token of another bot must never be accepted for the pinned bot ID.
        {"bot_token": "2000000002:" + "x" * 35},
        {"bot_token": str(SYNTHETIC_BOT_ID) + ":" + "x" * 30 + "/../getMe"},
        {"api_base": "http://api.telegram.org"},
        {"api_base": "http://bot-api:8081"},
        {"api_base": "https://localhost"},
        {"api_base": "https://api.telegram.org.evil.example"},
        {"api_base": "https://api.telegram.org:8443"},
        {"api_base": "https://user@api.telegram.org"},
        {"api_base": "https://api.telegram.org/bot"},
        {"api_base": "https://api.telegram.org?x=1"},
        {"max_file_bytes": CLOUD_DOWNLOAD_LIMIT + 1},
        {"max_file_bytes": 2 * 1024**3},
        {"media_root": tmp_path / "output" / "media"},
    ]:
        with pytest.raises(ValueError):
            replace(cfg, **changes)


def test_cloud_defaults(tmp_path):
    cfg = settings(tmp_path)
    assert cfg.api_base == "https://api.telegram.org"
    assert cfg.max_file_bytes == CLOUD_DOWNLOAD_LIMIT == 20 * 1024 * 1024
    assert cfg.bot_token not in repr(cfg)
    assert replace(cfg, api_base="https://api.telegram.org/").api_base.startswith("https://")


def test_owner_and_bot_managed_chat_defaults(tmp_path):
    cfg = settings(tmp_path)
    assert cfg.owner_tg_id == SYNTHETIC_OWNER_ID
    # Configured chats only override slug/consent; the approval gate is the owner's bot flow.
    assert cfg.slug(-10) == "alpha" and cfg.slug(-1005000000001) == "chat-1005000000001"
    assert not cfg.transcription_consent(-1005000000001)
    open_scope = replace(cfg, chats={}, agents={"r": Agent("a" * 64, (-1005000000001,))})
    assert open_scope.scopes("x" * 32) == ()
    for changes in [
        {"owner_tg_id": 0},
        {"owner_tg_id": str(SYNTHETIC_OWNER_ID)},
        {"chats": {-10: Chat("chat-20")}},
        {"agents": {"r": Agent("a" * 64, (0,))}},
    ]:
        with pytest.raises(ValueError):
            replace(cfg, **changes)


def test_staff_ids_are_validated(tmp_path):
    cfg = settings(tmp_path)
    assert cfg.staff == () and replace(cfg, staff=(5, 6)).staff == (5, 6)
    for staff in ([5], (0,), ("5",), (True,)):
        with pytest.raises(ValueError):
            replace(cfg, staff=staff)


def test_agent_capabilities_default_closed_and_are_validated(tmp_path, monkeypatch):
    cfg = settings(tmp_path)
    reader = cfg.agents["reader"]
    assert reader.files is False and reader.rate_per_minute == 60
    assert cfg.agent("x" * 32) == "reader" and cfg.agent("bad") is None
    digest = reader.sha256
    for agent in [
        Agent(digest, (-10,), files=1),
        Agent(digest, (-10,), files="true"),
        Agent(digest, (-10,), rate_per_minute=0),
        Agent(digest, (-10,), rate_per_minute=True),
        Agent(digest, (-10,), rate_per_minute="60"),
        Agent(digest, (-10,), rate_per_minute=100_001),
    ]:
        with pytest.raises(ValueError, match="invalid_agent_limits"):
            replace(cfg, agents={"r": agent})
    monkeypatch.setenv("ARCHIVE_DATABASE_DSN", "synthetic")
    monkeypatch.setenv("ARCHIVE_BOT_TOKEN", SYNTHETIC_TOKEN)
    base = f"""expected_bot_id = {SYNTHETIC_BOT_ID}
owner_tg_id = {SYNTHETIC_OWNER_ID}
founder_id = 999
media_root = "{tmp_path / "media"}"
export_root = "{tmp_path / "output"}"
[agents.reader]
sha256 = "{digest}"
chats = [-10]
"""
    path = tmp_path / "settings.toml"
    for extra, expected in [
        ("", (False, 60)),
        ("files = true\nrate_per_minute = 120\n", (True, 120)),
    ]:
        path.write_text(base + extra)
        path.chmod(0o600)
        agent = load(path).agents["reader"]
        assert (agent.files, agent.rate_per_minute) == expected
    path.write_text(base + 'files = "true"\n')
    with pytest.raises(ValueError, match="invalid_agent_limits"):
        load(path)


def _minimal_toml(tmp_path, owner=True):
    owner_line = f"owner_tg_id = {SYNTHETIC_OWNER_ID}\n" if owner else ""
    path = tmp_path / "settings.toml"
    path.write_text(
        f"expected_bot_id = {SYNTHETIC_BOT_ID}\n{owner_line}"
        f'media_root = "{tmp_path / "media"}"\nexport_root = "{tmp_path / "output"}"\n'
    )
    path.chmod(0o600)
    return path


def test_agents_are_optional_and_alerts_default_to_owner(tmp_path, monkeypatch):
    # No agents: the read API has no valid token at all; the owner receives alerts.
    cfg = replace(settings(tmp_path), agents={})
    assert cfg.agents == {} and cfg.scopes("x" * 32) == () and cfg.agent("x" * 32) is None
    monkeypatch.setenv("ARCHIVE_DATABASE_DSN", "synthetic")
    monkeypatch.setenv("ARCHIVE_BOT_TOKEN", SYNTHETIC_TOKEN)
    loaded = load(_minimal_toml(tmp_path))
    assert loaded.agents == {} and loaded.founder_id == SYNTHETIC_OWNER_ID


def test_owner_is_required(tmp_path, monkeypatch):
    monkeypatch.setenv("ARCHIVE_DATABASE_DSN", "synthetic")
    monkeypatch.setenv("ARCHIVE_BOT_TOKEN", SYNTHETIC_TOKEN)
    with pytest.raises(TypeError):
        load(_minimal_toml(tmp_path, owner=False))
