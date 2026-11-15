"""Strict TOML settings; no implicit scopes and no credential defaults."""

import hashlib
import hmac
import logging
import os
import re
import stat
import tomllib
from dataclasses import dataclass, field
from pathlib import Path
from urllib.parse import urlsplit

CLOUD_API_BASE = "https://api.telegram.org"
# Cloud Bot API getFile limit (20 MB). Larger files are kept as metadata only.
CLOUD_DOWNLOAD_LIMIT = 20 * 1024 * 1024
BOT_TOKEN = re.compile(r"([0-9]{1,20}):[A-Za-z0-9_-]{20,128}")
DERIVED_SLUG = re.compile(r"chat-?[0-9]+")


def quiet_http():
    for name in ("httpx", "httpcore", "uvicorn.access"):
        logging.getLogger(name).disabled = True


@dataclass(frozen=True)
class Chat:
    """Optional per-chat override. Approval itself is the owner's bot flow, not config."""

    slug: str
    transcription: bool = False
    enabled: bool = True

    def __post_init__(self):
        if type(self.transcription) is not bool or type(self.enabled) is not bool:
            raise ValueError("explicit_boolean_consent_required")


@dataclass(frozen=True)
class Agent:
    """Read-API identity. Capabilities default closed: no file bytes unless ``files``."""

    sha256: str
    chats: tuple[int, ...]
    # GET /v1/files bytes; metadata and transcripts stay readable without it.
    files: bool = False
    # Requests per sliding minute, enforced in the single API process.
    rate_per_minute: int = 60


@dataclass(frozen=True)
class Settings:
    dsn: str = field(repr=False)
    bot_token: str = field(repr=False)
    expected_bot_id: int
    # Telegram user who may approve/stop chats through the bot (the only decider).
    owner_tg_id: int
    # Telegram chat that receives operational alerts; usually the same user as the owner.
    founder_id: int
    media_root: Path
    export_root: Path
    chats: dict[int, Chat]
    agents: dict[str, Agent] = field(repr=False)
    api_base: str = CLOUD_API_BASE
    transcription_enabled: bool = False
    groq_key: str = field(default="", repr=False)
    assemblyai_key: str = field(default="", repr=False)
    lease_seconds: int = 120
    max_attempts: int = 5
    max_file_bytes: int = CLOUD_DOWNLOAD_LIMIT
    download_seconds: int = 600
    alert_threshold: int = 5
    # Owner's team members: never shown as the client on a /chats card.
    staff: tuple[int, ...] = ()

    def __post_init__(self):
        if type(self.transcription_enabled) is not bool:
            raise ValueError("explicit_boolean_consent_required")
        if (
            not self.dsn
            or not self.bot_token
            or self.bot_token.lower()
            in {
                "change_me",
                "placeholder",
                "changeme",
                "example",
                "todo",
            }
        ):
            raise ValueError("credentials_unconfigured")
        # The token becomes a URL path segment: accept only the BotFather shape whose
        # numeric prefix equals the pinned bot ID, before any network call.
        if type(self.expected_bot_id) is not int or self.expected_bot_id <= 0:
            raise ValueError("archive_bot_required")
        token = BOT_TOKEN.fullmatch(self.bot_token)
        if not token or int(token.group(1)) != self.expected_bot_id:
            raise ValueError("archive_bot_token_required")
        if self.founder_id <= 0 or type(self.owner_tg_id) is not int or self.owner_tg_id <= 0:
            raise ValueError("identity_unconfigured")
        if type(self.staff) is not tuple or any(type(i) is not int or i <= 0 for i in self.staff):
            raise ValueError("invalid_staff")
        parsed = urlsplit(self.api_base) if isinstance(self.api_base, str) else None
        if (
            parsed is None
            or parsed.scheme != "https"
            or parsed.hostname != "api.telegram.org"
            or parsed.port is not None
            or parsed.username
            or parsed.password
            or parsed.query
            or parsed.fragment
            or parsed.path not in ("", "/")
        ):
            raise ValueError("cloud_bot_api_https_required")
        for root in (self.media_root, self.export_root):
            if not root.is_absolute():
                raise ValueError("absolute_roots_required")
        media, export = self.media_root.resolve(), self.export_root.resolve()
        if media.is_relative_to(export) or export.is_relative_to(media):
            raise ValueError("disjoint_storage_roots_required")
        slugs = [c.slug for c in self.chats.values()]
        if (
            len(set(slugs)) != len(slugs)
            or any(not re.fullmatch(r"[a-z][a-z0-9-]{0,62}", s) for s in slugs)
            or any(type(i) is not int or i == 0 for i in self.chats)
            # Derived slugs are reserved so an override cannot shadow another chat.
            or any(
                DERIVED_SLUG.fullmatch(c.slug) and c.slug != derived_slug(i)
                for i, c in self.chats.items()
            )
        ):
            raise ValueError("invalid_chats")
        # No agents is valid: the read API then answers 401 to every request, while the
        # CLI export still works.
        seen = set()
        for agent in self.agents.values():
            if (
                not re.fullmatch(r"[0-9a-f]{64}", agent.sha256)
                or agent.sha256 == "0" * 64
                or not agent.chats
                or any(type(i) is not int or i == 0 for i in agent.chats)
                or agent.sha256 in seen
            ):
                raise ValueError("explicit_agent_scopes_required")
            seen.add(agent.sha256)
            if (
                type(agent.files) is not bool
                or type(agent.rate_per_minute) is not int
                or not 1 <= agent.rate_per_minute <= 100_000
            ):
                raise ValueError("invalid_agent_limits")
        if not 5 <= self.lease_seconds <= 3600 or not 1 <= self.max_attempts <= 20:
            raise ValueError("invalid_worker_bounds")
        if (
            type(self.max_file_bytes) is not int
            or not 1 <= self.max_file_bytes <= CLOUD_DOWNLOAD_LIMIT
            or not 10 <= self.download_seconds <= 3600
            or self.alert_threshold < 1
        ):
            raise ValueError("invalid_limits")
        quiet_http()

    def allows(self, chat_id):
        """Operator kill switch only: ``enabled = false`` revokes an approved chat.

        Never sufficient on its own; collection and reads also require the owner's
        approval in chat_approvals (see archive.approvals).
        """
        return chat_id not in self.chats or self.chats[chat_id].enabled

    def slug(self, chat_id):
        chat = self.chats.get(chat_id)
        return chat.slug if chat else derived_slug(chat_id)

    def transcription_consent(self, chat_id):
        chat = self.chats.get(chat_id)
        return bool(chat and chat.transcription)

    def agent(self, token):
        """Name of the agent holding ``token``; every hash is compared, no early exit."""
        digest = hashlib.sha256(token.encode()).hexdigest()
        name = None
        for key, agent in self.agents.items():
            if hmac.compare_digest(digest, agent.sha256) and len(token.encode()) >= 32:
                name = key
        return name

    def scopes(self, token):
        """Configured numeric scopes; the API narrows them to owner-approved chats."""
        name = self.agent(token)
        if name is None:
            return ()
        return tuple(i for i in self.agents[name].chats if self.allows(i))


def derived_slug(chat_id):
    """Deterministic slug for a bot-approved chat without a configured override."""
    # Group/channel IDs are negative ("chat-100..."); the sign keeps slugs unique.
    return "chat" + str(chat_id)


def load(path: Path) -> Settings:
    """Secret-free private TOML plus explicit service environment credentials."""
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    with os.fdopen(fd, "rb") as handle:
        info = os.fstat(handle.fileno())
        if not stat.S_ISREG(info.st_mode) or info.st_mode & 0o077:
            raise ValueError("private_config_permissions_required")
        data = tomllib.load(handle)
    secrets = {
        "dsn": "ARCHIVE_DATABASE_DSN",
        "bot_token": "ARCHIVE_BOT_TOKEN",
        "groq_key": "GROQ_API_KEY",
        "assemblyai_key": "ASSEMBLYAI_API_KEY",
    }
    if set(data) & set(secrets):
        raise ValueError("environment_secrets_required")
    if set(data) & {"sidecar_root", "sidecar_session"}:
        # Local Bot API is deferred: stale sidecar settings are refused, not ignored.
        raise ValueError("local_bot_api_settings_removed")
    data.update({key: os.environ.get(variable, "") for key, variable in secrets.items()})
    # Alerts go to the owner unless a separate alert chat is configured.
    if "owner_tg_id" in data:
        data.setdefault("founder_id", data["owner_tg_id"])
    data["chats"] = {int(i): Chat(**v) for i, v in data.get("chats", {}).items()}
    # Missing capability keys take the closed defaults (no files, 60 requests/minute).
    data["agents"] = {
        k: Agent(
            v["sha256"],
            tuple(v["chats"]),
            v.get("files", False),
            v.get("rate_per_minute", 60),
        )
        for k, v in data.get("agents", {}).items()
    }
    if "staff" in data:
        data["staff"] = tuple(data["staff"]) if isinstance(data["staff"], list) else None
    for key in ("media_root", "export_root"):
        data[key] = Path(data[key])
    return Settings(**data)
