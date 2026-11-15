"""Cloud Bot API over HTTPS only. Exceptions never carry URLs, response bodies or paths.

Every request URL contains the bot token (method calls and file downloads alike), so
URLs are never logged, stored, returned or embedded in exception messages.
"""

import re
import time
from contextlib import contextmanager

import httpx

from archive.config import quiet_http
from archive.ingest import KINDS

# Telegram cloud file paths look like "documents/file_12.pdf"; reject anything else.
SEGMENT = re.compile(r"[A-Za-z0-9_-][A-Za-z0-9_.-]{0,127}")


class BotError(RuntimeError):
    pass


class PollConflict(BotError):
    pass


class LeaveRejected(BotError):
    pass


class FileTooBig(BotError):
    """Telegram refused or exceeded the cloud download limit: terminal, never retried."""


class BotAPI:
    def __init__(self, cfg, transport=None):
        quiet_http()
        self.cfg = cfg
        self.client = httpx.Client(
            base_url=cfg.api_base,
            transport=transport,
            timeout=httpx.Timeout(50, connect=5),
            trust_env=False,
            follow_redirects=False,
        )

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.client.close()

    def call(self, method, params=None):
        if method == "leaveChat":
            # A queued unknown-chat job must never use another bot's credential.
            self.verify_identity()
        try:
            response = self.client.post(
                "/bot" + self.cfg.bot_token + "/" + method,
                json=params or {},
                timeout=httpx.Timeout(50, connect=5),
            )
            if response.status_code == 409:
                raise PollConflict("poll_conflict")
            if method == "leaveChat" and response.status_code in {400, 403}:
                raise LeaveRejected("leave_rejected")
            if method == "getFile" and response.status_code == 400 and too_big(response):
                raise FileTooBig("file_too_big")
            if response.status_code != 200:
                raise BotError("bot_http_error")
            data = response.json()
            if not isinstance(data, dict) or not data.get("ok") or "result" not in data:
                raise BotError("bot_response_error")
            return data["result"]
        except (httpx.HTTPError, ValueError):
            raise BotError("bot_transport_error") from None

    def verify_identity(self):
        me = self.call("getMe")
        if not isinstance(me, dict) or me.get("id") != self.cfg.expected_bot_id:
            raise BotError("bot_identity_mismatch")

    def preflight(self):
        self.verify_identity()
        webhook = self.call("getWebhookInfo")
        if not isinstance(webhook, dict) or webhook.get("url") != "":
            raise BotError("webhook_present")

    def updates(self, offset):
        updates = self.call(
            "getUpdates",
            {"offset": offset, "timeout": 30, "limit": 100, "allowed_updates": list(KINDS)},
        )
        if not isinstance(updates, list):
            raise BotError("invalid_updates")
        return updates

    def get_file(self, file_id):
        """Return (relative cloud file_path, declared size or None)."""
        result = self.call("getFile", {"file_id": file_id})
        if not isinstance(result, dict):
            raise BotError("invalid_file_result")
        path, size = result.get("file_path"), result.get("file_size")
        if not safe_file_path(path):
            raise BotError("invalid_file_result")
        if size is not None and (type(size) is not int or size < 0):
            raise BotError("invalid_file_result")
        return path, size

    @contextmanager
    def download(self, file_path, maximum):
        """Stream a cloud file as bounded chunks; never follows redirects.

        Yields an iterator of byte chunks. Raises FileTooBig when the response
        declares or delivers more than ``maximum`` bytes.
        """
        if not safe_file_path(file_path):
            raise BotError("invalid_file_result")
        deadline = time.monotonic() + self.cfg.download_seconds
        try:
            with self.client.stream(
                "GET",
                "/file/bot" + self.cfg.bot_token + "/" + file_path,
                # Raw wire bytes are bounded and hashed; no decompression bombs.
                headers={"Accept-Encoding": "identity"},
                timeout=httpx.Timeout(60, connect=5),
            ) as response:
                if response.status_code != 200:
                    raise BotError("file_http_error")
                if response.headers.get("content-encoding", "identity") != "identity":
                    raise BotError("file_response_error")
                length = response.headers.get("content-length")
                if length is not None:
                    if not length.isdigit():
                        raise BotError("file_response_error")
                    if int(length) > maximum:
                        raise FileTooBig("file_too_big")
                yield self._chunks(response, maximum, deadline, length)
        except httpx.HTTPError:
            raise BotError("file_transport_error") from None

    @staticmethod
    def _chunks(response, maximum, deadline, length):
        total = 0
        # Content-Encoding is identity-only (checked above), so these are the wire bytes.
        # Unsized iteration yields every network read, so the deadline also bounds a
        # slow drip that stays under the per-read timeout.
        for chunk in response.iter_bytes():
            total += len(chunk)
            if total > maximum:
                raise FileTooBig("file_too_big")
            if time.monotonic() > deadline:
                raise BotError("file_download_deadline")
            yield chunk
        if length is not None and total != int(length):
            raise BotError("file_size_mismatch")


def safe_file_path(path):
    if not isinstance(path, str) or not 0 < len(path) <= 256:
        return False
    segments = path.split("/")
    return all(SEGMENT.fullmatch(part) and ".." not in part for part in segments)


def too_big(response):
    # Telegram: {"ok":false,"error_code":400,"description":"Bad Request: file is too big"}.
    try:
        data = response.json()
    except ValueError:
        return False
    description = data.get("description") if isinstance(data, dict) else None
    return isinstance(description, str) and "file is too big" in description.lower()
