"""Exercise the cloud 20 MB boundary through BotAPI streaming + real filesystem publish.

The Telegram transport is synthetic (streamed, no Content-Length); this is not live
Telegram evidence.
"""

import hashlib
import json
import os
import resource
import sys
import tempfile
import time
from pathlib import Path

import httpx

from archive.config import CLOUD_DOWNLOAD_LIMIT, Agent, Chat, Settings
from archive.storage import publish_stream
from archive.telegram import BotAPI, FileTooBig

PIECE = b"\x5a" * 65536


def stream(size):
    sent = 0
    while sent < size:
        piece = PIECE[: min(len(PIECE), size - sent)]
        sent += len(piece)
        yield piece


def main():
    started = time.monotonic()
    root = Path(os.environ["TMPDIR"])
    with tempfile.TemporaryDirectory(prefix="archive-20mb-", dir=root) as value:
        work = Path(value)
        cfg = Settings(
            dsn="synthetic",
            bot_token="1234567890:" + "synthetic_smoke_" + "z" * 19,
            expected_bot_id=1234567890,
            owner_tg_id=1000000001,
            founder_id=999,
            media_root=work / "media",
            export_root=work / "output",
            chats={-10: Chat("smoke")},
            agents={"smoke": Agent(hashlib.sha256(b"s" * 32).hexdigest(), (-10,))},
        )
        limit = CLOUD_DOWNLOAD_LIMIT
        sizes = {"documents/exact": limit, "documents/over": limit + 1}

        def handler(request):
            name = request.url.path.split("/", 3)[3]
            return httpx.Response(200, content=stream(sizes[name]))

        with BotAPI(cfg, httpx.MockTransport(handler)) as bot:
            with bot.download("documents/exact", CLOUD_DOWNLOAD_LIMIT) as chunks:
                copied, checksum = publish_stream(
                    cfg.media_root, "exact/lease/file", chunks, CLOUD_DOWNLOAD_LIMIT
                )
            expected = hashlib.sha256()
            for piece in stream(CLOUD_DOWNLOAD_LIMIT):
                expected.update(piece)
            published = cfg.media_root / "exact/lease/file"
            assert copied == limit == published.stat().st_size
            assert checksum == expected.hexdigest()
            try:
                with bot.download("documents/over", CLOUD_DOWNLOAD_LIMIT) as chunks:
                    publish_stream(cfg.media_root, "over/lease/file", chunks, CLOUD_DOWNLOAD_LIMIT)
            except FileTooBig:
                pass
            else:
                raise AssertionError("20 MB + 1 must be refused")
            assert not (cfg.media_root / "over/lease/file").exists()
            assert not list(cfg.media_root.rglob(".partial-*"))
        rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        rss_bytes = rss if sys.platform == "darwin" else rss * 1024
        print(
            json.dumps(
                {
                    "result": "PASS",
                    "scope": "synthetic streamed transport + real local filesystem",
                    "bytes_copied": copied,
                    "sha256": checksum,
                    "over_limit_refused": True,
                    "max_rss_bytes": rss_bytes,
                    "elapsed_seconds": round(time.monotonic() - started, 3),
                }
            )
        )


if __name__ == "__main__":
    main()
