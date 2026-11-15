"""One session-owned ingestion connection. Connection loss ends ownership."""

import logging
import time

import psycopg

from archive.db import connect, enqueue
from archive.ingest import Ingestor, MappingRetry, OwnershipLost
from archive.telegram import BotAPI, BotError, PollConflict


def poll_once(ing, bot):
    updates = bot.updates(ing.fetch_offset())
    with ing.conn.transaction():
        ing.guard()
        ing.conn.execute("UPDATE poll_state SET last_poll=now()")
    for update in updates:
        ing.ingest(update)


def run(cfg):
    with connect(cfg.dsn) as conn, BotAPI(cfg) as bot:
        bot.preflight()
        with Ingestor(conn, cfg) as ing:
            failures = 0
            while True:
                try:
                    poll_once(ing, bot)
                    failures = 0
                except (PollConflict, OwnershipLost):
                    raise
                except (psycopg.Error, BotError, MappingRetry, ValueError, KeyError, TypeError):
                    if conn.closed or conn.broken:
                        raise OwnershipLost("ingestion_connection_lost") from None
                    failures += 1
                    with conn.transaction():
                        ing.guard()
                        conn.execute("UPDATE poll_state SET failures=%s", (failures,))
                        if failures >= cfg.alert_threshold:
                            enqueue(
                                conn,
                                "poller-failure:" + str(ing.epoch),
                                "founder_alert",
                                payload={"code": "poller_failure"},
                            )
                    logging.getLogger(__name__).warning("poll_retry")
                    time.sleep(min(60, 2 ** min(failures, 6)))
