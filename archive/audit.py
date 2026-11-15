"""Read-API audit rows and per-agent request windows. No queries, bodies, tokens or content."""

import math
import threading
from collections import deque

from archive.db import connect

# Unknown-token rows are capped per process so anonymous traffic cannot grow the table.
ANONYMOUS_PER_MINUTE = 60
WINDOW_SECONDS = 60


def record(dsn, agent, route, chat_id, status, result_count):
    with connect(dsn) as conn:
        conn.execute(
            "INSERT INTO read_audit(agent,route,chat_id,status,result_count) "
            "VALUES(%s,%s,%s,%s,%s)",
            (agent, route, chat_id, status, result_count),
        )


def prune(conn, days):
    """Delete audit rows older than ``days``; returns the deleted count."""
    return conn.execute(
        "DELETE FROM read_audit WHERE at < now() - make_interval(days => %s)", (days,)
    ).rowcount


class Windows:
    """Sliding one-minute request log per key, valid only within this one API process."""

    def __init__(self, clock):
        self.clock = clock
        self.hits = {}
        self.lock = threading.Lock()

    def take(self, key, limit):
        """Count one request; returns 0 if allowed, else whole seconds until a slot frees."""
        with self.lock:
            now = self.clock()
            hits = self.hits.setdefault(key, deque())
            while hits and hits[0] <= now - WINDOW_SECONDS:
                hits.popleft()
            if len(hits) >= limit:
                return max(1, math.ceil(hits[0] + WINDOW_SECONDS - now))
            hits.append(now)
            return 0
