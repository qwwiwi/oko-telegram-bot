import base64
import os

import psycopg
import pytest
from test_ingest import count, update

from archive.ingest import Ingestor, MappingRetry


@pytest.fixture
def raw_index(db):
    """A real btree on raw makes PostgreSQL raise SQLSTATE 54000 for large payloads."""

    def create(where=""):
        db.execute("CREATE INDEX test_raw_limit ON events(raw) " + where)

    yield create
    db.execute("DROP INDEX IF EXISTS test_raw_limit")


def oversized(uid):
    # Incompressible text: the index tuple exceeds the btree row limit (2704 bytes).
    return update(uid, mid=uid, text=base64.b64encode(os.urandom(12000)).decode())


def test_class_54_main_insert_uses_failure_budget_then_minimal_quarantine(db, cfg, raw_index):
    raw_index()
    with pytest.raises(psycopg.errors.ProgramLimitExceeded):
        db.execute(
            "INSERT INTO events(fingerprint,update_id,raw) VALUES('probe',0,%s)",
            (os.urandom(12000),),
        )
    poison = oversized(1)
    for attempt in range(3):
        with Ingestor(db, cfg) as ing:
            if attempt < 2:
                # Not quarantined+acked on first sight: bounded retry like other mapping errors.
                with pytest.raises(MappingRetry):
                    ing.ingest(poison)
                assert ing.offset == 0 and count(db, "events") == 0
            else:
                ing.ingest(poison)
                assert ing.offset == 2
    row = db.execute("SELECT raw,error_code,chat_id FROM events").fetchone()
    assert row["raw"] is None and row["chat_id"] == -10
    assert row["error_code"] == "sql_54_raw_omitted"
    assert count(db, "messages") == 0
    assert db.execute("SELECT kind FROM jobs").fetchall() == [{"kind": "founder_alert"}]
    with Ingestor(db, cfg) as ing:
        ing.ingest(update(2, mid=2, text="next chat update"))
        assert ing.offset == 3
    assert count(db, "messages") == 1


def test_class_54_in_fallback_is_bounded(db, cfg, raw_index):
    # Only quarantine rows are indexed: the message insert works, the raw fallback cannot.
    raw_index("WHERE error_code IS NOT NULL")

    def unique_violation(conn, event, body, uid):
        raise psycopg.errors.UniqueViolation("synthetic")

    poison = oversized(1)
    for attempt in range(3):
        with Ingestor(db, cfg, mapper=unique_violation) as ing:
            if attempt < 2:
                with pytest.raises(MappingRetry):
                    ing.ingest(poison)
                assert ing.offset == 0 and count(db, "events") == 0
            else:
                ing.ingest(poison)
                assert ing.offset == 2
    row = db.execute("SELECT raw,error_code FROM events").fetchone()
    assert row["raw"] is None and row["error_code"] == "sql_23_raw_omitted"
    assert count(db, "messages") == 0 and count(db, "attachments") == 0
