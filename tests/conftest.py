import os

import pytest
from psycopg.conninfo import conninfo_to_dict
from test_config import SYNTHETIC_OWNER_ID, settings

from archive.approvals import activate
from archive.db import connect, migrate


@pytest.fixture
def cfg(tmp_path):
    return settings(tmp_path)


@pytest.fixture
def db():
    dsn = os.environ.get("ARCHIVE_DISPOSABLE_TEST_DSN")
    if not dsn or os.environ.get("ARCHIVE_DISPOSABLE_TEST_ROOT") is None:
        pytest.fail("disposable_postgres_required: run scripts/check.sh")
    values = conninfo_to_dict(dsn)
    root = os.environ["ARCHIVE_DISPOSABLE_TEST_ROOT"]
    if values.get("host") != root or values.get("dbname") != "archive_synthetic_test":
        pytest.fail("refuse_non_disposable_database")
    with connect(dsn) as conn:
        migrate(conn)
        conn.execute("""TRUNCATE events,mapping_failures,messages,revisions,attachments,
                     transcripts,jobs,unknown_chats,enrollment_hints,chat_approvals,read_audit
                     RESTART IDENTITY CASCADE""")
        conn.execute("""UPDATE poll_state SET epoch=0,next_offset=0,sidecar_session=NULL,
                     last_commit=NULL,last_poll=NULL,failures=0""")
        conn.execute("UPDATE notification_state SET last_attempt=NULL")
        # The synthetic configured chats start owner-approved; approval tests use others.
        for chat in (-10, -20):
            activate(conn, chat, SYNTHETIC_OWNER_ID)
        yield conn


def pytest_sessionfinish(session, exitstatus):
    reporter = session.config.pluginmanager.getplugin("terminalreporter")
    if reporter and reporter.stats.get("skipped"):
        session.exitstatus = 1
