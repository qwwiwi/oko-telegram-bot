"""Per-agent file capability, read audit and rate limit on the read API."""

import hashlib
from dataclasses import replace

from fastapi.testclient import TestClient
from test_api_export import seed

from archive import audit
from archive.api import create_app
from archive.config import Agent

FILES = "f" * 32


def agents(cfg, **reader):
    return replace(
        cfg,
        agents={
            "reader": Agent(hashlib.sha256(b"x" * 32).hexdigest(), (-10,), **reader),
            "files": Agent(hashlib.sha256(FILES.encode()).hexdigest(), (-10,), files=True),
        },
    )


def bearer(token):
    return {"Authorization": "Bearer " + token}


def rows(db):
    return db.execute(
        "SELECT agent,route,chat_id,status,result_count FROM read_audit ORDER BY id"
    ).fetchall()


def test_files_capability_is_off_by_default_and_keeps_metadata(db, cfg):
    cfg = agents(seed(db, cfg))
    reader = TestClient(create_app(cfg), headers=bearer("x" * 32))
    for attachment in (1, 2, 999):  # in scope, other chat, missing: no existence leak
        denied = reader.get(f"/v1/files/{attachment}")
        assert denied.status_code == 403
        assert denied.json() == {"detail": "files_not_allowed"}
    detail = reader.get("/v1/chats/-10/messages/1").json()
    assert detail["attachments"][0]["file_available"] is True
    assert detail["attachments"][0]["status"] == "ready"
    assert detail["transcripts"][0]["text"] == "searchable transcript"
    assert len(reader.get("/v1/search?q=searchable").json()["items"]) == 1
    summary = reader.get("/v1/chats/-10/summary?date=2023-11-15").json()
    assert summary["messages"][0]["attachments"]
    files = TestClient(create_app(cfg), headers=bearer(FILES))
    assert files.get("/v1/files/1").content == b"synthetic downloadable"
    assert files.get("/v1/files/2").status_code == 404


def test_rate_limit_per_agent_with_retry_after(db, cfg):
    cfg = agents(seed(db, cfg), rate_per_minute=2)
    now = [1000.0]
    app = create_app(cfg, clock=lambda: now[0])
    reader = TestClient(app, headers=bearer("x" * 32))
    assert reader.get("/v1/chats").status_code == 200
    now[0] += 20
    assert reader.get("/v1/status").status_code == 200
    limited = reader.get("/v1/chats")
    assert limited.status_code == 429
    assert limited.json() == {"detail": "rate_limited"}
    assert limited.headers["retry-after"] == "40"
    # Another agent has its own window; unknown tokens stay 401, not 429.
    assert TestClient(app, headers=bearer(FILES)).get("/v1/chats").status_code == 200
    assert TestClient(app, headers=bearer("bad")).get("/v1/chats").status_code == 401
    now[0] += 40
    assert reader.get("/v1/chats").status_code == 200
    assert reader.get("/v1/chats").status_code == 429


def test_read_audit_rows_without_queries_tokens_or_content(db, cfg):
    cfg = agents(seed(db, cfg), rate_per_minute=6)
    app = create_app(cfg)
    reader = TestClient(app, headers=bearer("x" * 32))
    assert reader.get("/health").status_code == 200
    assert reader.get("/v1/chats").status_code == 200
    assert reader.get("/v1/chats/-10/messages?limit=5").status_code == 200
    assert reader.get("/v1/search?q=searchable&chat_id=-10").status_code == 200
    assert reader.get("/v1/chats/-20/messages/1").status_code == 404
    assert reader.get("/v1/files/1").status_code == 403
    assert reader.get("/v1/chats/-10/summary?date=2023-11-15").status_code == 200
    assert reader.get("/v1/status").status_code == 429
    assert TestClient(app, headers=bearer("y" * 32)).get("/v1/status").status_code == 401
    assert TestClient(app).get("/v1/files/1").status_code == 401
    assert TestClient(app, headers=bearer(FILES)).get("/v1/files/1").status_code == 200
    assert rows(db) == [
        {
            "agent": "reader",
            "route": "/v1/chats",
            "chat_id": None,
            "status": 200,
            "result_count": 1,
        },
        {
            "agent": "reader",
            "route": "/v1/chats/{chat_id}/messages",
            "chat_id": -10,
            "status": 200,
            "result_count": 1,
        },
        {
            "agent": "reader",
            "route": "/v1/search",
            "chat_id": -10,
            "status": 200,
            "result_count": 1,
        },
        {
            "agent": "reader",
            "route": "/v1/chats/{chat_id}/messages/{message_id}",
            "chat_id": -20,
            "status": 404,
            "result_count": None,
        },
        {
            "agent": "reader",
            "route": "/v1/files/{attachment_id}",
            "chat_id": None,
            "status": 403,
            "result_count": None,
        },
        {
            "agent": "reader",
            "route": "/v1/chats/{chat_id}/summary",
            "chat_id": -10,
            "status": 200,
            "result_count": None,
        },
        {
            "agent": "reader",
            "route": "/v1/status",
            "chat_id": None,
            "status": 429,
            "result_count": None,
        },
        {
            "agent": None,
            "route": "/v1/status",
            "chat_id": None,
            "status": 401,
            "result_count": None,
        },
        {
            "agent": None,
            "route": "/v1/files/{attachment_id}",
            "chat_id": None,
            "status": 401,
            "result_count": None,
        },
        {
            "agent": "files",
            "route": "/v1/files/{attachment_id}",
            "chat_id": -10,
            "status": 200,
            "result_count": None,
        },
    ]
    stored = str(db.execute("SELECT * FROM read_audit").fetchall())
    for secret in ("searchable", "q=", "limit", "x" * 32, "y" * 32, FILES, "alpha question"):
        assert secret not in stored


def test_audit_failure_fails_closed_for_agents_only(db, cfg, monkeypatch):
    cfg = agents(seed(db, cfg))

    def broken(*args):
        raise RuntimeError("synthetic audit outage")

    monkeypatch.setattr(audit, "record", broken)
    app = create_app(cfg)
    failed = TestClient(app, headers=bearer("x" * 32)).get("/v1/chats")
    assert failed.status_code == 500 and failed.json() == {"error": "internal_error"}
    assert TestClient(app, headers=bearer("bad")).get("/v1/chats").status_code == 401


def test_anonymous_audit_rows_are_capped(db, cfg):
    cfg = agents(seed(db, cfg))
    app = create_app(cfg, clock=lambda: 5.0)
    anonymous = TestClient(app, headers=bearer("z" * 32))
    for _ in range(audit.ANONYMOUS_PER_MINUTE + 5):
        assert anonymous.get("/v1/chats").status_code == 401
    assert len(rows(db)) == audit.ANONYMOUS_PER_MINUTE


def test_prune_keeps_recent_rows(db):
    db.execute(
        "INSERT INTO read_audit(at,agent,route,status) VALUES"
        "(now()-interval '200 days','reader','/v1/chats',200),"
        "(now()-interval '10 days','reader','/v1/chats',200)"
    )
    assert audit.prune(db, 180) == 1
    assert len(rows(db)) == 1
