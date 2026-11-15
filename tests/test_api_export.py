import hashlib
import json
from dataclasses import replace

import pytest
from cloud import CloudFiles
from fastapi.testclient import TestClient
from test_ingest import update

from archive.api import create_app
from archive.export import export_day
from archive.ingest import Ingestor
from archive.worker import Worker


def with_files(cfg, db):
    # These tests exercise file bytes; the capability itself is covered in test_api_access.
    agents = {k: replace(v, files=True) for k, v in cfg.agents.items()}
    return replace(cfg, dsn=db.info.dsn, agents=agents)


def seed(db, cfg):
    cfg = with_files(cfg, db)
    cloud = CloudFiles(cfg, {"f": b"synthetic downloadable", "b": b"beta bytes"})
    with Ingestor(db, cfg) as ing:
        ing.ingest(update(text="alpha question?", document={"file_id": "f"}, message_thread_id=7))
        ing.ingest(update(2, chat=-20, text="BETA_PRIVATE", document={"file_id": "b"}))

    worker = Worker(db, cfg, cloud.bot())
    worker.once()
    worker.once()
    db.execute(
        "INSERT INTO transcripts(attachment_id,chat_id,status,text) "
        "VALUES(1,-10,'ready','searchable transcript')"
    )
    return cfg


@pytest.mark.parametrize("headers", [{}, {"Authorization": "Bearer wrong"}])
def test_auth_all_routes(db, cfg, headers):
    cfg = seed(db, cfg)
    client = TestClient(create_app(cfg))
    for route in [
        "/v1/status",
        "/v1/chats",
        "/v1/search?q=test",
        "/v1/chats/-10/messages",
        "/v1/chats/-10/messages/1",
        "/v1/files/1",
        "/v1/chats/-10/summary?date=2023-11-15",
    ]:
        assert client.get(route, headers=headers).status_code == 401
    assert client.get("/health").json() == {"status": "ok"}
    assert client.get("/docs").status_code == 404


def test_every_scope_search_file_summary_revocation(db, cfg):
    cfg = seed(db, cfg)
    headers = {"Authorization": "Bearer " + "x" * 32}
    client = TestClient(create_app(cfg), headers=headers)
    for route in [
        "/v1/chats/-20/messages",
        "/v1/chats/-20/messages/1",
        "/v1/chats/-20/summary?date=2023-11-15",
        "/v1/files/2",
        "/v1/search?q=PRIVATE&chat_id=-20",
    ]:
        assert client.get(route).status_code == 404
    assert client.get("/v1/search?q=PRIVATE").json()["items"] == []
    assert len(client.get("/v1/search?q=searchable").json()["items"]) == 1
    assert client.get("/v1/status").json()["messages"] == 1
    assert client.get("/v1/chats").json() == [{"chat_id": -10, "slug": "alpha"}]
    detail = client.get("/v1/chats/-10/messages/1").json()
    assert json.loads(detail["revisions"][0]["raw_json"])["message"]["message_thread_id"] == 7
    assert detail["transcripts"][0]["text"] == "searchable transcript"
    response = client.get("/v1/files/1")
    assert response.content == b"synthetic downloadable"
    assert response.headers["content-type"] == "application/octet-stream"
    assert response.headers["content-disposition"].startswith("attachment;")
    assert str(cfg.media_root) not in str(detail)
    assert "BETA_PRIVATE" not in client.get("/v1/chats/-10/summary?date=2023-11-15").text
    disabled = replace(cfg, chats={**cfg.chats, -10: replace(cfg.chats[-10], enabled=False)})
    revoked = TestClient(create_app(disabled), headers=headers)
    for route in [
        "/v1/status",
        "/v1/chats",
        "/v1/search?q=a",
        "/v1/files/1",
        "/v1/chats/-10/messages",
        "/v1/chats/-10/messages/1",
        "/v1/chats/-10/summary?date=2023-11-15",
    ]:
        assert revoked.get(route).status_code == 401
    assert client.post("/v1/chats").status_code == 405


def test_filters_pagination_safe_errors_and_export(db, cfg, tmp_path):
    cfg = seed(db, cfg)
    with Ingestor(db, cfg) as ing:
        ing.ingest(update(3, mid=2, text="second"))
    client = TestClient(create_app(cfg), headers={"Authorization": "Bearer " + "x" * 32})
    page = client.get("/v1/chats/-10/messages?limit=1").json()
    assert page["next_cursor"] == 1
    assert client.get("/v1/chats/-10/messages?cursor=1").json()["items"][0]["message_id"] == 2
    assert client.get("/v1/chats/-10/messages?author=999").json()["items"] == []
    assert client.get("/v1/chats/-10/messages?from=2023-11-15T00:00:00Z").json()["items"] == []
    assert client.get("/v1/chats/-10/messages?limit=99999").json() == {"error": "invalid_request"}
    assert client.get("/v1/search?q=%27%20OR%201%3D1--").json()["items"] == []
    paths = export_day(db, cfg, -10, "2023-11-15")
    before = [p.read_bytes() for p in paths]
    assert [p.read_bytes() for p in export_day(db, cfg, -10, "2023-11-15")] == before
    assert paths[0].is_relative_to(cfg.export_root / "alpha/chat")
    assert b"unclassified" in before[0] and b"alpha question?" in before[0]
    assert hashlib.sha256(before[0]).digest()
    bad = replace(cfg, export_root=tmp_path / "repo")
    bad.export_root.mkdir()
    (bad.export_root / ".git").write_text("gitdir: /synthetic")
    with pytest.raises(ValueError, match="git_export_forbidden"):
        export_day(db, bad, -10, "2023-11-15")


def test_unavailable_file_and_transcript_states_in_api_and_export(db, cfg):
    cfg = with_files(cfg, db)
    cloud = CloudFiles(cfg, {"small": b"tiny voice"}, too_big={"big"})
    with Ingestor(db, cfg) as ing:
        ing.ingest(update(text="voices", voice={"file_id": "big", "file_unique_id": "ub"}))
        ing.ingest(update(2, mid=2, voice={"file_id": "small"}))
        ing.ingest(update(3, mid=3, video={"file_id": "later", "file_size": 5}))
        ing.ingest(update(4, chat=-20, document={"file_id": "other", "file_size": 2**30}))
    worker = Worker(db, cfg, cloud.bot())
    assert worker.once() and worker.once()  # big -> oversized, small -> ready; 3 pending
    db.execute("UPDATE jobs SET next_run=now()+interval '1 hour' WHERE attachment_id=3")
    assert worker.once() and not worker.once()  # chat -20 oversized
    client = TestClient(create_app(cfg), headers={"Authorization": "Bearer " + "x" * 32})
    unavailable = client.get("/v1/files/1")
    assert unavailable.status_code == 409
    assert unavailable.json() == {
        "error": "file_unavailable",
        "status": "oversized_not_downloaded",
        "error_code": "cloud_download_limit",
    }
    assert client.get("/v1/files/3").json()["status"] == "pending"
    assert client.get("/v1/files/4").status_code == 404  # other chat: no existence leak
    assert client.get("/v1/files/2").content == b"tiny voice"
    big = client.get("/v1/chats/-10/messages/1").json()
    assert big["attachments"][0]["file_available"] is False
    assert big["attachments"][0]["file_unique_id"] == "ub"
    assert "file_id" not in big["attachments"][0]
    assert big["transcripts"] == [
        {
            "attachment_id": 1,
            "status": "unavailable",
            "provider": None,
            "text": None,
            "error_code": "file_not_downloaded",
            "transcript_available": False,
        }
    ]
    small = client.get("/v1/chats/-10/messages/2").json()
    assert small["attachments"][0]["file_available"] is True
    assert small["transcripts"][0]["status"] == "blocked"
    assert small["transcripts"][0]["error_code"] == "consent_required"
    later = client.get("/v1/chats/-10/messages/3").json()["transcripts"][0]
    assert (later["status"], later["error_code"]) == ("pending", "file_pending")
    status = client.get("/v1/status").json()
    assert status["files_unavailable"] == 2 and status["files_oversized_not_downloaded"] == 1
    summary = client.get("/v1/chats/-10/summary?date=2023-11-15").json()
    assert summary["messages"][0]["transcripts"][0]["status"] == "unavailable"
    json_path, md_path = export_day(db, cfg, -10, "2023-11-15")
    exported = json.loads(json_path.read_text())
    assert exported["messages"][0]["attachments"][0]["status"] == "oversized_not_downloaded"
    markdown = md_path.read_text()
    assert (
        "- File 1 (voice): UNAVAILABLE, status oversized_not_downloaded "
        "(cloud_download_limit); bytes not archived"
    ) in markdown
    assert (
        "- Transcript 1: UNAVAILABLE, status unavailable (file_not_downloaded); no transcript text"
    ) in markdown
    assert "- Transcript 3: UNAVAILABLE, status pending (file_pending)" in markdown
    assert "- File 2 (voice): available" in markdown
    assert cfg.bot_token not in markdown + json_path.read_text()
    db.execute("DELETE FROM transcripts WHERE attachment_id=2")
    legacy = client.get("/v1/chats/-10/messages/2").json()["transcripts"][0]
    assert (legacy["status"], legacy["error_code"]) == ("unavailable", "transcript_not_created")
