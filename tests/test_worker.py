import hashlib
import os
from dataclasses import replace

import pytest
from cloud import CloudFiles
from test_ingest import update

from archive.ingest import Ingestor
from archive.storage import StorageError, open_under, publish_stream
from archive.worker import LeaseLost, Worker, claim, finish, renew


def test_descriptor_safe_publish_and_bounds(tmp_path):
    target = tmp_path / "target"
    target.mkdir()
    content = b"synthetic" * 400000
    chunks = [content[i : i + 65536] for i in range(0, len(content), 65536)]
    size, digest = publish_stream(target, "a/lease/file", iter(chunks), len(content))
    assert size == len(content)
    assert digest == hashlib.sha256(content).hexdigest()
    assert (target / "a/lease/file").read_bytes() == content
    assert (target / "a/lease/file").stat().st_mode & 0o777 == 0o600
    with pytest.raises(StorageError):  # immutable: never overwrite a published file
        publish_stream(target, "a/lease/file", iter([b"x"]), 10)
    with pytest.raises(StorageError, match="oversized"):
        publish_stream(target, "other/file", iter(chunks), 2)
    with pytest.raises(StorageError, match="unsafe_path"):
        publish_stream(target, "../escape/file", iter([b"x"]), 10)

    def refuse(_size, _checksum, _link):
        raise LeaseLost("stale_lease")

    with pytest.raises(LeaseLost):
        publish_stream(target, "fenced/file", iter([b"x"]), 10, fence=refuse)
    assert not (target / "fenced/file").exists()
    assert not [p for p in target.rglob(".partial-*")], "temporaries must never survive"
    source = tmp_path / "source"
    source.mkdir()
    (source / "file").write_bytes(content)
    (source / "link").symlink_to(source / "file")
    (source / "dirlink").symlink_to(target, target_is_directory=True)
    for path in [
        str(source / "link"),
        str(source / "dirlink/a/lease/file"),
        str(source) + "/../source/file",
        "/etc/passwd",
    ]:
        with pytest.raises(StorageError):
            open_under(source, path)
    os.mkfifo(source / "pipe")
    with pytest.raises(StorageError):
        open_under(source, str(source / "pipe"))


def test_lease_recovery_fences_old_owner_and_copy(db, cfg):
    cfg = replace(cfg, dsn=db.info.dsn)
    with Ingestor(db, cfg) as ing:
        ing.ingest(update(document={"file_id": "file", "file_size": 10}))
    old = claim(db, cfg)
    db.execute("UPDATE jobs SET lease_until=now()-interval '1 second'")
    new = claim(db, cfg)
    assert old["lease_token"] != new["lease_token"] and new["attempts"] == 2
    with pytest.raises(LeaseLost):
        renew(db, cfg, old)
    with pytest.raises(LeaseLost):
        finish(db, old)
    finish(db, new)
    assert db.execute("SELECT status FROM jobs").fetchone()["status"] == "done"


def test_worker_cloud_file_and_declared_oversize(db, cfg):
    cfg = replace(cfg, dsn=db.info.dsn)
    content = b"synthetic-file"
    cloud = CloudFiles(cfg, {"f": content})
    with Ingestor(db, cfg) as ing:
        ing.ingest(update(voice={"file_id": "f", "file_size": len(content)}))
    worker = Worker(db, cfg, cloud.bot())
    assert worker.once()
    row = db.execute("SELECT * FROM attachments").fetchone()
    assert row["status"] == "ready" and row["sha256"] == hashlib.sha256(content).hexdigest()
    assert (cfg.media_root / row["relative_path"]).read_bytes() == content
    assert db.execute("SELECT status FROM transcripts").fetchone()["status"] == "blocked"
    assert cloud.calls[-1] == ("download", "documents/f")
    with Ingestor(db, cfg) as ing:
        ing.ingest(update(2, mid=2, document={"file_id": "g", "file_size": 2**31 + 1}))
    assert worker.once()
    row = db.execute("SELECT * FROM attachments WHERE message_id=2").fetchone()
    assert row["status"] == "oversized_not_downloaded"
    assert row["error_code"] == "cloud_download_limit"
    assert row["file_id"] == "g" and row["relative_path"] is None
    assert cloud.count("getFile") == 1, "declared oversize must not call getFile"
