import threading
from dataclasses import replace

import pytest
from cloud import CloudFiles
from test_config import settings
from test_ingest import update

from archive.config import Chat, load
from archive.db import connect
from archive.ingest import Ingestor
from archive.worker import Heartbeat, LeaseLost, Worker, claim, guard


def test_consent_types_and_environment_only_secrets(tmp_path, monkeypatch):
    cfg = settings(tmp_path)
    for changes in (
        {"transcription_enabled": "false"},
        {"media_root": cfg.export_root / "archive"},
    ):
        with pytest.raises(ValueError):
            replace(cfg, **changes)
    for changes in ({"transcription": "false"}, {"enabled": "false"}):
        with pytest.raises(ValueError):
            Chat("alpha", **changes)
    config = tmp_path / "settings.toml"
    config.write_text('dsn="do-not-read-secrets-from-toml"\n')
    config.chmod(0o600)
    with pytest.raises(ValueError, match="environment_secrets_required"):
        load(config)
    # Stale local Bot API settings are refused, never silently ignored.
    config.write_text('sidecar_root="/var/lib/telegram-bot-api"\n')
    with pytest.raises(ValueError, match="local_bot_api_settings_removed"):
        load(config)


def test_heartbeat_renews_without_passive_delay(db, cfg, monkeypatch):
    cfg = replace(cfg, dsn=db.info.dsn)
    with Ingestor(db, cfg) as ing:
        ing.ingest(update(document={"file_id": "file", "file_size": 8}))
    job = claim(db, cfg)
    # Make renewal observable without sleeping a lease interval.
    db.execute("UPDATE jobs SET lease_until=now()+interval '2 seconds'")
    renewed = threading.Event()
    import archive.worker as module

    original = module.renew

    def observed(conn, config, item):
        original(conn, config, item)
        renewed.set()

    monkeypatch.setattr(module, "renew", observed)
    hb = Heartbeat(cfg, job)
    real_wait = hb.stop.wait
    first = True

    def wait(timeout):
        nonlocal first
        if first:
            first = False
            return False
        return real_wait(timeout)

    monkeypatch.setattr(hb.stop, "wait", wait)
    with hb:
        assert renewed.wait(5), "heartbeat failed to renew on separate DB connection"
        with connect(cfg.dsn) as other:
            row = other.execute(
                "SELECT lease_until>now()+interval '60 seconds' AS ok FROM jobs"
            ).fetchone()
        assert row["ok"]
        hb.check()


def test_expired_copy_never_overwrites_replacement_ready(db, cfg, monkeypatch):
    cfg = replace(cfg, dsn=db.info.dsn)
    cloud = CloudFiles(cfg, {"file": b"replacement-safe"})
    with Ingestor(db, cfg) as ing:
        ing.ingest(update(document={"file_id": "file", "file_size": 16}))
    stale = claim(db, cfg)
    worker = Worker(db, cfg, cloud.bot())
    import archive.worker as module

    original_publish = module.publish_stream
    replacement = None

    class HeartbeatProbe:
        def check(self):
            return

    def racing_publish(root, relative, chunks, maximum, check, fence, reconcile=None):
        nonlocal replacement
        data = b"".join(chunks)
        db.execute("UPDATE jobs SET lease_until=now()-interval '1 second'")
        replacement = claim(db, cfg)
        monkeypatch.setattr(module, "publish_stream", original_publish)
        worker.download(replacement, HeartbeatProbe())
        # The stale owner's fence must refuse publication of its own temp file.
        return original_publish(root, relative, [data], maximum, check, fence, reconcile)

    monkeypatch.setattr(module, "publish_stream", racing_publish)
    with pytest.raises(LeaseLost):
        worker.download(stale, HeartbeatProbe())
    with pytest.raises(LeaseLost):
        with db.transaction():
            guard(db, stale)
    row = db.execute("SELECT * FROM attachments").fetchone()
    assert str(replacement["lease_token"]) in row["relative_path"]
    assert str(stale["lease_token"]) not in row["relative_path"]
    assert (cfg.media_root / row["relative_path"]).read_bytes() == b"replacement-safe"
    stale_file = cfg.media_root / row["relative_path"].replace(
        str(replacement["lease_token"]), str(stale["lease_token"])
    )
    assert not stale_file.exists(), "stale owner published despite the fence"
