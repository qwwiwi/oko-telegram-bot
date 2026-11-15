"""Leased SQL queue. All mutations are current-token AND unexpired-lease fenced."""

import threading
import time
from datetime import UTC, datetime
from uuid import uuid4

from archive import approvals
from archive.db import cleanup_unknown, connect, enqueue, enqueue_transcription
from archive.storage import CommitUnknown, StorageError, interrupted_commit, publish_stream
from archive.telegram import BotAPI, BotError, FileTooBig, LeaveRejected

OVERSIZED = "oversized_not_downloaded"
TRANSCRIBABLE = ("voice", "audio", "video", "video_note")
# Owner-facing interactions run before file work so taps get a timely answer.
OWNER_KINDS = ("owner_callback", "approval_prompt", "chats_list")


class LeaseLost(RuntimeError):
    pass


class Revoked(RuntimeError):
    """The chat stopped collecting while its work was in flight: discard the result."""


def claim(conn, cfg):
    with conn.transaction():
        cleanup_unknown(conn)
        # Expired final attempts must not loop forever after a worker crash.
        conn.execute(
            """UPDATE jobs SET status='failed',error_code='attempts_exhausted'
            WHERE status='running' AND lease_until<=now() AND attempts>=%s""",
            (cfg.max_attempts,),
        )
        conn.execute("""UPDATE attachments SET status='failed',error_code='attempts_exhausted'
            WHERE id IN (SELECT attachment_id FROM jobs WHERE kind='download'
                         AND status='failed') AND status='pending'""")
        conn.execute("""UPDATE transcripts SET status=CASE WHEN status='submitting'
            THEN 'reconciliation_required' ELSE 'failed' END,
            error_code=CASE WHEN status='submitting' THEN 'submission_uncertain'
                            ELSE 'attempts_exhausted' END
            WHERE attachment_id IN (SELECT attachment_id FROM jobs
                WHERE kind IN ('transcribe','transcript_poll') AND status='failed')
            AND status NOT IN ('ready','blocked','reconciliation_required')""")
        return conn.execute(
            """WITH selected AS (
            SELECT id FROM jobs WHERE next_run<=now() AND attempts<%s AND
            (status='pending' OR (status='running' AND lease_until<=now()))
            ORDER BY CASE WHEN kind='leave' OR kind=ANY(%s) THEN 0
            WHEN kind='download' THEN 1 ELSE 2 END,
            created_at,id
            FOR UPDATE SKIP LOCKED LIMIT 1)
            UPDATE jobs SET status='running',attempts=attempts+1,lease_token=%s,
            lease_until=now()+%s*interval '1 second' FROM selected
            WHERE jobs.id=selected.id RETURNING jobs.*""",
            (cfg.max_attempts, list(OWNER_KINDS), uuid4(), cfg.lease_seconds),
        ).fetchone()


def guard(conn, job):
    if not conn.execute(
        """SELECT id FROM jobs WHERE id=%s AND lease_token=%s
        AND status='running' AND lease_until>now() FOR UPDATE""",
        (job["id"], job["lease_token"]),
    ).fetchone():
        raise LeaseLost("stale_lease")


def renew(conn, cfg, job):
    if not conn.execute(
        """UPDATE jobs SET lease_until=now()+%s*interval '1 second'
        WHERE id=%s AND lease_token=%s AND status='running' AND lease_until>now()
        RETURNING id""",
        (cfg.lease_seconds, job["id"], job["lease_token"]),
    ).fetchone():
        raise LeaseLost("stale_lease")


def finish(conn, job):
    with conn.transaction():
        guard(conn, job)
        conn.execute("UPDATE jobs SET status='done',lease_until=NULL WHERE id=%s", (job["id"],))


class Heartbeat:
    """Independent DB connection renews during bounded HTTP calls and file I/O."""

    def __init__(self, cfg, job):
        self.cfg, self.job = cfg, job
        self.stop = threading.Event()
        self.lost = threading.Event()

    def __enter__(self):
        self.thread = threading.Thread(target=self.run, daemon=True)
        self.thread.start()
        return self

    def run(self):
        try:
            with connect(self.cfg.dsn) as conn:
                while not self.stop.wait(self.cfg.lease_seconds / 3):
                    renew(conn, self.cfg, self.job)
        except Exception:
            self.lost.set()

    def check(self):
        if self.lost.is_set():
            raise LeaseLost("stale_lease")

    def __exit__(self, *args):
        self.stop.set()
        self.thread.join(timeout=10)
        if self.thread.is_alive():
            self.lost.set()


class Worker:
    def __init__(self, conn, cfg, bot, transcription_transport=None):
        self.conn, self.cfg, self.bot = conn, cfg, bot
        self.transcription_transport = transcription_transport

    def once(self):
        job = claim(self.conn, self.cfg)
        if not job:
            return False
        try:
            with Heartbeat(self.cfg, job) as heartbeat:
                if job["kind"] == "download":
                    self.download(job, heartbeat)
                elif job["kind"] in {"transcribe", "transcript_poll"}:
                    from archive.transcription import process

                    process(self.conn, self.cfg, job, heartbeat, self.transcription_transport)
                else:
                    self.operation(job)
        except LeaseLost:
            pass  # No stale-owner status writes, even on failure.
        except LeaveRejected:
            self.discard_leave(job, alert=True)
        except (StorageError, BotError) as exc:
            code = str(exc) if isinstance(exc, StorageError) else "bot_error"
            self.failure(job, code)
        except Exception:
            self.failure(job, "worker_error")  # never leak exception body/URL/path
        return True

    def failure(self, job, code):
        permanent = code in {
            "outside_root",
            "unsafe_source",
            "not_regular",
            "unsafe_path",
            "unsafe_directory",
            "reconciliation_required",
        }
        failed = permanent or job["attempts"] >= self.cfg.max_attempts
        try:
            if failed and job["kind"] == "leave":
                self.discard_leave(job, alert=True)
                return
            with self.conn.transaction():
                guard(self.conn, job)
                self.conn.execute(
                    """UPDATE jobs SET status=%s,error_code=%s,lease_until=NULL,
                    next_run=now()+%s*interval '1 second' WHERE id=%s""",
                    (
                        "failed" if failed else "pending",
                        code,
                        min(3600, 2 ** job["attempts"]),
                        job["id"],
                    ),
                )
                if failed and job["kind"] == "download":
                    self.conn.execute(
                        "UPDATE attachments SET status='failed',error_code=%s WHERE id=%s",
                        (code, job["attachment_id"]),
                    )
        except LeaseLost:
            pass

    def discard_leave(self, job, alert=False):
        try:
            with self.conn.transaction():
                guard(self.conn, job)
                if alert:
                    hour = datetime.now(UTC).strftime("%Y-%m-%dT%H")
                    enqueue(
                        self.conn,
                        "leave-failure:" + hour,
                        "founder_alert",
                        payload={"code": "unknown_exit_failed"},
                    )
                self.conn.execute("DELETE FROM jobs WHERE id=%s", (job["id"],))
                self.conn.execute(
                    "DELETE FROM unknown_chats WHERE chat_id=%s AND kind='group'",
                    (job["chat_id"],),
                )
        except LeaseLost:
            return

    def download(self, job, heartbeat):
        row = self.conn.execute(
            "SELECT * FROM attachments WHERE id=%s", (job["attachment_id"],)
        ).fetchone()
        if not approvals.collecting(self.conn, self.cfg, row["chat_id"]):
            self.blocked(job, row)
            return
        limit = self.cfg.max_file_bytes
        if row["declared_size"] is not None and row["declared_size"] > limit:
            # Known above the cloud limit: no getFile call, no retry.
            self.oversized(job, row)
            return
        renew(self.conn, self.cfg, job)
        try:
            path, telegram_size = self.bot.get_file(row["file_id"])
            heartbeat.check()
            if telegram_size is not None and telegram_size > limit:
                self.oversized(job, row)
                return
            date = (
                self.conn.execute("SELECT received_at FROM events WHERE id=%s", (row["event_id"],))
                .fetchone()["received_at"]
                .astimezone(UTC)
            )
            relative = (
                f"{self.cfg.slug(row['chat_id'])}/{row['chat_id']}/"
                f"{date:%Y-%m-%d}/{row['id']}/{job['lease_token']}/file"
            )

            def fence(written, checksum, link):
                if telegram_size is not None and written != telegram_size:
                    raise StorageError("size_mismatch")
                heartbeat.check()
                # Proves current ownership right before the exclusive publication.
                renew(self.conn, self.cfg, job)
                self.publish(job, row, relative, written, checksum, link)

            backend = self.conn.info.backend_pid

            def reconcile():
                return self.referenced(backend, row["id"], relative)

            with self.bot.download(path, limit) as chunks:
                publish_stream(
                    self.cfg.media_root, relative, chunks, limit, heartbeat.check, fence, reconcile
                )
        except FileTooBig:
            self.oversized(job, row)
            return
        except Revoked:
            self.blocked(job, row)  # nothing was linked; the temporary file is removed
            return
        except StorageError as exc:
            if str(exc) != "oversized":
                raise
            self.oversized(job, row)
            return

    def publish(self, job, row, relative, size, checksum, link):
        """Link and reference the file in one transaction that holds the approval row.

        fenced() takes the row FOR SHARE before the hardlink, so a Stop either commits
        first (nothing is linked) or waits until the file is referenced and the job is
        done. If this transaction fails after link(), publish_stream removes the file;
        if the connection is lost during COMMIT the outcome is unknown (CommitUnknown)
        and publish_stream asks referenced() before removing anything.
        """
        committing = False
        try:
            with self.conn.transaction():
                guard(self.conn, job)
                if not approvals.fenced(self.conn, self.cfg, row["chat_id"]):
                    raise Revoked("chat_disabled")
                link()
                self.conn.execute(
                    """UPDATE attachments SET status='ready',relative_path=%s,
                    size=%s,sha256=%s,error_code=NULL WHERE id=%s""",
                    (relative, size, checksum, row["id"]),
                )
                if row["kind"] in TRANSCRIBABLE:
                    consent = self.cfg.transcription_enabled and self.cfg.transcription_consent(
                        row["chat_id"]
                    )
                    self.conn.execute(
                        """INSERT INTO transcripts(attachment_id,chat_id,status,error_code)
                        VALUES(%s,%s,%s,%s) ON CONFLICT DO NOTHING""",
                        (
                            row["id"],
                            row["chat_id"],
                            "pending" if consent else "blocked",
                            None if consent else "consent_required",
                        ),
                    )
                    if consent:
                        enqueue_transcription(self.conn, row["id"], row["chat_id"])
                finish(self.conn, job)
                committing = True  # leaving the block sends COMMIT
        except Exception as exc:
            # A server-reported error (with SQLSTATE) is a definite rollback; a lost
            # connection while committing may have committed anyway.
            if committing and getattr(exc, "sqlstate", None) is None:
                raise CommitUnknown("commit_unknown") from None
            raise
        except BaseException as exc:
            # An interruption while awaiting COMMIT may follow a landed COMMIT
            # (psycopg re-raises it even after draining the result): keep the file.
            if committing:
                interrupted_commit(exc)
            raise

    def referenced(self, backend, attachment, relative):
        """After an uncertain COMMIT: does committed state reference the published file?

        Read through a fresh connection. While the old backend is still inside a
        transaction its COMMIT may yet land, so the file is kept (True, an orphan at
        worst). The reference is read in a later statement than that check, so a
        commit that finished in between is seen.
        """
        with connect(self.cfg.dsn) as conn:
            if conn.execute(
                """SELECT 1 FROM pg_stat_activity WHERE pid=%s
                AND state IS DISTINCT FROM 'idle'""",
                (backend,),
            ).fetchone():
                return True
            return (
                conn.execute(
                    """SELECT 1 FROM attachments WHERE id=%s AND status='ready'
                    AND relative_path=%s""",
                    (attachment, relative),
                ).fetchone()
                is not None
            )

    def blocked(self, job, row):
        """Terminal: the chat is not collecting; no bytes, no transcript, no retry."""
        with self.conn.transaction():
            guard(self.conn, job)
            self.conn.execute(
                "UPDATE attachments SET status='blocked',error_code='chat_disabled' "
                "WHERE id=%s AND status<>'ready'",
                (row["id"],),
            )
            self.conn.execute(
                "UPDATE jobs SET error_code='chat_disabled' WHERE id=%s", (job["id"],)
            )
            finish(self.conn, job)

    def oversized(self, job, row):
        """Terminal metadata-only record: Telegram IDs kept, no bytes, no transcript."""
        with self.conn.transaction():
            guard(self.conn, job)
            self.conn.execute(
                """UPDATE attachments SET status=%s,error_code='cloud_download_limit',
                relative_path=NULL,size=NULL,sha256=NULL WHERE id=%s AND status<>'ready'""",
                (OVERSIZED, row["id"]),
            )
            if row["kind"] in TRANSCRIBABLE:
                self.conn.execute(
                    """INSERT INTO transcripts(attachment_id,chat_id,status,error_code)
                    VALUES(%s,%s,'unavailable','file_not_downloaded') ON CONFLICT DO NOTHING""",
                    (row["id"], row["chat_id"]),
                )
            self.conn.execute("UPDATE jobs SET error_code=%s WHERE id=%s", (OVERSIZED, job["id"]))
            finish(self.conn, job)

    def operation(self, job):
        # Fixed metadata only. No client-supplied text or names.
        if job["kind"] == "leave":
            # A chat the owner is deciding on (or has decided on) is never left implicitly.
            # The per-chat lock serializes the recheck and leaveChat with approval
            # requests and migration, so a chat that became known is never left. (No
            # job row lock here: the heartbeat must keep renewing during leaveChat.)
            with self.conn.transaction():
                approvals.lock(self.conn, job["chat_id"])
                if approvals.status(self.conn, job["chat_id"]) in approvals.GONE:
                    self.bot.call("leaveChat", {"chat_id": job["chat_id"]})
            self.discard_leave(job)
            return
        elif job["kind"] in OWNER_KINDS:
            self.owner(job)
        elif job["kind"] == "unknown_digest":
            from archive.notifications import send_digest

            send_digest(self.conn, self.cfg, self.bot, job)
            return
        elif job["kind"] == "founder_alert":
            codes = {
                "coverage_check": "Coverage check required",
                "pending_enrollment": "Chat migration requires enrollment",
                "poller_failure": "Poller failure threshold reached",
                "quarantine": "Approved update quarantined; operator replay required",
                "unknown_exit_failed": "Unknown group exit failed; operator check required",
            }
            safe = {
                k: v
                for k, v in job["payload"].items()
                if k in {"chat_id", "old", "new"} and type(v) is int
            }
            self.bot.call(
                "sendMessage",
                {
                    "chat_id": self.cfg.founder_id,
                    "text": codes.get(job["payload"].get("code"), "Archive operational alert")
                    + " "
                    + str(safe),
                },
            )
        else:
            raise StorageError("unknown_job_kind")
        finish(self.conn, job)

    def owner(self, job):
        """Messages to the owner only (owner_tg_id); fixed texts plus chat titles."""
        owner = self.cfg.owner_tg_id
        payload = job["payload"]
        if job["kind"] == "approval_prompt":
            row = self.conn.execute(
                "SELECT status,title FROM chat_approvals WHERE chat_id=%s", (job["chat_id"],)
            ).fetchone()
            if row and row["status"] == approvals.PENDING:
                text, markup = approvals.prompt(job["chat_id"], row["title"])
                self.bot.call(
                    "sendMessage", {"chat_id": owner, "text": text, "reply_markup": markup}
                )
        elif job["kind"] == "chats_list":
            text, markup = approvals.render(self.conn, self.cfg)
            self.bot.call("sendMessage", {"chat_id": owner, "text": text, "reply_markup": markup})
        else:
            # Callback answers and keyboard edits are ephemeral UI: the decision is
            # already committed, Telegram expires old queries, so they are not retried.
            refused = payload["answer"] in ("refused", "invalid")
            answer = {"callback_query_id": payload["callback_id"], "show_alert": refused}
            if approvals.ANSWERS[payload["answer"]]:
                answer["text"] = approvals.ANSWERS[payload["answer"]]
            try:
                self.bot.call("answerCallbackQuery", answer)
            except BotError:
                pass
            origin, mid = payload.get("origin"), payload.get("message_id")
            try:
                if not refused and origin == "p" and type(mid) is int:
                    self.bot.call(
                        "editMessageReplyMarkup",
                        {
                            "chat_id": owner,
                            "message_id": mid,
                            "reply_markup": {"inline_keyboard": []},
                        },
                    )
                elif not refused and origin in ("l", "c") and type(mid) is int:
                    page = payload.get("page")
                    page = page if type(page) is int else 0
                    chat = payload.get("chat")
                    if origin == "c" and type(chat) is int:
                        text, markup = approvals.card(
                            self.conn, self.cfg, chat, page, payload.get("confirm") is True
                        )
                    else:
                        text, markup = approvals.render(self.conn, self.cfg, page)
                    self.bot.call(
                        "editMessageText",
                        {"chat_id": owner, "message_id": mid, "text": text, "reply_markup": markup},
                    )
            except BotError:
                pass


def run(cfg):
    with connect(cfg.dsn) as conn, BotAPI(cfg) as bot:
        bot.preflight()
        worker = Worker(conn, cfg, bot)
        while True:
            if not worker.once():
                time.sleep(1)
