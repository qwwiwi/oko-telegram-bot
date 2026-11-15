"""Read-only HTTP surface. Every content route rechecks owner-approved numeric scopes."""

import logging
import time
from datetime import UTC, datetime
from typing import Annotated

from fastapi import Depends, FastAPI, Header, HTTPException, Query, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse, StreamingResponse
from starlette.concurrency import run_in_threadpool

from archive import audit
from archive.approvals import readable
from archive.config import quiet_http
from archive.db import connect
from archive.export import digest, message_detail
from archive.storage import StorageError, open_under

log = logging.getLogger(__name__)


def path_chat(request):
    try:
        return int(request.path_params["chat_id"])
    except (KeyError, ValueError):
        return None


def create_app(cfg, clock=time.monotonic):
    quiet_http()
    app = FastAPI(docs_url=None, redoc_url=None, openapi_url=None)
    windows = audit.Windows(clock)

    @app.middleware("http")
    async def errors(request, call_next):
        try:
            response = await call_next(request)
        except Exception:
            response = JSONResponse({"error": "internal_error"}, status_code=500)
        response = await audited(request, response)
        response.headers["Cache-Control"] = "no-store"
        response.headers["X-Content-Type-Options"] = "nosniff"
        return response

    async def audited(request, response):
        """One read_audit row per matched /v1 route; an agent's read fails without it."""
        route = getattr(request.scope.get("route"), "path", "")
        if not route.startswith("/v1/"):
            return response
        state = request.state
        agent = getattr(state, "agent", None)
        if agent is None and windows.take(None, audit.ANONYMOUS_PER_MINUTE):
            return response
        chat = getattr(state, "chat_id", None)
        row = (agent, route, path_chat(request) if chat is None else chat)
        try:
            await run_in_threadpool(
                audit.record, cfg.dsn, *row, response.status_code, getattr(state, "count", None)
            )
        except Exception:
            log.warning("read_audit_failed")
            if agent is not None:
                return JSONResponse({"error": "internal_error"}, status_code=500)
        return response

    @app.exception_handler(RequestValidationError)
    async def invalid(request, exc):
        return JSONResponse({"error": "invalid_request"}, status_code=422)

    def scopes(request: Request, authorization: Annotated[str | None, Header()] = None):
        if not authorization or not authorization.startswith("Bearer "):
            raise HTTPException(401, "unauthorized")
        name = cfg.agent(authorization[7:])
        if name is None:
            raise HTTPException(401, "unauthorized")
        request.state.agent = name
        result = cfg.scopes(authorization[7:])
        if not result:
            raise HTTPException(401, "unauthorized")
        retry = windows.take(name, cfg.agents[name].rate_per_minute)
        if retry:
            raise HTTPException(429, "rate_limited", headers={"Retry-After": str(retry)})
        return list(result)

    def files(request: Request, _scope: Annotated[list[int], Depends(scopes)]):
        # Checked before any lookup: the same 403 for every ID leaks no existence.
        if not cfg.agents[request.state.agent].files:
            raise HTTPException(403, "files_not_allowed")

    def database(_scope: Annotated[list[int], Depends(scopes)]):
        with connect(cfg.dsn) as conn:
            conn.execute("SET default_transaction_read_only=on")
            with conn.transaction():
                yield conn

    DB = Annotated[object, Depends(database)]

    def visible(conn: DB, configured: Annotated[list[int], Depends(scopes)]):
        # Configured scope narrowed to chats the owner approved via the bot; a pending
        # or declined chat is never visible, even if listed in the agent's scope.
        return readable(conn, cfg, configured)

    Scopes = Annotated[list[int], Depends(visible)]

    def allowed(chat, scope):
        if chat not in scope:
            raise HTTPException(404, "not_found")

    @app.get("/health")
    def health():
        return {"status": "ok"}

    @app.get("/v1/status")
    def status(conn: DB, scope: Scopes):
        result = {}
        for table in ("messages", "attachments", "transcripts", "jobs"):
            result[table] = conn.execute(
                f"SELECT count(*) AS n FROM {table} WHERE chat_id=ANY(%s)", (scope,)
            ).fetchone()["n"]
        result["files_unavailable"] = conn.execute(
            "SELECT count(*) AS n FROM attachments WHERE chat_id=ANY(%s) AND status<>'ready'",
            (scope,),
        ).fetchone()["n"]
        result["files_oversized_not_downloaded"] = conn.execute(
            "SELECT count(*) AS n FROM attachments WHERE chat_id=ANY(%s) "
            "AND status='oversized_not_downloaded'",
            (scope,),
        ).fetchone()["n"]
        result["failed_jobs"] = conn.execute(
            "SELECT count(*) AS n FROM jobs WHERE chat_id=ANY(%s) AND status='failed'", (scope,)
        ).fetchone()["n"]
        result["oldest_pending_job"] = conn.execute(
            "SELECT min(created_at) AS oldest FROM jobs "
            "WHERE chat_id=ANY(%s) AND status IN ('pending','running')",
            (scope,),
        ).fetchone()["oldest"]
        result["last_committed_update"] = conn.execute(
            "SELECT max(received_at) AS latest FROM events WHERE chat_id=ANY(%s)", (scope,)
        ).fetchone()["latest"]
        return result

    @app.get("/v1/chats")
    def chats(request: Request, scope: Scopes):
        request.state.count = len(scope)
        return [{"chat_id": i, "slug": cfg.slug(i)} for i in sorted(scope)]

    @app.get("/v1/chats/{chat_id}/messages")
    def messages(
        request: Request,
        chat_id: int,
        conn: DB,
        scope: Scopes,
        start: Annotated[datetime | None, Query(alias="from")] = None,
        end: Annotated[datetime | None, Query(alias="to")] = None,
        author: int | None = None,
        limit: Annotated[int, Query(ge=1, le=100)] = 50,
        cursor: Annotated[int | None, Query(ge=0)] = None,
    ):
        allowed(chat_id, scope)
        for boundary in (start, end):
            if boundary and (
                boundary.tzinfo is None or boundary.utcoffset() != UTC.utcoffset(None)
            ):
                raise HTTPException(422, "utc_required")
        clauses = ["chat_id=%s"]
        params = [chat_id]
        for expression, value in (
            ("sent_at>=%s", start),
            ("sent_at<%s", end),
            ("author_id=%s", author),
            ("message_id>%s", cursor),
        ):
            if value is not None:
                clauses.append(expression)
                params.append(value)
        rows = conn.execute(
            "SELECT * FROM messages WHERE "
            + " AND ".join(clauses)
            + " ORDER BY message_id LIMIT %s",
            (*params, limit),
        ).fetchall()
        request.state.count = len(rows)
        return {
            "items": rows,
            "next_cursor": rows[-1]["message_id"] if len(rows) == limit else None,
        }

    @app.get("/v1/chats/{chat_id}/messages/{message_id}")
    def message(chat_id: int, message_id: int, conn: DB, scope: Scopes):
        allowed(chat_id, scope)
        result = message_detail(conn, chat_id, message_id)
        if result is None:
            raise HTTPException(404, "not_found")
        return result

    @app.get("/v1/search")
    def search(
        request: Request,
        conn: DB,
        scope: Scopes,
        q: Annotated[str, Query(min_length=1, max_length=512)],
        chat_id: int | None = None,
        limit: Annotated[int, Query(ge=1, le=100)] = 50,
        cursor: Annotated[int, Query(ge=0)] = 0,
    ):
        if chat_id is not None:
            request.state.chat_id = chat_id
            allowed(chat_id, scope)
            scope = [chat_id]
        # Literal substring, not wildcard expansion. SQL remains fully parameterized.
        rows = conn.execute(
            """SELECT m.* FROM messages m WHERE m.chat_id=ANY(%s)
            AND m.event_id>%s AND (strpos(lower(m.text),lower(%s))>0 OR EXISTS (
            SELECT 1 FROM attachments a JOIN transcripts t ON t.attachment_id=a.id
            WHERE a.chat_id=m.chat_id AND a.message_id=m.message_id
            AND strpos(lower(t.text),lower(%s))>0)) ORDER BY m.event_id LIMIT %s""",
            (scope, cursor, q, q, limit),
        ).fetchall()
        request.state.count = len(rows)
        return {"items": rows, "next_cursor": rows[-1]["event_id"] if len(rows) == limit else None}

    @app.get("/v1/files/{attachment_id}")
    def file(
        attachment_id: int,
        request: Request,
        _files: Annotated[None, Depends(files)],
        conn: DB,
        scope: Scopes,
    ):
        row = conn.execute(
            "SELECT * FROM attachments WHERE id=%s AND chat_id=ANY(%s)", (attachment_id, scope)
        ).fetchone()
        if not row:
            raise HTTPException(404, "not_found")
        request.state.chat_id = row["chat_id"]
        if row["status"] != "ready":
            # In-scope record whose bytes are not archived (pending, failed, blocked or
            # oversized_not_downloaded above the cloud limit): explicit, never a fake file.
            return JSONResponse(
                {
                    "error": "file_unavailable",
                    "status": row["status"],
                    "error_code": row["error_code"],
                },
                status_code=409,
            )
        try:
            handle = open_under(cfg.media_root, str(cfg.media_root / row["relative_path"]))
        except StorageError:
            raise HTTPException(404, "file_unavailable") from None

        def stream():
            try:
                while chunk := handle.read(1024 * 1024):
                    yield chunk
            finally:
                handle.close()

        return StreamingResponse(
            stream(),
            media_type="application/octet-stream",
            headers={
                "Content-Disposition": f'attachment; filename="attachment-{attachment_id}.bin"'
            },
        )

    @app.get("/v1/chats/{chat_id}/summary")
    def summary(chat_id: int, date: str, conn: DB, scope: Scopes):
        allowed(chat_id, scope)
        try:
            return digest(conn, cfg, chat_id, date)
        except ValueError:
            raise HTTPException(422, "invalid_date") from None

    return app
