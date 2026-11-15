"""Synthetic cloud Bot API: real BotAPI/httpx client over an injected transport."""

import json

import httpx

from archive.telegram import BotAPI


class CloudFiles:
    """Serves getFile and /file/bot<token>/<path> like api.telegram.org.

    ``files`` maps file_id -> bytes. ``too_big`` file_ids get Telegram's getFile 400.
    Request URLs are recorded only to assert they never leak elsewhere.
    """

    def __init__(self, cfg, files=None, too_big=(), report_size=True, extra=None):
        self.cfg = cfg
        self.files = dict(files or {})
        self.too_big = set(too_big)
        self.report_size = report_size
        self.extra = extra or {}
        self.calls = []

    def handler(self, request):
        assert request.url.scheme == "https" and request.url.host == "api.telegram.org"
        path = request.url.path
        prefix = "/file/bot" + self.cfg.bot_token + "/"
        if request.method == "GET" and path.startswith(prefix):
            name = path[len(prefix) :]
            self.calls.append(("download", name))
            file_id = name.rsplit("/", 1)[-1]
            if file_id not in self.files:
                return httpx.Response(404)
            return httpx.Response(200, content=self.files[file_id])
        method = path.rsplit("/", 1)[-1]
        body = json.loads(request.content or b"{}")
        self.calls.append((method, body.get("file_id")))
        if method == "getFile":
            file_id = body["file_id"]
            if file_id in self.too_big:
                return httpx.Response(
                    400,
                    json={
                        "ok": False,
                        "error_code": 400,
                        "description": "Bad Request: file is too big",
                    },
                )
            if file_id not in self.files:
                return httpx.Response(
                    400, json={"ok": False, "error_code": 400, "description": "Bad Request"}
                )
            result = {"file_id": file_id, "file_path": "documents/" + file_id}
            if self.report_size:
                result["file_size"] = len(self.files[file_id])
            return httpx.Response(200, json={"ok": True, "result": result})
        results = {"getMe": {"id": self.cfg.expected_bot_id}, "getWebhookInfo": {"url": ""}}
        results.update(self.extra)
        return httpx.Response(200, json={"ok": True, "result": results.get(method, True)})

    def bot(self):
        return BotAPI(self.cfg, httpx.MockTransport(self.handler))

    def count(self, kind):
        return sum(1 for call in self.calls if call[0] == kind)
