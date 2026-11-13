#!/usr/bin/env python3
"""Example implementation: a read-only cross-platform context bridge.

An MCP server that lets an AI on platform B (e.g. ChatGPT) read the ORIGINAL
conversation history from platform A (here: a Codex-based Telegram bridge's
session logs) — recent messages plus keyword search across all history.
This is the L3 (raw-record archive) layer of the three-layer memory method,
exposed cross-platform. Pattern transfers to any log-backed agent.

  - Streamable HTTP MCP at /mcp, Bearer access token required (1h)
  - OAuth 2.1 authorization code + PKCE (S256), dynamic client registration
  - Refresh tokens (file-persisted, rotated) so the client stays connected
  - Authorize page protected by a single owner passphrase (sha256 hash in env)
Config via env: TGCTX_ISSUER (public base URL), TGCTX_PORT,
TGCTX_DATA_BASE (agent data dir), TGCTX_STORE, TGCTX_PASSPHRASE_HASH.
Only stdlib. Strictly read-only toward the source data.
"""
import base64
import hashlib
import hmac
import json
import os
import re
import secrets
import threading
import time
import urllib.parse
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

# ---------------------------------------------------------------- reader ---
BASE = os.environ.get("TGCTX_DATA_BASE",
                      "/var/lib/docker/volumes/codex-wirebot-data/_data")
POINTER = os.path.join(BASE, "workspace", "telegram-bridge", "current-thread.json")
SESSIONS = os.path.join(BASE, "codex-home", "sessions")
CST = timezone(timedelta(hours=8))
TG_MARK = re.compile(r"^\[Telegram[^\]]*\]\s*\n?")
_cache = {"key": None, "msgs": None}
_cache_lock = threading.Lock()


def session_path():
    tid = None
    try:
        with open(POINTER, "r", encoding="utf-8") as f:
            tid = json.load(f).get("thread_id")
    except Exception:
        pass
    best = None
    for root, _dirs, files in os.walk(SESSIONS):
        for name in files:
            if not name.endswith(".jsonl"):
                continue
            if tid and tid in name:
                return os.path.join(root, name)
            if best is None or name > os.path.basename(best):
                best = os.path.join(root, name)
    return best


def tail_lines(path, want_tg, max_bytes=48 * 1024 * 1024):
    size = os.path.getsize(path)
    read = 0
    chunks = []
    tg_seen = 0
    needle = "Telegram\u6d88\u606f".encode("utf-8")
    with open(path, "rb") as f:
        pos = size
        while pos > 0 and read < max_bytes and tg_seen < want_tg:
            step = min(1024 * 1024, pos)
            pos -= step
            f.seek(pos)
            data = f.read(step)
            chunks.append(data)
            read += step
            tg_seen += data.count(needle)
    raw = b"".join(reversed(chunks))
    lines = raw.split(b"\n")
    if pos > 0 and lines:
        lines = lines[1:]
    return lines


def _parse_all():
    path = session_path()
    if not path:
        return []
    st = os.stat(path)
    key = (path, st.st_mtime_ns, st.st_size)
    with _cache_lock:
        if _cache["key"] == key and _cache["msgs"] is not None:
            return _cache["msgs"]
    lines = tail_lines(path, want_tg=420)
    msgs = []
    last_was_tg_user = False
    for raw in lines:
        raw = raw.strip()
        if not raw:
            continue
        try:
            e = json.loads(raw.decode("utf-8", "replace"))
        except Exception:
            continue
        p = e.get("payload")
        if not isinstance(p, dict):
            continue
        ts_raw = e.get("timestamp", "")
        try:
            dt = datetime.fromisoformat(ts_raw.replace("Z", "+00:00")).astimezone(CST)
            ts = dt.strftime("%Y-%m-%d %H:%M")
        except Exception:
            dt, ts = None, ts_raw
        if e.get("type") == "response_item" and p.get("role") == "user":
            texts = []
            c = p.get("content")
            if isinstance(c, list):
                for it in c:
                    if isinstance(it, dict) and it.get("text"):
                        texts.append(str(it["text"]))
            text = "\n".join(texts).strip()
            if text.startswith("[Telegram"):
                body = TG_MARK.sub("", text, count=1).strip()
                msgs.append({"time": ts, "dt": dt, "sender": "\u6708\u6708", "text": body[:2000]})
                last_was_tg_user = True
            else:
                last_was_tg_user = False
        elif e.get("type") == "event_msg" and p.get("type") == "task_complete":
            reply = p.get("last_agent_message")
            if last_was_tg_user and isinstance(reply, str) and reply.strip():
                msgs.append({"time": ts, "dt": dt, "sender": "Codex", "text": reply.strip()[:2000]})
            last_was_tg_user = False
    with _cache_lock:
        _cache["key"] = key
        _cache["msgs"] = msgs
    return msgs


def extract(limit, offset=0, within_hours=None):
    msgs = _parse_all()
    if within_hours is not None and within_hours > 0:
        cutoff = datetime.now(CST) - timedelta(hours=within_hours)
        sel = [m for m in msgs if m.get("dt") is not None and m["dt"] >= cutoff][-200:]
    else:
        end = len(msgs) - offset if offset else len(msgs)
        if end < 0:
            end = 0
        sel = msgs[max(0, end - limit):end]
    return [{"time": m["time"], "sender": m["sender"], "text": m["text"]} for m in sel]


# ------------------------------------------------------- history search ---
# Keyword search spans ALL session files (same extraction rules as
# session_history.py on the Codex side), cached on a dir fingerprint.
import glob as _glob

_hist_cache = {"key": None, "msgs": None}


def _parse_history():
    files = sorted(_glob.glob(os.path.join(SESSIONS, "**", "rollout-*.jsonl"), recursive=True))
    fp = []
    for p in files:
        try:
            st = os.stat(p)
            fp.append((p, st.st_size, st.st_mtime_ns))
        except OSError:
            continue
    fp = tuple(fp)
    with _cache_lock:
        if _hist_cache["key"] == fp and _hist_cache["msgs"] is not None:
            return _hist_cache["msgs"]
    msgs = []
    for path in files:
        pending = None  # (dt, ts, text) TG user msg waiting for its reply
        try:
            f = open(path, encoding="utf-8")
        except OSError:
            continue
        with f:
            for line in f:
                try:
                    e = json.loads(line)
                except Exception:
                    continue
                ts_raw = e.get("timestamp")
                if not ts_raw:
                    continue
                try:
                    dt = datetime.fromisoformat(ts_raw.replace("Z", "+00:00")).astimezone(CST)
                    ts = dt.strftime("%Y-%m-%d %H:%M")
                except Exception:
                    continue
                p = e.get("payload")
                if not isinstance(p, dict):
                    continue
                if e.get("type") == "response_item" and p.get("role") == "user":
                    texts = []
                    c = p.get("content")
                    if isinstance(c, list):
                        for it in c:
                            if isinstance(it, dict) and it.get("text"):
                                texts.append(str(it["text"]))
                    text = "\n".join(texts).strip()
                    if text.startswith("[Telegram"):
                        if pending is not None:
                            msgs.append({"time": pending[1], "dt": pending[0],
                                         "sender": "\u6708\u6708", "text": pending[2]})
                        pending = (dt, ts, TG_MARK.sub("", text, count=1).strip())
                elif e.get("type") == "event_msg" and p.get("type") == "task_complete":
                    if pending is not None:
                        msgs.append({"time": pending[1], "dt": pending[0],
                                     "sender": "\u6708\u6708", "text": pending[2]})
                        reply = p.get("last_agent_message")
                        if isinstance(reply, str) and reply.strip():
                            msgs.append({"time": ts, "dt": dt, "sender": "Codex",
                                         "text": reply.strip()})
                        pending = None
        if pending is not None:
            msgs.append({"time": pending[1], "dt": pending[0],
                         "sender": "\u6708\u6708", "text": pending[2]})
    msgs.sort(key=lambda m: m["dt"])
    with _cache_lock:
        _hist_cache["key"] = fp
        _hist_cache["msgs"] = msgs
    return msgs


def search(query, limit=20):
    q = (query or "").strip().casefold()
    if not q:
        return 0, []
    msgs = _parse_history()
    hits = [m for m in msgs if q in m["text"].casefold()]
    sel = hits[-limit:]
    return len(hits), [{"time": m["time"], "sender": m["sender"], "text": m["text"][:500]}
                        for m in sel]


TOOL = {
    "name": "get_recent_telegram_messages",
    "description": (
        "Read the user's most recent Telegram conversation with her AI companion "
        "(Codex). Returns the ORIGINAL messages in order, each with time, sender "
        "(\u6708\u6708 = the user, Codex = the companion) and full text (not a summary). "
        "Use this when the user says things like 'continue what we were just talking "
        "about' or asks what was just discussed on Telegram. Default: the latest 50 "
        "messages. If the topic still lacks context, call again with offset to page "
        "further back (e.g. offset=50 returns the 50 messages before those). "
        "Alternatively pass within_hours to get everything from a recent time "
        "window (0.5, 2 or 24 hours)."
    ),
    "inputSchema": {
        "type": "object",
        "properties": {
            "limit": {"type": "integer", "description": "How many messages to return (1-100). Default 50.",
                      "default": 50, "minimum": 1, "maximum": 100},
            "offset": {"type": "integer", "description": "Skip this many of the most recent messages first, to read an older batch. Default 0.",
                       "default": 0, "minimum": 0, "maximum": 500},
            "within_hours": {"type": "number", "description": "Only return messages from the last X hours (e.g. 0.5, 2, 24). When set, limit/offset are ignored.",
                             "minimum": 0.1, "maximum": 168},
        },
        "additionalProperties": False,
    },
    "annotations": {"readOnlyHint": True, "destructiveHint": False, "openWorldHint": False},
}

SEARCH_TOOL = {
    "name": "search_telegram_messages",
    "description": (
        "Search ALL of the user's Telegram conversation history with her AI companion "
        "(Codex) by keyword, across every past conversation. Returns the matching "
        "ORIGINAL messages (the most recent matches, in chronological order), each "
        "with time, sender (\u6708\u6708 = the user, Codex = the companion) and text. "
        "Use this when the user asks about a specific topic, thing or earlier "
        "conversation (e.g. 'what did we say about X', 'find what I mentioned about "
        "Y before'), or when the recent messages do not contain what you need. For "
        "just catching up on the latest conversation, use "
        "get_recent_telegram_messages instead."
    ),
    "inputSchema": {
        "type": "object",
        "properties": {
            "query": {"type": "string", "description": "The keyword or phrase to search for in the message texts."},
            "limit": {"type": "integer", "description": "Max matching messages to return (1-50). Default 20.",
                      "default": 20, "minimum": 1, "maximum": 50},
        },
        "required": ["query"],
        "additionalProperties": False,
    },
    "annotations": {"readOnlyHint": True, "destructiveHint": False, "openWorldHint": False},
}

# ----------------------------------------------------------------- oauth ---
ISSUER = os.environ.get("TGCTX_ISSUER", "https://example.com").rstrip("/")
PORT = int(os.environ.get("TGCTX_PORT", "8932"))
STORE_PATH = os.environ.get("TGCTX_STORE", "/root/tg-context-mcp/oauth_store.json")
PASSPHRASE_HASH = os.environ.get("TGCTX_PASSPHRASE_HASH", "")
_store_lock = threading.Lock()
_codes = {}
_fails = {"count": 0, "locked_until": 0.0}


def _sha(s):
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


def store_load():
    try:
        with open(STORE_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {"clients": {}, "access": {}, "refresh": {}}


def store_save(data):
    tmp = STORE_PATH + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f)
    os.chmod(tmp, 0o600)
    os.replace(tmp, STORE_PATH)


def issue_tokens(client_id):
    access = secrets.token_urlsafe(48)
    refresh = secrets.token_urlsafe(64)
    with _store_lock:
        data = store_load()
        now = time.time()
        data["access"][_sha(access)] = {"client_id": client_id, "exp": now + 3600}
        data["refresh"][_sha(refresh)] = {"client_id": client_id, "exp": now + 90 * 86400}
        # prune expired
        data["access"] = {k: v for k, v in data["access"].items() if v["exp"] > now}
        data["refresh"] = {k: v for k, v in data["refresh"].items() if v["exp"] > now}
        store_save(data)
    return access, refresh


def check_access(token):
    if not token:
        return False
    with _store_lock:
        data = store_load()
        rec = data.get("access", {}).get(_sha(token))
        return bool(rec and rec["exp"] > time.time())


# ---------------------------------------------------------------- handler ---
class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, fmt, *args):
        pass

    def _send(self, code, body_bytes, ctype="application/json", headers=None):
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body_bytes)))
        if headers:
            for k, v in headers.items():
                self.send_header(k, v)
        self.end_headers()
        self.wfile.write(body_bytes)

    def _json(self, code, obj, headers=None):
        self._send(code, json.dumps(obj, ensure_ascii=False).encode("utf-8"), headers=headers)

    def _read_body(self):
        length = int(self.headers.get("Content-Length", "0") or 0)
        return self.rfile.read(length) if length else b""

    # ------------------------------------------------------------ GET ----
    def do_GET(self):
        path = urllib.parse.urlparse(self.path).path
        if path == "/health":
            return self._json(200, {"ok": True})
        if path in ("/.well-known/oauth-protected-resource", "/.well-known/oauth-protected-resource/mcp"):
            return self._json(200, {
                "resource": ISSUER + "/mcp",
                "authorization_servers": [ISSUER],
                "scopes_supported": ["mcp"],
                "bearer_methods_supported": ["header"],
            })
        if path in ("/.well-known/oauth-authorization-server", "/.well-known/openid-configuration"):
            return self._json(200, {
                "issuer": ISSUER,
                "authorization_endpoint": ISSUER + "/oauth/authorize",
                "token_endpoint": ISSUER + "/oauth/token",
                "registration_endpoint": ISSUER + "/oauth/register",
                "response_types_supported": ["code"],
                "grant_types_supported": ["authorization_code", "refresh_token"],
                "code_challenge_methods_supported": ["S256"],
                "token_endpoint_auth_methods_supported": ["none", "client_secret_post", "client_secret_basic"],
                "scopes_supported": ["mcp"],
            })
        if path == "/oauth/authorize":
            return self._authorize_page(urllib.parse.parse_qs(urllib.parse.urlparse(self.path).query))
        if path == "/mcp":
            return self._json(405, {"error": "use POST"})
        return self._json(404, {"error": "not found"})

    def _authorize_page(self, q, error=""):
        def g(k):
            return (q.get(k) or [""])[0]
        hidden = {k: g(k) for k in ("response_type", "client_id", "redirect_uri", "code_challenge",
                                    "code_challenge_method", "state", "scope")}
        inputs = "".join(f'<input type="hidden" name="{k}" value="{html_escape(v)}">' for k, v in hidden.items())
        err = f'<p style="color:#c00">{html_escape(error)}</p>' if error else ""
        page = f"""<!doctype html><html lang="zh"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1"><title>Telegram 记录 · 授权</title></head>
<body style="font-family:-apple-system,sans-serif;max-width:420px;margin:60px auto;padding:0 20px">
<h2>连接 Telegram 聊天记录</h2>
<p>有个应用想读取你的 Telegram 最近对话（只读）。确认是你本人的操作，请输入授权口令：</p>
{err}
<form method="POST" action="/oauth/authorize">
{inputs}
<input type="password" name="passphrase" placeholder="授权口令" autofocus
 style="width:100%;padding:12px;font-size:16px;box-sizing:border-box">
<button type="submit" style="width:100%;padding:12px;margin-top:14px;font-size:16px">授权连接</button>
</form></body></html>"""
        self._send(200, page.encode("utf-8"), ctype="text/html; charset=utf-8")

    # ------------------------------------------------------------ POST ---
    def do_POST(self):
        path = urllib.parse.urlparse(self.path).path
        if path == "/oauth/register":
            return self._register()
        if path == "/oauth/authorize":
            form = urllib.parse.parse_qs(self._read_body().decode("utf-8", "replace"))
            return self._authorize_submit(form)
        if path == "/oauth/token":
            form = urllib.parse.parse_qs(self._read_body().decode("utf-8", "replace"))
            return self._token(form)
        if path == "/mcp":
            return self._mcp()
        return self._json(404, {"error": "not found"})

    def _register(self):
        try:
            body = json.loads(self._read_body().decode("utf-8")) if True else {}
        except Exception:
            return self._json(400, {"error": "invalid_request"})
        client_id = "tgctx-" + secrets.token_hex(12)
        secret = secrets.token_urlsafe(32)
        with _store_lock:
            data = store_load()
            data["clients"][client_id] = {
                "redirect_uris": body.get("redirect_uris") or [],
                "client_secret_hash": _sha(secret),
                "client_name": body.get("client_name", ""),
            }
            store_save(data)
        return self._json(201, {
            "client_id": client_id,
            "client_secret": secret,
            "redirect_uris": body.get("redirect_uris") or [],
            "grant_types": ["authorization_code", "refresh_token"],
            "response_types": ["code"],
            "token_endpoint_auth_method": body.get("token_endpoint_auth_method", "client_secret_basic"),
        })

    def _authorize_submit(self, q):
        def g(k):
            return (q.get(k) or [""])[0]
        now = time.time()
        if _fails["locked_until"] > now:
            return self._authorize_page(q, error="尝试次数过多，请十分钟后再试。")
        ok = bool(PASSPHRASE_HASH) and hmac.compare_digest(_sha(g("passphrase")), PASSPHRASE_HASH)
        if not ok:
            _fails["count"] += 1
            if _fails["count"] >= 5:
                _fails["locked_until"] = now + 600
                _fails["count"] = 0
            return self._authorize_page(q, error="口令不对，再检查一下。")
        _fails["count"] = 0
        client_id, redirect_uri = g("client_id"), g("redirect_uri")
        with _store_lock:
            data = store_load()
            client = data.get("clients", {}).get(client_id)
        if not client or (client["redirect_uris"] and redirect_uri not in client["redirect_uris"]):
            return self._json(400, {"error": "invalid_client"})
        if g("response_type") != "code" or not g("code_challenge"):
            return self._json(400, {"error": "invalid_request"})
        code = secrets.token_urlsafe(32)
        _codes[code] = {"client_id": client_id, "redirect_uri": redirect_uri,
                        "challenge": g("code_challenge"), "exp": now + 300}
        params = {"code": code}
        if g("state"):
            params["state"] = g("state")
        sep = "&" if "?" in redirect_uri else "?"
        self._send(302, b"", headers={"Location": redirect_uri + sep + urllib.parse.urlencode(params)})

    def _client_auth_ok(self, form, client_id):
        with _store_lock:
            data = store_load()
            client = data.get("clients", {}).get(client_id)
        if not client:
            return False
        provided = (form.get("client_secret") or [""])[0]
        if not provided:
            auth = self.headers.get("Authorization", "")
            if auth.startswith("Basic "):
                try:
                    dec = base64.b64decode(auth[6:]).decode("utf-8")
                    provided = dec.split(":", 1)[1] if ":" in dec else ""
                except Exception:
                    provided = ""
        if not provided:
            return True  # public client + PKCE
        return hmac.compare_digest(_sha(provided), client["client_secret_hash"])

    def _token(self, form):
        def g(k):
            return (form.get(k) or [""])[0]
        grant = g("grant_type")
        client_id = g("client_id")
        if grant == "authorization_code":
            code = g("code")
            rec = _codes.pop(code, None)
            if not rec or rec["exp"] < time.time():
                return self._json(400, {"error": "invalid_grant"})
            if not client_id:
                client_id = rec["client_id"]
            if rec["client_id"] != client_id or (g("redirect_uri") and g("redirect_uri") != rec["redirect_uri"]):
                return self._json(400, {"error": "invalid_grant"})
            verifier = g("code_verifier")
            digest = base64.urlsafe_b64encode(hashlib.sha256(verifier.encode()).digest()).rstrip(b"=").decode()
            if not verifier or not hmac.compare_digest(digest, rec["challenge"]):
                return self._json(400, {"error": "invalid_grant"})
            if not self._client_auth_ok(form, client_id):
                return self._json(401, {"error": "invalid_client"})
            access, refresh = issue_tokens(client_id)
            return self._json(200, {"access_token": access, "token_type": "Bearer",
                                    "expires_in": 3600, "refresh_token": refresh, "scope": "mcp"})
        if grant == "refresh_token":
            rt = g("refresh_token")
            with _store_lock:
                data = store_load()
                rec = data.get("refresh", {}).get(_sha(rt))
                if rec and rec["exp"] > time.time():
                    del data["refresh"][_sha(rt)]
                    store_save(data)
                else:
                    rec = None
            if not rec:
                return self._json(400, {"error": "invalid_grant"})
            if client_id and rec["client_id"] != client_id:
                return self._json(400, {"error": "invalid_grant"})
            if not self._client_auth_ok(form, rec["client_id"]):
                return self._json(401, {"error": "invalid_client"})
            access, refresh = issue_tokens(rec["client_id"])
            return self._json(200, {"access_token": access, "token_type": "Bearer",
                                    "expires_in": 3600, "refresh_token": refresh, "scope": "mcp"})
        return self._json(400, {"error": "unsupported_grant_type"})

    # -------------------------------------------------------------- MCP --
    def _mcp(self):
        auth = self.headers.get("Authorization", "")
        token = auth[7:] if auth.startswith("Bearer ") else ""
        if not check_access(token):
            meta = ISSUER + "/.well-known/oauth-protected-resource"
            return self._json(401, {"error": "invalid_token"},
                              headers={"WWW-Authenticate": f'Bearer resource_metadata="{meta}"'})
        try:
            req = json.loads(self._read_body().decode("utf-8"))
        except Exception:
            return self._json(400, {"jsonrpc": "2.0", "id": None,
                                    "error": {"code": -32700, "message": "parse error"}})
        method = req.get("method", "")
        rid = req.get("id")
        if method.startswith("notifications/") or rid is None:
            self.send_response(202)
            self.send_header("Content-Length", "0")
            self.end_headers()
            return
        if method == "initialize":
            params = req.get("params") or {}
            return self._json(200, {"jsonrpc": "2.0", "id": rid, "result": {
                "protocolVersion": params.get("protocolVersion", "2025-03-26"),
                "capabilities": {"tools": {}},
                "serverInfo": {"name": "telegram-context", "version": "1.1.0"}}},
                headers={"Mcp-Session-Id": "tgctx-session"})
        if method == "ping":
            return self._json(200, {"jsonrpc": "2.0", "id": rid, "result": {}})
        if method == "tools/list":
            return self._json(200, {"jsonrpc": "2.0", "id": rid, "result": {"tools": [TOOL, SEARCH_TOOL]}})
        if method == "tools/call":
            params = req.get("params") or {}
            if params.get("name") == SEARCH_TOOL["name"]:
                sargs = params.get("arguments") or {}
                query = str(sargs.get("query", "") or "").strip()
                try:
                    slimit = max(1, min(50, int(sargs.get("limit", 20))))
                except Exception:
                    slimit = 20
                try:
                    if not query:
                        sresult = {"content": [{"type": "text", "text": "请提供要搜索的关键词（query）"}],
                                   "isError": True}
                    else:
                        total, smsgs = search(query, limit=slimit)
                        if total == 0:
                            stext = f"没有找到包含「{query}」的 Telegram 对话。"
                        else:
                            body = "\n\n".join(f"[{m['time']}] {m['sender']}: {m['text']}" for m in smsgs)
                            stext = f"关键词「{query}」共找到 {total} 条，显示最近 {len(smsgs)} 条：\n\n{body}"
                        sresult = {"content": [{"type": "text", "text": stext}],
                                   "structuredContent": {"total": total, "messages": smsgs},
                                   "isError": False}
                except Exception as exc:
                    sresult = {"content": [{"type": "text", "text": f"搜索失败: {type(exc).__name__}"}],
                               "isError": True}
                return self._json(200, {"jsonrpc": "2.0", "id": rid, "result": sresult})
            if params.get("name") != TOOL["name"]:
                return self._json(200, {"jsonrpc": "2.0", "id": rid, "result": {
                    "content": [{"type": "text", "text": "unknown tool"}], "isError": True}})
            args = params.get("arguments") or {}
            try:
                limit = max(1, min(100, int(args.get("limit", 50))))
            except Exception:
                limit = 50
            try:
                offset = max(0, min(500, int(args.get("offset", 0))))
            except Exception:
                offset = 0
            wh = args.get("within_hours")
            try:
                wh = float(wh) if wh is not None else None
            except Exception:
                wh = None
            try:
                msgs = extract(limit, offset=offset, within_hours=wh)
                text = "\n\n".join(f"[{m['time']}] {m['sender']}: {m['text']}" for m in msgs) \
                    if msgs else "（最近没有读到 Telegram 对话）"
                result = {"content": [{"type": "text", "text": text}],
                          "structuredContent": {"messages": msgs}, "isError": False}
            except Exception as exc:
                result = {"content": [{"type": "text", "text": f"读取失败: {type(exc).__name__}"}],
                          "isError": True}
            return self._json(200, {"jsonrpc": "2.0", "id": rid, "result": result})
        return self._json(200, {"jsonrpc": "2.0", "id": rid,
                                "error": {"code": -32601, "message": "method not found"}})


def html_escape(s):
    return (s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
             .replace('"', "&quot;"))


if __name__ == "__main__":
    if not PASSPHRASE_HASH:
        raise SystemExit("TGCTX_PASSPHRASE_HASH env required")
    ThreadingHTTPServer(("127.0.0.1", PORT), Handler).serve_forever()
