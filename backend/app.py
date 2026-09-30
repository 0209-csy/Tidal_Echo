import asyncio
import hmac
import json
import os
import re
import urllib.request
import urllib.parse
from datetime import datetime, timezone

import psycopg
from fastapi import FastAPI, Request, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import Response, StreamingResponse

SECRET = os.environ.get("RELAY_SECRET", "")
AI_NAME = os.environ.get("RELAY_AI_NAME", "AI")
HUMAN_NAME = os.environ.get("RELAY_HUMAN_NAME", "对方")
PUBLIC_PREFIX = os.environ.get("RELAY_PUBLIC_PREFIX", "/relay")
APP_PATH = os.environ.get("RELAY_APP_PATH", "/")
DATABASE_URL = os.environ.get("DATABASE_URL") or os.environ.get("POSTGRES_URL", "")
ALLOW_ORIGINS = [x.strip() for x in os.environ.get(
    "RELAY_ALLOW_ORIGINS", "*"
).split(",") if x.strip()]
PRESENCE_ONLINE_SEC = int(os.environ.get("RELAY_PRESENCE_ONLINE_SEC", "180"))
PRESENCE_RECENT_SEC = int(os.environ.get("RELAY_PRESENCE_RECENT_SEC", "1800"))
MINIMAX_API_BASE = os.environ.get("MINIMAX_API_BASE", "https://api.minimaxi.com")
MINIMAX_API_KEY = os.environ.get("MINIMAX_API_KEY", "")
MINIMAX_GROUP_ID = os.environ.get("MINIMAX_GROUP_ID", "")
MINIMAX_MODEL = os.environ.get("MINIMAX_MODEL", "speech-02-hd")
MINIMAX_VOICE_ZH = os.environ.get("MINIMAX_VOICE_ZH", "")
MINIMAX_TTS_TIMEOUT = float(os.environ.get("MINIMAX_TTS_TIMEOUT", "30"))
PUSH_PREVIEW_CHARS = int(os.environ.get("RELAY_PUSH_PREVIEW_CHARS", "120"))

if not SECRET:
    raise RuntimeError("RELAY_SECRET is required")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL is required")

def now_iso():
    return datetime.now(timezone.utc).isoformat()

def conn():
    return psycopg.connect(DATABASE_URL, sslmode="require")

def init_db():
    with conn() as c:
        c.execute("""
        CREATE TABLE IF NOT EXISTS messages (
            id BIGSERIAL PRIMARY KEY,
            ts TEXT NOT NULL,
            direction TEXT NOT NULL,
            kind TEXT NOT NULL,
            text TEXT NOT NULL,
            meta JSONB NOT NULL DEFAULT '{}'::jsonb
        )
        """)
        c.execute("""
        CREATE TABLE IF NOT EXISTS push_subscriptions (
            endpoint TEXT PRIMARY KEY,
            p256dh TEXT NOT NULL,
            auth TEXT NOT NULL,
            ua TEXT,
            created TEXT NOT NULL,
            last_ok TEXT
        )
        """)
        c.execute("""
        CREATE TABLE IF NOT EXISTS relay_events (
            id BIGSERIAL PRIMARY KEY,
            ts TEXT NOT NULL,
            event_type TEXT NOT NULL,
            payload JSONB NOT NULL DEFAULT '{}'::jsonb
        )
        """)
        c.commit()

_db_ready = False
def ensure_db():
    global _db_ready
    if not _db_ready:
        init_db()
        _db_ready = True

def save_message(direction, kind, text, meta):
    ensure_db()
    ts = (meta or {}).get("ts") or now_iso()
    with conn() as c:
        row = c.execute(
            "INSERT INTO messages (ts,direction,kind,text,meta) VALUES (%s,%s,%s,%s,%s) RETURNING id",
            (ts, direction, kind, text, json.dumps(meta or {}, ensure_ascii=False))
        ).fetchone()
        c.commit()
    return {"id": int(row[0]), "ts": ts, "direction": direction,
            "kind": kind, "text": text, "meta": meta or {}}

def row_message(r):
    return {
        "id": int(r[0]), "ts": r[1], "direction": r[2],
        "kind": r[3], "text": r[4], "meta": r[5] or {}
    }

def get_messages(since=0, limit=200, direction=None, session_id=None):
    ensure_db()
    where = ["id > %s"]
    args = [since]
    if direction:
        where.append("direction = %s")
        args.append(direction)
    if session_id:
        if session_id == "__legacy__":
            where.append("(meta->>'api_session' IS NULL OR meta->>'api_session' = '')")
        else:
            where.append("meta->>'api_session' = %s")
            args.append(session_id)
    sql = "SELECT id,ts,direction,kind,text,meta FROM messages WHERE " + \
          " AND ".join(where) + " ORDER BY id ASC LIMIT %s"
    args.append(min(limit, 500))
    with conn() as c:
        rows = c.execute(sql, args).fetchall()
    return [row_message(r) for r in rows]

def latest_message():
    ensure_db()
    with conn() as c:
        r = c.execute(
            "SELECT id,ts,direction,kind,text,meta FROM messages "
            "WHERE kind != 'thinking' ORDER BY id DESC LIMIT 1"
        ).fetchone()
    return row_message(r) if r else None

def add_event(event_type, payload):
    ensure_db()
    with conn() as c:
        r = c.execute(
            "INSERT INTO relay_events(ts,event_type,payload) VALUES(%s,%s,%s) RETURNING id",
            (now_iso(), event_type, json.dumps(payload or {}, ensure_ascii=False))
        ).fetchone()
        c.commit()
    return int(r[0])

def get_events(since, limit=100):
    ensure_db()
    with conn() as c:
        rows = c.execute(
            "SELECT id,ts,event_type,payload FROM relay_events "
            "WHERE id > %s ORDER BY id ASC LIMIT %s", (since, limit)
        ).fetchall()
    return [{"id": int(r[0]), "ts": r[1], "type": r[2], "payload": r[3] or {}} for r in rows]

def app_payload(m):
    return {
        "id": m["id"], "ts": m["ts"],
        "from": "human" if m["direction"] == "in" else "ai",
        "kind": m["kind"], "text": m["text"], "meta": m["meta"]
    }

def plugin_payload(m):
    meta = m.get("meta") or {}
    return {
        "id": m["id"], "content": m["text"],
        "user": meta.get("user") or "human",
        "ts": m["ts"], "attachments": meta.get("attachments") or []
    }

def check_auth(request):
    auth = request.headers.get("authorization", "")
    token = auth[7:] if auth.startswith("Bearer ") else request.query_params.get("token")
    if not token or not hmac.compare_digest(token, SECRET):
        raise HTTPException(401, "unauthorized")

def sse_data(payload):
    eid = payload.get("id")
    prefix = f"id: {eid}\n" if eid is not None else ""
    return prefix + "data: " + json.dumps(payload, ensure_ascii=False) + "\n\n"

async def poll_sse(request, direction=None, since=0):
    last_id = int(since or 0)
    last_event = 0
    yield "retry: 3000\n: connected\n\n"
    while not await request.is_disconnected():
        rows = get_messages(last_id, 100, direction=direction)
        for m in rows:
            last_id = m["id"]
            yield sse_data(plugin_payload(m) if direction == "in" else app_payload(m))
        events = get_events(last_event, 100)
        for e in events:
            last_event = e["id"]
            payload = dict(e["payload"])
            payload.setdefault("type", e["type"])
            yield sse_data(payload)
        await asyncio.sleep(1.2)

def minimax_tts_mp3(text):
    if not MINIMAX_API_KEY or not MINIMAX_VOICE_ZH:
        raise HTTPException(503, "minimax tts not configured")
    clean = (text or "").strip()[:900]
    if not clean:
        raise HTTPException(400, "empty text")
    url = f"{MINIMAX_API_BASE.rstrip('/')}/v1/t2a_v2"
    if MINIMAX_GROUP_ID:
        url += "?GroupId=" + urllib.parse.quote(MINIMAX_GROUP_ID)
    payload = {
        "model": MINIMAX_MODEL, "text": clean, "stream": False,
        "voice_setting": {"voice_id": MINIMAX_VOICE_ZH, "speed": 1.0, "vol": 1.0, "pitch": 0},
        "audio_setting": {"sample_rate": 32000, "bitrate": 128000, "format": "mp3", "channel": 1}
    }
    req = urllib.request.Request(
        url, data=json.dumps(payload, ensure_ascii=False).encode(),
        headers={"Authorization": f"Bearer {MINIMAX_API_KEY}", "Content-Type": "application/json"},
        method="POST"
    )
    try:
        with urllib.request.urlopen(req, timeout=MINIMAX_TTS_TIMEOUT) as r:
            data = json.loads(r.read().decode())
    except Exception as e:
        raise HTTPException(502, f"minimax tts failed: {e}")
    audio_hex = (data.get("data") or {}).get("audio")
    if not audio_hex:
        raise HTTPException(502, "minimax tts returned no audio")
    try:
        return bytes.fromhex(audio_hex)
    except ValueError:
        raise HTTPException(502, "bad minimax audio payload")

app = FastAPI(title="Tidal Echo Relay - Vercel")
app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOW_ORIGINS,
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/healthz")
async def healthz():
    ensure_db()
    return {"ok": True, "runtime": "vercel", "database": "postgres"}

@app.get("/channel/in")
async def channel_in(request: Request, since: int = 0, limit: int = 100):
    check_auth(request)
    return StreamingResponse(
        poll_sse(request, direction="in", since=since),
        media_type="text/event-stream",
        headers={"Cache-Control":"no-cache, no-transform", "X-Accel-Buffering":"no"}
    )

@app.post("/channel/out")
async def channel_out(request: Request):
    check_auth(request)
    body = await request.json()
    kind = body.get("type", "reply")
    if kind in ("thinking_delta", "reply_delta"):
        # Vercel version saves each completed stream chunk as a normal message.
        # The frontend remains compatible; true cross-instance draft pub/sub is intentionally avoided.
        if body.get("done"):
            text = body.get("final_text") or body.get("text") or ""
            base = kind[:-6]
            if text:
                m = save_message("out", base, text, {
                    k:v for k,v in body.items()
                    if k not in ("type","text","done","final_text")
                })
                add_event("typing", {"active": False})
                return {"id": m["id"], "stream_id": body.get("stream_id"), "saved": True}
        return {"ok": True, "draft": True}
    if kind == "react":
        mid = int(body.get("id") or 0)
        emoji = (body.get("emoji") or "").strip()
        ensure_db()
        with conn() as c:
            r = c.execute("SELECT meta FROM messages WHERE id=%s", (mid,)).fetchone()
            if not r:
                raise HTTPException(404, "message not found")
            meta = r[0] or {}
            reactions = meta.get("reactions") or {}
            if emoji: reactions["ai"] = emoji
            else: reactions.pop("ai", None)
            if reactions: meta["reactions"] = reactions
            else: meta.pop("reactions", None)
            c.execute("UPDATE messages SET meta=%s WHERE id=%s",
                      (json.dumps(meta, ensure_ascii=False), mid))
            c.commit()
        add_event("reaction", {"id": mid, "reactions": reactions, "by": "ai"})
        add_event("typing", {"active": False})
        return {"id": mid, "reactions": reactions}
    text = body.get("text", "")
    meta = {k:v for k,v in body.items() if k not in ("type","text")}
    m = save_message("out", kind, text, meta)
    add_event("typing", {"active": False})
    add_event("message", app_payload(m))
    return {"id": m["id"]}

@app.post("/app/send")
async def app_send(request: Request):
    check_auth(request)
    body = await request.json()
    text = (body.get("text") or "").strip()
    attachments = body.get("attachments") if isinstance(body.get("attachments"), list) else []
    session = str(body.get("api_session") or body.get("session_id") or "").strip()
    if not text and not attachments:
        raise HTTPException(400, "empty text")
    meta = {"user":"human", "attachments":attachments}
    if session: meta["api_session"] = session
    m = save_message("in", "user", text, meta)
    add_event("message", app_payload(m))
    add_event("typing", {"active": True})
    return {"id": m["id"]}

@app.post("/app/voice")
async def app_voice(request: Request):
    check_auth(request)
    ctype = request.headers.get("content-type", "")
    if ctype.startswith("application/json"):
        body = await request.json()
        transcript = (body.get("text") or body.get("transcript") or "").strip()
        if not transcript: raise HTTPException(400, "empty transcript")
        if not transcript.startswith("🎤"): transcript = "🎤 " + transcript
        m = save_message("in", "voice", transcript, {
            "user":"human", "voice":True,
            "source":body.get("source") or "browser_speech"
        })
        add_event("message", app_payload(m))
        add_event("typing", {"active": True})
        return {"id":m["id"], "text":transcript}
    raise HTTPException(501, "binary voice upload is disabled in the first Vercel backend build; use browser speech transcript first")

@app.post("/app/tts")
async def app_tts(request: Request):
    check_auth(request)
    body = await request.json()
    audio = minimax_tts_mp3(body.get("text") or "")
    return Response(audio, media_type="audio/mpeg", headers={"Cache-Control":"no-store"})

@app.post("/app/call")
async def app_call(request: Request):
    check_auth(request)
    body = await request.json()
    action = (body.get("action") or "").strip().lower()
    call_id = (body.get("call_id") or "").strip()
    if action not in {"start","end"}: raise HTTPException(400, "invalid call action")
    text = (
        f"📞 [call_start] {HUMAN_NAME}开启了语音通话。接下来带 🎤 的消息来自语音。请用适合朗读的短句回复。"
        if action == "start" else f"📞 [call_end] {HUMAN_NAME}结束了语音通话。"
    )
    m = save_message("in", "call", text, {"user":"human","call":action,"call_id":call_id})
    add_event("message", app_payload(m))
    if action == "start": add_event("typing", {"active": True})
    return {"id":m["id"]}

@app.post("/app/ping")
async def app_ping(request: Request):
    check_auth(request)
    ensure_db()
    add_event("presence", {"last_seen": now_iso()})
    return {"ok":True}

@app.get("/app/status")
async def app_status(request: Request):
    check_auth(request)
    last = latest_message()
    return {
        "now": now_iso(), "last_seen": None,
        "seen_age_sec": None, "online": False, "state": "unknown",
        "last_msg_ts": last["ts"] if last else None,
        "last_msg_dir": last["direction"] if last else None,
        "last_msg_age_sec": None
    }

@app.get("/app/history")
async def app_history(request: Request, since:int=0, limit:int=200, session_id:str=""):
    check_auth(request)
    rows = get_messages(since, limit, session_id=session_id or None)
    return {"messages":[app_payload(m) for m in rows]}

@app.get("/app/stream")
async def app_stream(request: Request, since:int=0):
    check_auth(request)
    return StreamingResponse(
        poll_sse(request, direction=None, since=since),
        media_type="text/event-stream",
        headers={"Cache-Control":"no-cache, no-transform", "X-Accel-Buffering":"no"}
    )

@app.get("/app/vapid_public")
async def app_vapid_public(request: Request):
    check_auth(request)
    return {"key": os.environ.get("VAPID_PUBLIC_KEY","")}

@app.post("/app/subscribe")
async def app_subscribe(request: Request):
    check_auth(request)
    body = await request.json()
    endpoint = (body.get("endpoint") or "").strip()
    keys = body.get("keys") or {}
    if not endpoint or not keys.get("p256dh") or not keys.get("auth"):
        raise HTTPException(400, "endpoint + keys required")
    ensure_db()
    with conn() as c:
        c.execute("""
        INSERT INTO push_subscriptions(endpoint,p256dh,auth,ua,created)
        VALUES(%s,%s,%s,%s,%s)
        ON CONFLICT(endpoint) DO UPDATE SET p256dh=EXCLUDED.p256dh,auth=EXCLUDED.auth,ua=EXCLUDED.ua
        """, (endpoint, keys["p256dh"], keys["auth"], request.headers.get("user-agent","")[:200], now_iso()))
        c.commit()
    return {"ok":True}

@app.post("/app/unsubscribe")
async def app_unsubscribe(request: Request):
    check_auth(request)
    body = await request.json()
    endpoint = (body.get("endpoint") or "").strip()
    ensure_db()
    with conn() as c:
        c.execute("DELETE FROM push_subscriptions WHERE endpoint=%s", (endpoint,))
        c.commit()
    return {"ok":True}

# Compatibility endpoints. The original local Claude Code loop cannot live inside
# Vercel without another always-on process, so these are deliberately explicit.
@app.get("/app/brain")
async def get_brain(request: Request):
    check_auth(request)
    return {"target": os.environ.get("RELAY_BRAIN_TARGET","desktop")}

@app.post("/app/brain")
async def set_brain(request: Request):
    check_auth(request)
    body = await request.json()
    target = str(body.get("target") or "").strip()
    if target not in ("desktop","loop"): raise HTTPException(400, "target must be desktop or loop")
    return {"target":target, "note":"persist RELAY_BRAIN_TARGET in Vercel Environment Variables to change it"}

@app.get("/app/sessions")
async def app_sessions(request: Request):
    check_auth(request)
    return {"sessions":[],"note":"server-side loop sessions require a separate AI service"}

@app.post("/app/sessions")
async def app_sessions_create(request: Request):
    check_auth(request)
    return {"ok":False,"detail":"server-side loop sessions are not enabled in this Vercel build"}

@app.get("/app/loop_config")
async def loop_config(request: Request):
    check_auth(request)
    return {"enabled":False}

@app.post("/app/loop_config")
async def loop_config_post(request: Request):
    check_auth(request)
    return {"enabled":False}

# The original /uploads route depended on a writable local filesystem.
# Vercel Blob should be wired directly from the browser in the next media-storage step.
@app.get("/uploads/{name}")
async def uploads_disabled(request: Request, name:str):
    check_auth(request)
    raise HTTPException(410, "local uploads are disabled on Vercel; use Vercel Blob URLs")

# Initialize lazily so importing the module itself never performs a network call.
