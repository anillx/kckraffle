#!/usr/bin/env python3
"""Kick raffle server: serves the page and relays Kick chat over a websocket.

Local:  pip install -r requirements.txt && python server.py   ->  http://localhost:8080
Cloud:  start command "python server.py" (reads the PORT environment variable)
"""
import asyncio
import json
import os
import pathlib
import urllib.request

import aiohttp
from aiohttp import web

# Kick's public Pusher endpoint (hardcoded in Kick's web client; may change).
PUSHER_URL = os.environ.get("PUSHER_URL") or (
    "wss://ws-us2.pusher.com/app/32cbd69e4b950bf97679"
    "?protocol=7&client=js&version=8.4.0-rc2&flash=false"
)
CHAT_EVENTS = {"App\\Events\\ChatMessageEvent", "App\\Events\\ChatMessageSentEvent"}
HERE = pathlib.Path(__file__).parent
rooms = {}  # chatroom id -> {"clients": set, "task": Task, "state": dict}


def fetch_json(url):
    try:
        from curl_cffi import requests as cr  # optional: browser-like TLS fingerprint
        return cr.get(url, impersonate="chrome", timeout=10).json()
    except ImportError:
        pass
    req = urllib.request.Request(
        url, headers={"User-Agent": "Mozilla/5.0", "Accept": "application/json"}
    )
    with urllib.request.urlopen(req, timeout=10) as r:
        return json.load(r)


def resolve(target):
    """Chatroom id from a number, a kick.com link or a channel name."""
    target = target.strip().lstrip("@")
    if "kick.com/" in target:
        target = target.rstrip("/").split("/")[-1]
    if target.isdigit():
        return int(target)
    slug = target.lower().replace("_", "-")
    try:
        return int(fetch_json(f"https://kick.com/api/v2/channels/{slug}")["chatroom"]["id"])
    except Exception as e:
        raise RuntimeError(
            f"Could not look up '{slug}' ({e}). Use the chatroom ID number instead."
        )


async def send_safe(c, text):
    """Send to one client; a slow or dead client is dropped instead of blocking everyone."""
    try:
        await asyncio.wait_for(c.send_str(text), 5)
    except Exception:
        try:
            await c.close()
        except Exception:
            pass


async def broadcast(room, obj):
    r = rooms.get(room)
    if not r:
        return
    if obj.get("type") == "status":
        r["state"] = obj
    text = json.dumps(obj)
    await asyncio.gather(*(send_safe(c, text) for c in list(r["clients"])))


async def read_chat(room):
    delay = 1
    async with aiohttp.ClientSession() as session:
        while True:
            try:
                async with session.ws_connect(PUSHER_URL) as ws:
                    first = await ws.receive_json()
                    if first.get("event") != "pusher:connection_established":
                        raise RuntimeError(f"unexpected first message: {first.get('event')}")
                    await ws.send_json({
                        "event": "pusher:subscribe",
                        "data": {"channel": f"chatrooms.{room}.v2"},
                    })
                    await broadcast(room, {"type": "status", "state": "connected",
                                           "text": f"Connected to chatroom {room}"})
                    delay = 1
                    async for msg in ws:
                        if msg.type != aiohttp.WSMsgType.TEXT:
                            continue
                        env = json.loads(msg.data)
                        event = env.get("event", "")
                        if event == "pusher:ping":
                            await ws.send_json({"event": "pusher:pong", "data": {}})
                        elif event in CHAT_EVENTS:
                            data = env.get("data", {})
                            if isinstance(data, str):
                                data = json.loads(data)
                            user = (data.get("sender") or {}).get("username", "")
                            msg_text = str(data.get("content", ""))
                            print(f"[{room}] {user}: {msg_text}", flush=True)
                            await broadcast(room, {"type": "chat", "user": user, "msg": msg_text})
                    raise ConnectionError("connection closed")
            except asyncio.CancelledError:
                raise
            except Exception as e:
                await broadcast(room, {"type": "status", "state": "connecting",
                                       "text": f"Reconnecting to Kick ({e})"})
                await asyncio.sleep(delay)
                delay = min(delay * 2, 30)


async def join(ws, room):
    r = rooms.setdefault(room, {"clients": set(), "task": None, "state": None})
    r["clients"].add(ws)
    if r["state"]:
        await ws.send_json(r["state"])
    if not r["task"] or r["task"].done():
        r["task"] = asyncio.create_task(read_chat(room))


def leave(ws, room):
    r = rooms.get(room)
    if not r:
        return
    r["clients"].discard(ws)
    if not r["clients"]:
        if r["task"]:
            r["task"].cancel()
        rooms.pop(room, None)


async def ws_handler(request):
    ws = web.WebSocketResponse(heartbeat=30)
    await ws.prepare(request)
    room = None
    try:
        async for msg in ws:
            if msg.type != aiohttp.WSMsgType.TEXT:
                continue
            try:
                m = json.loads(msg.data)
            except ValueError:
                continue
            kind = m.get("type")
            if kind == "connect":
                if room is not None:
                    leave(ws, room)
                    room = None
                target = str(m.get("channel", "")).strip()
                if not target:
                    continue
                await ws.send_json({"type": "status", "state": "connecting",
                                    "text": f"Looking up {target}..."})
                try:
                    room = await asyncio.get_running_loop().run_in_executor(None, resolve, target)
                except Exception as e:
                    await ws.send_json({"type": "status", "state": "error", "text": str(e)})
                    continue
                await join(ws, room)
            elif kind == "disconnect":
                if room is not None:
                    leave(ws, room)
                    room = None
                await ws.send_json({"type": "status", "state": "disconnected", "text": ""})
            elif kind == "ping":
                await ws.send_json({"type": "pong"})
    finally:
        if room is not None:
            leave(ws, room)
    return ws


async def index(request):
    for name in ("index.html", "cekilis.html"):
        f = HERE / name
        if f.exists():
            return web.FileResponse(f)
    return web.Response(status=404, text="index.html not found next to server.py")


app = web.Application()
app.add_routes([web.get("/", index), web.get("/ws", ws_handler)])

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    print(f"Open http://localhost:{port}", flush=True)
    web.run_app(app, host="0.0.0.0", port=port, print=None)
