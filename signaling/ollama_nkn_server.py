#!/usr/bin/env python3
"""
client_signaling_server.py — NKN hub + Ollama bridge
(Session-backed chat context, ordered streaming with ACKs, control channel)

Fixes / Guarantees in this build:
- No Popen monkey-patch recursion. We record child start_time on spawn.
- Bridge watchdog: restart on crash, no-ready timeout, or stalled heartbeats.
- Streaming reliability: run final resend sweeps BEFORE llm.done is sent.
"""

# ──────────────────────────────────────────────────────────────────────
# I. bootstrap venv & deps (std-lib only)
# ──────────────────────────────────────────────────────────────────────
import os, sys, subprocess
from pathlib import Path
import importlib.util as _ilu

BASE_DIR = Path(__file__).resolve().parent
VENV_DIR = BASE_DIR / "venv"
BIN_DIR  = VENV_DIR / ("Scripts" if os.name == "nt" else "bin")
PY_VENV  = BIN_DIR / "python"

def _in_venv() -> bool:
    try:
        return Path(sys.executable).resolve() == PY_VENV.resolve()
    except Exception:
        return False

def _module_missing(mod: str) -> bool:
    return _ilu.find_spec(mod) is None

def _create_venv():
    if VENV_DIR.exists(): return
    import venv; venv.EnvBuilder(with_pip=True).create(VENV_DIR)
    subprocess.check_call([str(PY_VENV), "-m", "pip", "install", "--upgrade", "pip"])

def _ensure_deps():
    missing = []
    checks = [("flask", "flask"),
              ("flask-cors", "flask_cors"),
              ("eventlet", "eventlet"),
              ("PyJWT", "jwt"),
              ("ollama","ollama")]
    for pkg, mod in checks:
        if _module_missing(mod): missing.append(pkg)
    if missing:
        subprocess.check_call([str(PY_VENV), "-m", "pip", "install", *missing])
        os.execv(str(PY_VENV), [str(PY_VENV), *sys.argv])

if not _in_venv():
    _create_venv()
    os.execv(str(PY_VENV), [str(PY_VENV), *sys.argv])

_ensure_deps()

# ──────────────────────────────────────────────────────────────────────
# II. heavy imports & eventlet patch
# ──────────────────────────────────────────────────────────────────────
import eventlet; eventlet.monkey_patch()

import json, secrets, signal, threading, base64, time, re, shutil
from datetime import datetime, timedelta, timezone
from subprocess import Popen, PIPE
from typing import Dict, Any, List, Optional

import jwt
from flask import Flask, jsonify
from flask_cors import CORS

# Ollama
from ollama import Client as OllamaClient
from ollama import ResponseError as OllamaResponseError

# ──────────────────────────────────────────────────────────────────────
# III. .env and config
# ──────────────────────────────────────────────────────────────────────
ENV_PATH = BASE_DIR / ".env"
if not ENV_PATH.exists():
    pw        = secrets.token_urlsafe(16)
    jwt_sec   = secrets.token_urlsafe(32)
    seed_hex  = secrets.token_hex(32)  # 64 hex chars
    topic_ns  = "client"
    ollama_host = "http://127.0.0.1:11434"
    ENV_PATH.write_text(
        f"PEER_SHARED_SECRET={pw}\n"
        f"JWT_SECRET={jwt_sec}\n"
        f"NKN_SEED_HEX={seed_hex}\n"
        f"NKN_TOPIC_PREFIX={topic_ns}\n"
        f"OLLAMA_HOST={ollama_host}\n"
    )
    print("→ wrote .env (defaults created)")

dotenv: Dict[str,str] = {}
for line in ENV_PATH.read_text().splitlines():
    if "=" in line and not line.lstrip().startswith("#"):
        k,v = line.split("=",1); dotenv[k.strip()] = v.strip()

PEER_PW       = dotenv.get("PEER_SHARED_SECRET","")
JWT_SECRET    = dotenv["JWT_SECRET"]
NKN_SEED_HEX  = dotenv["NKN_SEED_HEX"].lower().replace("0x","")
TOPIC_PREFIX  = dotenv.get("NKN_TOPIC_PREFIX","client")
JWT_EXP       = timedelta(minutes=30)
OLLAMA_HOST   = dotenv.get("OLLAMA_HOST","http://127.0.0.1:11434")

# Session policy
SESSION_TTL_SECS   = 60 * 45      # prune sessions idle > 45m
SESSION_MAX_TURNS  = 20           # keep last 20 user/assistant turns (40 msgs) + system
SESSION_LOCK = threading.RLock()

# Watchdog config (bridge)
READY_GRACE_SECS   = 20.0
HB_INTERVAL_SECS   = 5.0
HB_STALL_SECS      = 90.0  # relaxed to tolerate idle/background timer clamping

# ──────────────────────────────────────────────────────────────────────
# IV. Node NKN bridge (DM-first; hardened sends) + watchdog
# ──────────────────────────────────────────────────────────────────────
NODE_DIR = BASE_DIR / "bridge-node"
NODE_BIN = shutil.which("node")
NPM_BIN  = shutil.which("npm")
if not NODE_BIN or not NPM_BIN:
    sys.exit("‼️  Node.js and npm are required for the NKN bridge. Install Node 18+.")

BRIDGE_JS = NODE_DIR / "nkn_bridge.js"
PKG_JSON  = NODE_DIR / "package.json"

if not NODE_DIR.exists():
    NODE_DIR.mkdir(parents=True)
if not PKG_JSON.exists():
    print("→ Initializing bridge-node …")
    subprocess.check_call([NPM_BIN, "init", "-y"], cwd=NODE_DIR)
    subprocess.check_call([NPM_BIN, "install", "nkn-sdk@^1.3.6"], cwd=NODE_DIR)

BRIDGE_SRC = r"""
/* nkn_bridge.js — DM-only bridge for client hub (timeout-hardened) */
const nkn = require('nkn-sdk');
const readline = require('readline');

const SEED_HEX = (process.env.NKN_SEED_HEX || '').toLowerCase().replace(/^0x/,'');
const TOPIC_NS = process.env.NKN_TOPIC_PREFIX || 'client';

function sendToPy(obj){ process.stdout.write(JSON.stringify(obj) + '\n'); }
function log(...args){ console.error('[bridge]', ...args); }

let READY = false;
let queue = []; // {to, data, isPub, topic}
const SEND_OPTS = { noReply: true, maxHoldingSeconds: 120 };

// Suspend window to pause drain on transient WS closures
let suspendUntil = 0;
function suspend(ms=2000){ suspendUntil = Date.now() + ms; READY = false; }

function rateLimit(fn, ms){
  let last=0; return (...args)=>{ const t=Date.now(); if(t-last>ms){ last=t; fn(...args); } };
}
const logSendErr = rateLimit((msg)=>log('send warn', msg), 1500);

(async () => {
  if (!/^[0-9a-f]{64}$/.test(SEED_HEX)) throw new RangeError('invalid hex seed');
  const client = new nkn.MultiClient({
    seed: SEED_HEX,
    identifier: 'hub',
    numSubClients: 4,
    // tolerate background-timer clamping & flaky NATs
    wsConnHeartbeatTimeout: 60000,
    reconnectIntervalMin: 1000,
    reconnectIntervalMax: 10000,
  });

  client.on('connect', () => {
    READY = true;
    sendToPy({ type: 'ready', address: client.addr, topicPrefix: TOPIC_NS, ts: Date.now() });
    log('ready at', client.addr);
  });

  client.on('error', e => {
    const msg = (e && e.message) || String(e || '');
    log('client error', msg);
    // Transient WS close: let MultiClient reconnect; pause draining
    if (msg.includes('WebSocket unexpectedly closed')) {
      try { sendToPy({ type: 'wsclosed', reason: msg, ts: Date.now() }); } catch {}
      suspend(2500);
    }
  });
  client.on('connectFailed', e => log('connect failed', e && e.message || e));

  const onMessage = (a, b) => {
    let src, payload;
    if (a && typeof a === 'object' && a.payload !== undefined) { src = a.src; payload = a.payload; }
    else { src = a; payload = b; }

    try {
      const asStr = Buffer.isBuffer(payload) ? payload.toString('utf8') : String(payload || '');
      try { const msg = JSON.parse(asStr); sendToPy({ type: 'nkn-dm', src, msg }); } catch {}
    } catch (e) { /* ignore */ }
  };
  client.on('message', onMessage);

  async function safeSendDM(to, data){
    try {
      await client.send(to, JSON.stringify(data), SEND_OPTS);
      return true;
    } catch (e) {
      const msg = (e && e.message) || String(e || '');
      logSendErr(`${to} → ${msg}`);
      if (msg.includes('WebSocket unexpectedly closed')) {
        suspend(2500);
        try { sendToPy({ type: 'wsclosed', reason: msg, ts: Date.now() }); } catch {}
      }
      return false;
    }
  }
  async function safePub(topic, data){
    try {
      await client.publish(TOPIC_NS + '.' + topic, JSON.stringify(data));
      return true;
    } catch (e) {
      const msg = (e && e.message) || String(e || '');
      logSendErr(`pub ${topic} → ${msg}`);
      if (msg.includes('WebSocket unexpectedly closed')) {
        suspend(2500);
        try { sendToPy({ type: 'wsclosed', reason: msg, ts: Date.now() }); } catch {}
      }
      return false;
    }
  }

  async function drain(){
    if (!READY || Date.now() < suspendUntil || queue.length===0) return;
    const batch = queue.splice(0, Math.min(queue.length, 64));
    for(const it of batch){
      const ok = it.isPub ? await safePub(it.topic, it.data)
                          : await safeSendDM(it.to, it.data);
      // Re-queue on failure so the "last message" is retried post-reconnect
      if (!ok) queue.unshift(it);
    }
  }
  setInterval(drain, 300);

  // Heartbeat to Python watchdog
  setInterval(() => {
    try { sendToPy({ type: 'hb', ts: Date.now(), ready: READY }); } catch {}
  }, 5000);

  const rl = readline.createInterface({ input: process.stdin });
  rl.on('line', async line => {
    if (!line) return;
    let cmd; try { cmd = JSON.parse(line); } catch { return; }
    if (cmd.type === 'dm' && cmd.to && cmd.data) {
      if (!READY) queue.push({to: cmd.to, data: cmd.data});
      else {
        const ok = await safeSendDM(cmd.to, cmd.data);
        if (!ok) queue.unshift({to: cmd.to, data: cmd.data}); // retry later
      }
    } else if (cmd.type === 'pub' && cmd.topic && cmd.data) {
      if (!READY) queue.push({isPub:true, topic: cmd.topic, data: cmd.data});
      else {
        const ok = await safePub(cmd.topic, cmd.data);
        if (!ok) queue.unshift({isPub:true, topic: cmd.topic, data: cmd.data}); // retry later
      }
    }
  });
})();
"""
if not BRIDGE_JS.exists() or BRIDGE_JS.read_text() != BRIDGE_SRC:
    BRIDGE_JS.write_text(BRIDGE_SRC)

bridge_env = os.environ.copy()
bridge_env["NKN_SEED_HEX"]     = NKN_SEED_HEX
bridge_env["NKN_TOPIC_PREFIX"] = TOPIC_PREFIX

# Bridge process + watchdog state
bridge = None
state: Dict[str, Any] = {"nkn_address": None, "topic_prefix": TOPIC_PREFIX}
_last_hb = 0.0
_ready_at = 0.0
_bridge_starts = 0
_bridge_lock = threading.Lock()

def _spawn_bridge() -> Popen:
    global _last_hb, _ready_at, _bridge_starts
    _last_hb = time.time()
    _ready_at = 0.0
    _bridge_starts += 1
    proc = Popen([str(shutil.which("node")), str(BRIDGE_JS)],
                 cwd=NODE_DIR, env=bridge_env,
                 stdin=PIPE, stdout=PIPE, stderr=PIPE,
                 text=True, bufsize=1)
    setattr(proc, "start_time", time.time())
    return proc

def _bridge_send(obj: dict):
    try:
        if bridge and bridge.stdin:
            bridge.stdin.write(json.dumps(obj) + "\n")
            bridge.stdin.flush()
    except Exception:
        pass

def _dm(to: str, data: dict):
    _bridge_send({"type": "dm", "to": to, "data": data})

def _bridge_reader_stdout():
    global _last_hb, _ready_at
    while True:
        try:
            if not bridge or not bridge.stdout:
                time.sleep(0.2); continue
            line = bridge.stdout.readline()
            if line == '' and bridge.poll() is not None:
                time.sleep(0.2); continue
            line = (line or "").strip()
            if not line: continue
            try:
                msg = json.loads(line)
            except Exception:
                continue

            if msg.get("type") == "ready":
                state["nkn_address"]  = msg.get("address")
                state["topic_prefix"] = msg.get("topicPrefix") or TOPIC_PREFIX
                _ready_at = time.time()
                print(f"→ NKN ready: {state['nkn_address']}  (topics prefix: {state['topic_prefix']})")

            elif msg.get("type") == "hb":
                _last_hb = time.time()

            elif msg.get("type") == "wsclosed":
                # Transient: bridge will auto-reconnect; keep process alive and pause sends.
                print("⚠️ bridge reported wsclosed — pausing sends until reconnect …")

            elif msg.get("type") == "nkn-dm":
                src = msg.get("src") or ""
                body = msg.get("msg") or {}
                _handle_dm(src, body)
        except Exception:
            time.sleep(0.2)

def _bridge_reader_stderr():
    while True:
        try:
            if bridge and bridge.stderr:
                line = bridge.stderr.readline()
                if line: sys.stderr.write(line)
                else:
                    time.sleep(0.1)
            else:
                time.sleep(0.2)
        except Exception:
            time.sleep(0.2)

def _restart_bridge_locked():
    global bridge
    try:
        if bridge:
            try: bridge.terminate()
            except Exception: pass
    finally:
        bridge = _spawn_bridge()

def _watchdog_loop():
    global bridge, _last_hb, _ready_at
    while True:
        try:
            with _bridge_lock:
                need_restart = False
                now = time.time()

                # Crash or dead pipe
                if not bridge or bridge.poll() is not None:
                    need_restart = True

                # Not ready within grace
                elif getattr(bridge, "start_time", 0) and _ready_at == 0.0:
                    if now - getattr(bridge, "start_time", 0) > READY_GRACE_SECS:
                        need_restart = True

                # Stalled heartbeat (after we've seen 'ready')
                elif _ready_at > 0.0 and (now - _last_hb) > HB_STALL_SECS:
                    need_restart = True

                if need_restart:
                    print("↻ restarting NKN bridge …")
                    _restart_bridge_locked()
            time.sleep(1.0)
        except Exception:
            time.sleep(1.0)

# Initial spawn and threads
with _bridge_lock:
    bridge = _spawn_bridge()
threading.Thread(target=_bridge_reader_stdout, daemon=True).start()
threading.Thread(target=_bridge_reader_stderr, daemon=True).start()
threading.Thread(target=_watchdog_loop, daemon=True).start()

def _shutdown(*_):
    try: bridge.terminate()
    except Exception: pass
    os._exit(0)

signal.signal(signal.SIGINT, _shutdown)
signal.signal(signal.SIGTERM, _shutdown)

# ──────────────────────────────────────────────────────────────────────
# V. JSON helpers & Ollama adapters
# ──────────────────────────────────────────────────────────────────────
def _now_ms() -> int:
    return int(time.time() * 1000)

def _stamp() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")

def _log(evt: str, msg: str):
    print(f"[{_stamp()}] {evt}  {msg}")

def _to_jsonable(x):
    try:
        json.dumps(x)
        return x
    except TypeError:
        pass
    if hasattr(x, "model_dump") and callable(getattr(x, "model_dump")):
        return _to_jsonable(x.model_dump())
    if hasattr(x, "dict") and callable(getattr(x, "dict")):
        return _to_jsonable(x.dict())
    if hasattr(x, "__dict__"):
        d = {k: v for k, v in x.__dict__.items() if not k.startswith("_")}
        return _to_jsonable(d)
    if isinstance(x, (list, tuple)):
        return [_to_jsonable(i) for i in x]
    if isinstance(x, dict):
        return {k: _to_jsonable(v) for k, v in x.items()}
    return repr(x)

def _normalize_ollama_list(resp) -> dict:
    r = _to_jsonable(resp)
    if isinstance(r, dict) and "models" in r:
        return {"models": [_to_jsonable(m) for m in r.get("models", [])]}
    if hasattr(resp, "models"):
        return {"models": [_to_jsonable(m) for m in getattr(resp, "models")]}
    if isinstance(r, list):
        return {"models": [_to_jsonable(m) for m in r]}
    return {"models": []}

def _ollama_list() -> dict:
    oc = OllamaClient(host=OLLAMA_HOST)
    return _normalize_ollama_list(oc.list())

def _print_models_on_start():
    print(f"→ Probing Ollama at {OLLAMA_HOST} …")
    try:
        data = _ollama_list()
        names = []
        for m in data.get("models", []):
            if isinstance(m, dict):
                name = m.get("model") or m.get("name") or m.get("digest") or ""
            else:
                name = getattr(m, "model", None) or getattr(m, "name", None) or str(m)
            if name: names.append(name)
        if names:
            print(f"   Models ({len(names)}): " + ", ".join(names))
        else:
            print("   No models found. Pull one with:  ollama pull <model>")
    except Exception as e:
        print(f"‼️  Ollama probe failed: {e}")

def _send_models_list(to: str):
    try:
        data = _ollama_list()
        pkt = {"event": "llm.models", "data": data, "ts": _now_ms()}
        _dm(to, pkt)
        _dm(to, {"event":"llm.result","id":"models","data":data,"ts":_now_ms()})
        _log("models.sent", f"→ {to} count={len(data.get('models',[]))}")
    except Exception as e:
        _dm(to, {"event":"llm.error","id":"models","message":f"models list failed: {e}","kind":"models","ts":_now_ms()})

# ──────────────────────────────────────────────────────────────────────
# VI. Session store
# ──────────────────────────────────────────────────────────────────────
class Session:
    __slots__ = ("sid","src","model","system","default_options","messages","last_used")
    def __init__(self, sid:str, src:str, model:str=None, system:str=None, default_options:dict=None):
        self.sid = sid
        self.src = src
        self.model = model
        self.system = system or ""
        self.default_options = default_options or {}
        self.messages: List[Dict[str,Any]] = []  # [{role, content, ts}]
        self.last_used = time.time()

    def touch(self):
        self.last_used = time.time()

SESSIONS: Dict[tuple, Session] = {}  # (src_addr, sid) -> Session

def _sess_key(src_addr: str, sid: str): return (src_addr, sid or "")

_VALID_ROLES = {"system", "user", "assistant"}
def _sanitize_msg(m: dict) -> Dict[str, str]:
    role = str(m.get("role","")).strip().lower()
    if role not in _VALID_ROLES:
        role = "user"
    content = m.get("content")
    if not isinstance(content, str):
        content = "" if content is None else str(content)
    return {"role": role, "content": content}

def _sanitize_history(history) -> List[Dict[str, str]]:
    out: List[Dict[str, str]] = []
    if not isinstance(history, (list, tuple)): return out
    for m in history:
        if isinstance(m, dict) and "content" in m:
            out.append(_sanitize_msg(m))
    return out

def _open_session(src_addr:str, sid:str, model:str=None, system:str=None, options:dict=None,
                  history: Optional[List[Dict[str,str]]]=None, replace: bool=False) -> Session:
    if not isinstance(sid, str) or not sid:
        raise ValueError("invalid sid")
    with SESSION_LOCK:
        key = _sess_key(src_addr, sid)
        s = SESSIONS.get(key)
        if s is None:
            s = Session(sid, src_addr, model=model, system=system, default_options=(options or {}))
            SESSIONS[key] = s
        else:
            if model: s.model = model
            if system is not None: s.system = system
            if options: s.default_options.update(options)

        if history is not None:
            hist = _sanitize_history(history)

            if (system is None) and not s.system:
                for m in hist:
                    if m["role"] == "system":
                        s.system = m["content"]
                        break

            turns = [{"role": m["role"], "content": m["content"], "ts": _now_ms()}
                     for m in hist if m["role"] in ("user","assistant")]

            if replace:
                s.messages = turns
            else:
                s.messages.extend(turns)

            _prune_session(s)

        s.touch()
        return s

def _get_session(src_addr:str, sid:str) -> Optional[Session]:
    with SESSION_LOCK:
        s = SESSIONS.get(_sess_key(src_addr, sid))
        if s: s.touch()
        return s

def _reset_session(src_addr:str, sid:str):
    with SESSION_LOCK:
        s = SESSIONS.get(_sess_key(src_addr, sid))
        if s:
            s.messages.clear()
            s.touch()

def _append_user(src_addr:str, sid:str, content:str):
    s = _get_session(src_addr, sid)
    if not s: raise KeyError("session not found")
    s.messages.append({"role":"user","content":str(content), "ts":_now_ms()})
    _prune_session(s)

def _start_assistant(src_addr:str, sid:str):
    s = _get_session(src_addr, sid)
    if not s: raise KeyError("session not found")
    s.messages.append({"role":"assistant","content":"", "ts":_now_ms()})
    _prune_session(s)

def _append_assistant_delta(src_addr:str, sid:str, delta:str):
    s = _get_session(src_addr, sid)
    if not s or not s.messages: return
    if s.messages[-1]["role"] != "assistant":
        _start_assistant(src_addr, sid)
    s.messages[-1]["content"] += str(delta)
    s.messages[-1]["ts"] = _now_ms()

def _finish_assistant(src_addr:str, sid:str):
    s = _get_session(src_addr, sid)
    if not s: return
    s.touch()

def _prune_session(s: Session):
    max_msgs = max(2*SESSION_MAX_TURNS, 2)
    if len(s.messages) > max_msgs:
        s.messages = s.messages[-max_msgs:]

def _build_chat_messages(s: Session, new_user:str=None, override_messages:Optional[List[Dict[str,str]]]=None):
    if override_messages is not None:
        return override_messages
    msgs: List[Dict[str,str]] = []
    if s.system:
        msgs.append({"role":"system","content":s.system})
    msgs.extend({"role":m["role"], "content":m["content"]} for m in s.messages)
    if new_user is not None:
        msgs.append({"role":"user","content":new_user})
    return msgs

def _merge_options(s: Session, req_options: Optional[dict]) -> dict:
    out = {}
    out.update(s.default_options or {})
    if isinstance(req_options, dict):
        out.update(req_options)
    return out

def _sessions_gc_loop():
    while True:
        time.sleep(60)
        cutoff = time.time() - SESSION_TTL_SECS
        with SESSION_LOCK:
            dead = [k for k,v in SESSIONS.items() if v.last_used < cutoff]
            for k in dead:
                SESSIONS.pop(k, None)

threading.Thread(target=_sessions_gc_loop, daemon=True).start()

# ──────────────────────────────────────────────────────────────────────
# VII. Legacy globe (kept minimal)
# ──────────────────────────────────────────────────────────────────────
clients = set()  # active DM src addrs
peers_by_addr: Dict[str, Dict[str, Any]] = {}  # addr -> {lat, lon, ts(ms)}

def _sanitize_state(s: dict) -> Dict[str, float]:
    try:
        lat = float(s.get("lat", 0.0))
        lon = float(s.get("lon", 0.0))
    except Exception:
        lat, lon = 0.0, 0.0
    if lat > 89.9: lat = 89.9
    if lat < -89.9: lat = -89.9
    lon = ((lon % 360.0) + 360.0) % 360.0
    return {"lat": lat, "lon": lon}

ACTIVE_WINDOW_MS = 6_000
PRUNE_WINDOW_MS  = 10_000

def _send_world_snapshot(targets=None):
    now = _now_ms()
    for addr, st in list(peers_by_addr.items()):
        if st.get("ts", 0) < (now - PRUNE_WINDOW_MS):
            _log("prune", f"{addr} idle >10s")
            peers_by_addr.pop(addr, None)
            clients.discard(addr)
    if targets is None:
        targets = [a for a in clients if peers_by_addr.get(a, {}).get("ts", 0) >= (now - ACTIVE_WINDOW_MS)]
    if not targets: return
    peers_obj = { addr: {"lat": st["lat"], "lon": st["lon"], "ts": st["ts"]} for addr, st in peers_by_addr.items() }
    pkt = {"event":"world", "peers": peers_obj, "ts": now}
    for to in targets:
        _dm(to, pkt)

def _broadcaster_loop():
    while True:
        try:
            _send_world_snapshot()
        except Exception as e:
            _log("broadcast-error", repr(e))
        time.sleep(0.20)

threading.Thread(target=_broadcaster_loop, daemon=True).start()

# ──────────────────────────────────────────────────────────────────────
# VIII. LLM streaming with session integration
# ──────────────────────────────────────────────────────────────────────
def _extract_delta_from_part(api: str, part: Any) -> str:
    try:
        if api == "chat":
            msg = part.get("message") if isinstance(part, dict) else None
            if msg and isinstance(msg, dict):
                c = msg.get("content")
                if isinstance(c, str): return c
        resp = part.get("response") if isinstance(part, dict) else None
        if isinstance(resp, str): return resp
    except Exception:
        pass
    return ""

def _extract_full_text(api: str, data: Any) -> str:
    try:
        if api == "chat":
            if isinstance(data, dict):
                m = data.get("message")
                if isinstance(m, dict) and isinstance(m.get("content"), str):
                    return m["content"]
        if isinstance(data, dict) and isinstance(data.get("response"), str):
            return data["response"]
    except Exception:
        pass
    try:
        return json.dumps(_to_jsonable(data))
    except Exception:
        return str(data)

class _LLMStream:
    RESEND_INTERVAL = 0.6
    WINDOW_LIMIT    = 512
    FINAL_SWEEPS    = 3
    FINAL_GAP_SECS  = 0.12

    def __init__(self, src_addr: str, req: dict,
                 on_start=None, on_delta=None, on_done=None):
        self.src_addr = src_addr
        self.id       = req.get("id") or secrets.token_hex(8)
        self.api      = (req.get("api") or "chat").strip().lower()
        self.model    = req.get("model")
        self.stream   = bool(req.get("stream", False))
        self.client_cfg = req.get("client") or {}
        self.kwargs   = req.get("kwargs") or {}

        self.seq      = 0
        self.last_acked = 0
        self.window   = {}
        self.window_order = []
        self.done_sent  = False
        self.done_seq   = 0
        self.cancelled  = False
        self._lock    = threading.Lock()
        self._resender = threading.Thread(target=self._resend_loop, daemon=True)

        self.on_start = on_start
        self.on_delta = on_delta
        self.on_done  = on_done

        host = self.client_cfg.get("host") or OLLAMA_HOST
        headers = self.client_cfg.get("headers") or {}
        try:
            self.client = OllamaClient(host=host, headers=headers)
        except Exception as e:
            self.client = None
            self._send_error(f"ollama client init failed: {e}", kind="client_init")

    def _dm(self, payload: dict):
        _dm(self.src_addr, payload)

    def _send_start(self):
        self._dm({"event":"llm.start","id":self.id,"api":self.api,"model":self.model,"stream":self.stream})
        try:
            if callable(self.on_start): self.on_start()
        except Exception as e:
            _log("llm.on_start.err", str(e))

    def _send_error(self, message: str, kind: str = "runtime"):
        self._dm({"event":"llm.error","id":self.id,"message":str(message),"kind":kind,"ts":_now_ms()})

    def _send_result(self, data: dict):
        self._dm({"event":"llm.result","id":self.id,"data":_to_jsonable(data),"ts":_now_ms()})
        try:
            if callable(self.on_done):
                txt = _extract_full_text(self.api, _to_jsonable(data))
                self.on_done(txt)
        except Exception as e:
            _log("llm.on_done.err", str(e))

    def _send_chunk(self, data: dict):
        with self._lock:
            self.seq += 1
            seq = self.seq
            pkt = {"event":"llm.chunk","id":self.id,"seq":seq,"data":_to_jsonable(data),"ts":_now_ms()}
            self.window[seq] = pkt
            self.window_order.append(seq)
            if len(self.window_order) > self.WINDOW_LIMIT:
                oldest = self.window_order.pop(0)
                if oldest <= self.last_acked:
                    self.window.pop(oldest, None)
                else:
                    self.window_order.insert(0, oldest)
            self._dm(pkt)
        try:
            if callable(self.on_delta):
                delta = _extract_delta_from_part(self.api, data)
                if delta: self.on_delta(delta)
        except Exception as e:
            _log("llm.on_delta.err", str(e))

    def _sweep_resend_unacked(self):
        """One quick pass of re-sending any not-yet-acked chunks up to current seq."""
        with self._lock:
            start = self.last_acked + 1
            end   = self.seq
            if end < start:
                return
            for s in range(start, end + 1):
                pkt = self.window.get(s)
                if pkt is not None:
                    self._dm(pkt)

    def _send_done(self, meta: dict):
        with self._lock:
            self.done_sent = True
            self.done_seq = self.seq
            self._dm({"event":"llm.done","id":self.id,"last_seq":self.done_seq,"meta":meta,"ts":_now_ms()})
        try:
            if callable(self.on_done):
                self.on_done(None)
        except Exception as e:
            _log("llm.on_done.err", str(e))

    def ack(self, upto: int):
        with self._lock:
            if upto > self.last_acked:
                self.last_acked = upto
                for s in [s for s in list(self.window.keys()) if s <= self.last_acked]:
                    self.window.pop(s, None)
                self.window_order = [s for s in self.window_order if s > self.last_acked]

    def cancel(self):
        with self._lock:
            self.cancelled = True

    def _resend_loop(self):
        while True:
            if self.cancelled:
                return
            time.sleep(self.RESEND_INTERVAL)
            with self._lock:
                target_hi = self.seq
                start = self.last_acked + 1
                for s in range(start, target_hi + 1):
                    pkt = self.window.get(s)
                    if pkt is not None:
                        self._dm(pkt)
                # Stop only after client has ACKed through last_seq (post-done)
                if self.done_sent and self.last_acked >= self.done_seq:
                    return

    def run(self):
        if self.client is None:
            return
        self._send_start()
        self._resender.start()

        try:
            apis_need_model = {"chat","generate","embed","show","pull","delete","copy","create"}
            if self.api in apis_need_model:
                if not isinstance(self.model, str) or not self.model:
                    self._send_error("missing or invalid 'model'", kind="bad_request")
                    return

            if not self.stream:
                data = self._invoke_once(self.api)
                self._send_result(data)
                # Non-stream: no chunks; still call done after result
                self._send_done({"mode":"non-stream","api":self.api})
                return

            gen, meta = self._invoke_stream(self.api)
            for part in gen:
                if self.cancelled: break
                self._send_chunk(part)

            # >>> Critical reliability change: do quick final sweeps BEFORE done.
            for _ in range(self.FINAL_SWEEPS):
                if self.cancelled: break
                self._sweep_resend_unacked()
                # If client already caught up, we can break early
                with self._lock:
                    if self.last_acked >= self.seq:
                        break
                time.sleep(self.FINAL_GAP_SECS)

            self._send_done(meta or {"mode":"stream","api":self.api})

        except OllamaResponseError as e:
            self._send_error(f"Ollama error: {e.error}", kind=f"http_{getattr(e,'status_code', 'unknown')}")
        except Exception as e:
            self._send_error(f"{type(e).__name__}: {e}", kind="exception")

    def _invoke_once(self, api: str) -> dict:
        if api == "chat":
            return self.client.chat(model=self.model, stream=False, **self.kwargs)
        elif api == "generate":
            return self.client.generate(model=self.model, stream=False, **self.kwargs)
        elif api == "embed":
            return self.client.embed(model=self.model, **self.kwargs)
        elif api == "show":
            return self.client.show(self.model)
        elif api == "list":
            return self.client.list()
        elif api == "ps":
            return self.client.ps()
        elif api == "pull":
            return self.client.pull(self.model)
        elif api == "delete":
            return self.client.delete(self.model)
        elif api == "copy":
            src = self.model
            dst = self.kwargs.get("dst") or self.kwargs.get("to")
            return self.client.copy(src, dst)
        elif api == "create":
            return self.client.create(model=self.model, **self.kwargs)
        else:
            raise ValueError(f"unsupported api '{api}'")

    def _invoke_stream(self, api: str):
        meta = {"mode":"stream","api":api}
        if api == "chat":
            gen = self.client.chat(model=self.model, stream=True, **self.kwargs)
            return gen, meta
        elif api == "generate":
            gen = self.client.generate(model=self.model, stream=True, **self.kwargs)
            return gen, meta
        else:
            raise ValueError(f"streaming not supported for api '{api}'")

# Registry of active LLM streams by id
_llm_streams: Dict[str, _LLMStream] = {}

def _start_llm(src_addr: str, body: dict,
               on_start=None, on_delta=None, on_done=None):
    stream = _LLMStream(src_addr, body, on_start=on_start, on_delta=on_delta, on_done=on_done)
    _llm_streams[stream.id] = stream
    threading.Thread(target=stream.run, daemon=True).start()
    return stream.id

def _ack_llm(body: dict):
    sid = body.get("id")
    upto = body.get("upto")
    if not isinstance(sid, str) or not isinstance(upto, int): return
    stream = _llm_streams.get(sid)
    if stream: stream.ack(upto)

def _cancel_llm(body: dict):
    sid = body.get("id")
    if not isinstance(sid, str): return
    stream = _llm_streams.get(sid)
    if stream: stream.cancel()

# ──────────────────────────────────────────────────────────────────────
# IX. DM handler (control + sessions + LLM + legacy globe)
# ──────────────────────────────────────────────────────────────────────
def _handle_dm(src_addr: str, body: dict):
    ev = (body.get("event") or "").strip()
    if not ev:
        _log("dm", f"from {src_addr}  event=<missing>  body={body}")
        return

    # Control
    if ev == "ctrl.request":
        op = (body.get("op") or "").strip().lower()
        if op == "models":
            _send_models_list(src_addr)
        elif op == "info":
            _dm(src_addr, {"event":"ctrl.info","ts":_now_ms(),
                           "nknAddress": state["nkn_address"], "topicPrefix": state["topic_prefix"],
                           "ollamaHost": OLLAMA_HOST})
        else:
            _dm(src_addr, {"event":"ctrl.error","ts":_now_ms(),"op":op,"message":"unknown op"})
        return

    # Sessions
    if ev == "session.open":
        sid    = body.get("sid")
        model  = body.get("model")
        system = body.get("system")
        options= body.get("options") or {}
        history= body.get("history")
        replace= bool(body.get("replace", False))
        try:
            s = _open_session(src_addr, sid, model=model, system=system, options=options,
                              history=history, replace=replace)
            _dm(src_addr, {"event":"session.ready","sid":sid,"model":s.model,"count":len(s.messages),"ts":_now_ms()})
            _log("session.ready", f"{src_addr} sid={sid} model={s.model} count={len(s.messages)} replace={replace and bool(history)}")
        except Exception as e:
            _dm(src_addr, {"event":"ctrl.error","message":str(e),"ts":_now_ms()})
        return

    if ev == "session.seed":
        sid    = body.get("sid")
        model  = body.get("model")
        system = body.get("system")
        options= body.get("options") or {}
        history= body.get("history")
        try:
            s = _open_session(src_addr, sid, model=model, system=system, options=options,
                              history=history, replace=True)
            _dm(src_addr, {"event":"session.ready","sid":sid,"model":s.model,"count":len(s.messages),"ts":_now_ms()})
            _log("session.seed", f"{src_addr} sid={sid} model={s.model} count={len(s.messages)}")
        except Exception as e:
            _dm(src_addr, {"event":"ctrl.error","message":str(e),"ts":_now_ms()})
        return

    if ev == "session.reset":
        sid = body.get("sid")
        _reset_session(src_addr, sid)
        _dm(src_addr, {"event":"session.ready","sid":sid,"reset":True,"count":0,"ts":_now_ms()})
        _log("session.reset", f"{src_addr} sid={sid}")
        return

    if ev == "session.info":
        sid = body.get("sid")
        s = _get_session(src_addr, sid)
        if not s:
            _dm(src_addr, {"event":"ctrl.error","message":"session not found","ts":_now_ms()})
            return
        _dm(src_addr, {"event":"session.info","sid":sid,
                       "model":s.model,"system":s.system,
                       "messages":len(s.messages),"ts":_now_ms()})
        return

    # LLM
    if ev == "llm.request":
        api    = (body.get("api") or "chat").strip().lower()
        stream = bool(body.get("stream", False))
        sid    = body.get("sid")
        delta  = body.get("delta")
        kwargs = body.get("kwargs") or {}
        model  = (body.get("model") or "").strip() or None

        if api == "list" and not stream:
            _send_models_list(src_addr)
            _log("llm.list", f"{src_addr} → models sent")
            return

        if sid and delta is not None:
            s = _get_session(src_addr, sid)
            if not s:
                _dm(src_addr, {"event":"llm.error","id":body.get("id") or "req",
                               "message":"session not found","kind":"bad_request","ts":_now_ms()})
                return
            if model: s.model = model
            if not s.model:
                _dm(src_addr, {"event":"llm.error","id":body.get("id") or "req",
                               "message":"session has no model; call session.open with model","kind":"bad_request","ts":_now_ms()})
                return
            _append_user(src_addr, sid, delta)
            req_messages = _build_chat_messages(s, new_user=None)
            opts = _merge_options(s, (kwargs.get("options") or {}))
            call_kwargs = dict(kwargs)
            call_kwargs["messages"] = req_messages
            call_kwargs["options"]  = opts

            def on_start():
                if stream:
                    _start_assistant(src_addr, sid)

            def on_delta_fn(text:str):
                if text:
                    _append_assistant_delta(src_addr, sid, text)

            def on_done_fn(final_text: Optional[str]):
                if not stream:
                    _start_assistant(src_addr, sid)
                    if final_text:
                        _append_assistant_delta(src_addr, sid, final_text)
                _finish_assistant(src_addr, sid)

            req_body = {
                "event":"llm.request",
                "api": api,
                "model": s.model,
                "stream": stream,
                "kwargs": call_kwargs,
                "id": body.get("id")
            }
            sid_started = _start_llm(src_addr, req_body,
                                     on_start=on_start,
                                     on_delta=on_delta_fn,
                                     on_done=on_done_fn)
            _log("llm.start", f"{src_addr} sid={sid} id={sid_started} api={api} model={s.model} stream={stream}")
            return

        else:
            rid = _start_llm(src_addr, body)
            _log("llm.start", f"{src_addr} id={rid} api={api} model={model} stream={stream} (compat)")
            return

    if ev == "llm.ack":
        _ack_llm(body)
        return

    if ev == "llm.cancel":
        _cancel_llm(body)
        _log("llm.cancel", f"{src_addr} id={body.get('id')}")
        return

    # Legacy demo globe
    if ev == "ping":
        _dm(src_addr, {"event":"pong","ts":_now_ms()})
        _log("ping", f"{src_addr}")
        return

    if ev == "join":
        st = _sanitize_state(body.get("state") or {})
        peers_by_addr[src_addr] = {"lat": st["lat"], "lon": st["lon"], "ts": _now_ms()}
        clients.add(src_addr)
        _log("join", f"{src_addr}  lat={st['lat']:.2f} lon={st['lon']:.2f}  clients={len(clients)}")
        _send_world_snapshot([src_addr])
        return

    if ev == "state":
        st = _sanitize_state(body.get("state") or {})
        peers_by_addr[src_addr] = {"lat": st["lat"], "lon": st["lon"], "ts": _now_ms()}
        return

    if ev == "leave":
        peers_by_addr.pop(src_addr, None)
        clients.discard(src_addr)
        _log("leave", f"{src_addr}  clients={len(clients)}")
        _send_world_snapshot()
        return

    if ev == "announce":
        peers_by_addr[src_addr] = {"lat": 0.0, "lon": 0.0, "ts": _now_ms()}
        clients.add(src_addr)
        _log("announce", f"{src_addr}")
        _send_world_snapshot([src_addr])
        _send_models_list(src_addr)
        return

    if ev in ("frame-color", "frame-depth"):
        data = body.get("data"); size = 0
        if isinstance(data, str):
            try: size = len(base64.b64decode(data.encode("ascii"), validate=True))
            except Exception: size = len(data)
        _log(ev, f"{src_addr} bytes≈{size}")
        return

    _log("dm-unknown", f"from {src_addr}  event={ev}  body={body}")

# ──────────────────────────────────────────────────────────────────────
# X. HTTP (metadata + models)
# ──────────────────────────────────────────────────────────────────────
app = Flask(__name__)
CORS(app, resources={r"/*": {"origins": "*"}})

@app.route("/")
def root():
    return jsonify(
        ok=True,
        nknAddress=state["nkn_address"],
        topicPrefix=state["topic_prefix"],
        clients=len(clients),
        peers=len(peers_by_addr),
        llmActive=len(_llm_streams),
        ollamaHost=OLLAMA_HOST,
        sessions=len(SESSIONS),
        bridgeStarts=_bridge_starts,
        readyAt=_ready_at,
        lastHeartbeat=_last_hb,
    )

@app.route("/models")
def models():
    try:
        data = _ollama_list()
        return jsonify(ok=True, host=OLLAMA_HOST, **data)
    except Exception as e:
        return jsonify(ok=False, error=str(e), host=OLLAMA_HOST), 500

# ──────────────────────────────────────────────────────────────────────
# XI. Run
# ──────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print("→ NKN client hub running. HTTP is metadata only (no websockets).")
    _print_models_on_start()

    if state["nkn_address"]:
        print(f"   Hub NKN address: {state['nkn_address']}")
    else:
        print("   (Waiting for NKN bridge to connect…)")

    from eventlet import wsgi
    listener = eventlet.listen(("0.0.0.0", 3000))
    wsgi.server(listener, app)
