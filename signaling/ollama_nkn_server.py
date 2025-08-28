#!/usr/bin/env python3
"""
client_signaling_server.py — NKN hub + Ollama bridge
(Session-backed chat context, ordered streaming with ACKs, control channel)

Fixes / Guarantees in this build:
- No Popen monkey-patch recursion. We record child start_time on spawn.
- Bridge watchdog: restart on crash, no-ready timeout, or stalled heartbeats.
- Streaming reliability: run final resend sweeps BEFORE llm.done is sent.
- Handles latent chunk buildup:
    • Node bridge always enqueues, continuously drains, optionally coalesces llm.chunk → llm.bulk
    • Python LLM stream bulk-buffers tiny deltas under backpressure and emits stats
- Models are presented like before (auto-push on 'announce' + request-based paths).
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

import json, secrets, signal, threading, base64, time, shutil, re
from datetime import datetime, timedelta, timezone
from subprocess import Popen, PIPE
from typing import Dict, Any, List, Optional

import jwt
from flask import Flask, jsonify, request
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
    # Default: disable WebRTC in browsers until a TURN is available
    disable_webrtc = "1"
    nkn_subclients = "4"  # default MultiClient sub-clients
    ENV_PATH.write_text(
        f"PEER_SHARED_SECRET={pw}\n"
        f"JWT_SECRET={jwt_sec}\n"
        f"NKN_SEED_HEX={seed_hex}\n"
        f"NKN_TOPIC_PREFIX={topic_ns}\n"
        f"OLLAMA_HOST={ollama_host}\n"
        f"NKN_DISABLE_WEBRTC={disable_webrtc}\n"
        f"NKN_NUM_SUBCLIENTS={nkn_subclients}\n"
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
DISABLE_WEBRTC= dotenv.get("NKN_DISABLE_WEBRTC", "1") in ("1","true","yes","on")
NKN_NUM_SUBCLIENTS = int(dotenv.get("NKN_NUM_SUBCLIENTS", "4"))

# Session policy
SESSION_TTL_SECS   = 60 * 45      # prune sessions idle > 45m
SESSION_MAX_TURNS  = 20           # keep last 20 user/assistant turns (40 msgs) + system
SESSION_LOCK = threading.RLock()

# Watchdog config (bridge)
READY_GRACE_SECS   = 45.0   # was 20.0
HB_INTERVAL_SECS   = 5.0
HB_STALL_SECS      = 180.0  # was 90.0

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
/* nkn_bridge.js — DM-only bridge for client hub (continuous drain, always-enqueue, optional coalesce) */
const nkn = require('nkn-sdk');
const readline = require('readline');

const SEED_HEX = (process.env.NKN_SEED_HEX || '').toLowerCase().replace(/^0x/,'');
const TOPIC_NS = process.env.NKN_TOPIC_PREFIX || 'client';

function sendToPy(obj){ process.stdout.write(JSON.stringify(obj) + '\n'); }
function log(...args){ console.error('[bridge]', ...args); }

let READY = false;
let queue = [];   // {to, data, isPub, topic}
let draining = false;

// Suspend window to pause drain on transient WS closures
let suspendUntil = 0;
function suspend(ms=2000){ suspendUntil = Date.now() + ms; READY = false; }

/* ── Tunables (env overridable) ───────────────────────────────────────── */
const MAX_QUEUE          = parseInt(process.env.NKN_MAX_QUEUE || '4000', 10);
const HEAP_SOFT          = parseInt(process.env.NKN_HEAP_SOFT_MB || '640', 10);
const HEAP_HARD          = parseInt(process.env.NKN_HEAP_HARD_MB || '896', 10);

/* Aggressive defaults to hold e2e latency <1s under load */
const DRAIN_BASE_BATCH   = parseInt(process.env.NKN_DRAIN_BASE || '192', 10);
const DRAIN_MAX_BATCH    = parseInt(process.env.NKN_DRAIN_MAX_BATCH || '1536', 10);
const CATCHUP_AGE_MS     = parseInt(process.env.NKN_CATCHUP_AGE_MS || '500', 10);
const SEND_CONCURRENCY   = parseInt(process.env.NKN_SEND_CONC || '16', 10);

/* Optional head coalescing (disabled by default for compatibility) */
const COALESCE_HEAD          = (process.env.NKN_COALESCE_HEAD || '0') !== '0';
const COALESCE_MAX_COUNT     = parseInt(process.env.NKN_COALESCE_MAX_COUNT || '64', 10);
const COALESCE_MAX_BYTES     = parseInt(process.env.NKN_COALESCE_MAX_BYTES || '65536', 10); // ~64 KiB
const COALESCE_MAX_AGE_MS    = parseInt(process.env.NKN_COALESCE_MAX_AGE_MS || '40', 10);   // flush quickly
const SEND_OPTS = { noReply: true, maxHoldingSeconds: 120 };

function heapMB(){ const m = process.memoryUsage(); return Math.round(((m.rss || 0)/1024/1024)); }

function enqueue(item){
  if (queue.length >= MAX_QUEUE) {
    const drop = queue.splice(0, Math.ceil(MAX_QUEUE * 0.10)); // drop oldest 10%
    try { sendToPy({ type: 'queue.drop', drop: drop.length, q: queue.length }); } catch {}
  }
  // stamp first-seen time for adaptive catch-up; use payload ts if present
  if (!item.data.gen_ts) item.data.gen_ts = item.data.ts || Date.now();
  queue.push(item);
  if (READY && Date.now() >= suspendUntil) kickDrain();
}

function rateLimit(fn, ms){ let last=0; return (...args)=>{ const t=Date.now(); if(t-last>ms){ last=t; fn(...args); } }; }
const logSendErr = rateLimit((msg)=>log('send warn', msg), 1500);

/* ── Optional: coalesce many small llm.chunk at the queue head into one llm.bulk ── */
function tryCoalesceHead(){
  if (!COALESCE_HEAD) return null;
  if (queue.length < 2) return null;

  const first = queue[0];
  const d0 = first && first.data || {};
  if (!first || d0.event !== 'llm.chunk') return null;

  const to = first.to;
  const streamId = d0.id;
  const startTs = d0.gen_ts || Date.now();

  let count = 1;
  let bytes = Buffer.byteLength(JSON.stringify(d0));
  const items = [d0];

  // look-ahead merge
  for (let i = 1; i < queue.length; i++) {
    if (count >= COALESCE_MAX_COUNT) break;
    const it = queue[i];
    if (!it || it.to !== to) break;
    const dx = it.data || {};
    if (dx.event !== 'llm.chunk' || dx.id !== streamId) break;

    const sz = Buffer.byteLength(JSON.stringify(dx));
    if (bytes + sz > COALESCE_MAX_BYTES) break;

    // keep the head window fresh; don't hold too long
    const age = Date.now() - startTs;
    if (age > COALESCE_MAX_AGE_MS) break;

    items.push(dx);
    bytes += sz;
    count++;
  }

  if (count <= 1) return null;

  // remove merged items from the head
  queue.splice(0, count);

  const merged = {
    to,
    data: {
      event: 'llm.bulk',
      id: streamId,
      items,                // array of original llm.chunk payloads (ordered)
      ts: Date.now(),
      gen_ts: d0.gen_ts
    }
  };
  return merged;
}

/* ── Core drain ──────────────────────────────────────────────────────── */
async function safeSendDM(client, to, data){
  try {
    await client.send(to, JSON.stringify(data), SEND_OPTS);
    return true;
  } catch (e) {
    const msg = (e && e.message) || String(e || '');
    logSendErr(`${to} → ${msg}`);
    if (msg.includes('WebSocket unexpectedly closed')) {
      suspend(2500);
      try { sendToPy({ type: 'wsclosed', reason: msg, ts: Date.now() }); } catch {}
      setTimeout(() => { if (READY) kickDrain(); }, 2600);
    }
    return false;
  }
}
async function safePub(client, topic, data){
  try {
    await client.publish(TOPIC_NS + '.' + topic, JSON.stringify(data));
    return true;
  } catch (e) {
    const msg = (e && e.message) || String(e || '');
    logSendErr(`pub ${topic} → ${msg}`);
    if (msg.includes('WebSocket unexpectedly closed')) {
      suspend(2500);
      try { sendToPy({ type: 'wsclosed', reason: msg, ts: Date.now() }); } catch {}
      setTimeout(() => { if (READY) kickDrain(); }, 2600);
    }
    return false;
  }
}

async function drain(client){
  if (!READY || Date.now() < suspendUntil || queue.length === 0) return;

  const now = Date.now();
  const firstTs = (queue[0] && queue[0].data && (queue[0].data.gen_ts || queue[0].data.ts)) || now;
  const age = Math.max(0, now - firstTs);
  const backlog = queue.length;

  // adaptive take size
  let take = DRAIN_BASE_BATCH;
  if (backlog > 256 || age > CATCHUP_AGE_MS) take = Math.min(DRAIN_MAX_BATCH, DRAIN_BASE_BATCH * 2);
  if (backlog > 512 || age > 2*CATCHUP_AGE_MS) take = Math.min(DRAIN_MAX_BATCH, Math.floor(DRAIN_BASE_BATCH * 3));
  if (backlog > 1024 || age > 3*CATCHUP_AGE_MS) take = DRAIN_MAX_BATCH;

  const batch = [];
  while (batch.length < take && queue.length > 0) {
    const merged = tryCoalesceHead();
    if (merged) { batch.push(merged); continue; }
    batch.push(queue.shift());
  }

  // higher parallelism when batch is big
  const workers = Math.min(SEND_CONCURRENCY, Math.max(1, Math.ceil(batch.length / 8)));
  let idx = 0;

  async function worker(){
    while (true) {
      const i = idx++; if (i >= batch.length) break;
      const it = batch[i];
      const ok = it.isPub ? await safePub(client, it.topic, it.data)
                          : await safeSendDM(client, it.to, it.data);
      if (!ok) queue.unshift(it); // retry after transient flap
    }
  }
  await Promise.all(new Array(workers).fill(0).map(()=>worker()));
}

async function kickDrain(){
  if (draining) return;
  draining = true;
  try {
    while (queue.length > 0 && READY && Date.now() >= suspendUntil) {
      await drain(clientRef);
      // micro backoff only when small backlog
      const q = queue.length;
      const wait = q > 1500 ? 0 : q > 300 ? 1 : 3;
      if (wait > 0) await new Promise(r => setTimeout(r, wait));
      // if wait==0, let the event loop breathe:
      if (wait === 0) await new Promise(r => setImmediate(r));
    }
  } finally {
    draining = false;
  }
}

/* ── Client bootstrap ────────────────────────────────────────────────── */
let clientRef = null;

(async () => {
  if (!/^[0-9a-f]{64}$/.test(SEED_HEX)) throw new RangeError('invalid hex seed');

  const SEED_WS = (process.env.NKN_BRIDGE_SEED_WS || '')
    .split(',').map(s => s.trim()).filter(Boolean);

  const NUM_SUBCLIENTS = parseInt(process.env.NKN_NUM_SUBCLIENTS || '4', 10) || 4;

  const client = new nkn.MultiClient({
    seed: SEED_HEX,
    identifier: 'hub',
    numSubClients: NUM_SUBCLIENTS,
    seedWsAddr: SEED_WS.length ? SEED_WS : undefined,
    wsConnHeartbeatTimeout: 120000,
    reconnectIntervalMin: 1000,
    reconnectIntervalMax: 10000,
    connectTimeout: 15000,
  });
  clientRef = client;

  client.on('connect', () => {
    if (READY) return;
    READY = true;
    sendToPy({ type: 'ready', address: client.addr, topicPrefix: TOPIC_NS, ts: Date.now() });
    log('ready at', client.addr);
    kickDrain();
  });

  client.on('error', e => {
    const msg = (e && e.message) || String(e || '');
    log('client error', msg);
    if (msg.includes('WebSocket unexpectedly closed') || msg.includes('VERIFY SIGNATURE ERROR')) {
      try { sendToPy({ type: 'wsclosed', reason: msg, ts: Date.now() }); } catch {}
      suspend(2500);
      setTimeout(() => { if (READY) kickDrain(); }, 2600);
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

  /* ALWAYS enqueue from stdin; let drain handle sending with concurrency */
  const rl = readline.createInterface({ input: process.stdin });
  rl.on('line', async line => {
    if (!line) return;
    let cmd; try { cmd = JSON.parse(line); } catch { return; }

    if (cmd.type === 'dm' && cmd.to && cmd.data) {
      enqueue({to: cmd.to, data: cmd.data});
    } else if (cmd.type === 'pub' && cmd.topic && cmd.data) {
      enqueue({isPub:true, topic: cmd.topic, data: cmd.data});
    }
  });

  /* Heap guard: soft trim, then hard exit (Python watchdog restarts us) */
  setInterval(() => {
    const mb = heapMB();
    if (mb >= HEAP_SOFT) {
      try { if (global.gc) global.gc(); } catch {}
      if (queue.length > Math.floor(MAX_QUEUE * 0.6)) {
        const drop = queue.splice(0, Math.max(1, queue.length - Math.floor(MAX_QUEUE * 0.6)));
        try { sendToPy({ type: 'queue.trim', drop: drop.length, q: queue.length, mb }); } catch {}
      }
    }
    if (mb >= HEAP_HARD) {
      try { sendToPy({ type: 'crit.heap', mb, q: queue.length }); } catch {}
      process.exitCode = 137;
      setTimeout(() => process.exit(137), 10);
    }
  }, 2000);

  // Bridge heartbeat to Python watchdog
  setInterval(() => {
    try { sendToPy({ type: 'hb', ts: Date.now(), ready: READY, q: queue.length, mb: heapMB() }); } catch {}
  }, 5000);

  // Safety net: if anything left in queue and we're READY, keep nudging the pump
  setInterval(() => {
    if (queue.length && READY && Date.now() >= suspendUntil) kickDrain();
  }, 25);

  // Fail fast on unexpected errors
  process.on('uncaughtException', (e) => { try { sendToPy({ type: 'crit.uncaught', msg: String(e&&e.message||e) }); } catch {}; process.exit(1); });
  process.on('unhandledRejection', (e) => { try { sendToPy({ type: 'crit.unhandled', msg: String(e&&e.message||e) }); } catch {}; process.exit(1); });

})();
"""

if not BRIDGE_JS.exists() or BRIDGE_JS.read_text() != BRIDGE_SRC:
    BRIDGE_JS.write_text(BRIDGE_SRC)

bridge_env = os.environ.copy()
bridge_env["NKN_SEED_HEX"]       = NKN_SEED_HEX
bridge_env["NKN_TOPIC_PREFIX"]   = TOPIC_PREFIX
bridge_env["NKN_NUM_SUBCLIENTS"] = str(NKN_NUM_SUBCLIENTS)

# Bridge process + watchdog state
bridge = None
state: Dict[str, Any] = {"nkn_address": None, "topic_prefix": TOPIC_PREFIX}
_last_hb = 0.0
_ready_at = 0.0
_bridge_starts = 0
_bridge_lock = threading.Lock()
_bridge_write_lock = threading.Lock()  # ← serialize writes to child stdin
_last_restart_ts = 0.0
_FORCE_RESTART = False
_CRIT_ERR_PATTERNS = (
    "FATAL ERROR: Ineffective mark-compacts",
    "JavaScript heap out of memory",
    "FatalProcessOutOfMemory",
    "node::Abort()",
    "v8::internal::V8::FatalProcessOutOfMemory",
)
# Track websocket flap storms (repeated 'wsclosed' events) so we can restart proactively
_ws_flap_times: List[float] = []
_ws_flap_lock = threading.Lock()
WS_FLAP_WINDOW = 60.0   # seconds to look back
WS_FLAP_MAX = 6         # restart if >= this many wsclosed in window
# ---- memory/backpressure tuning for the Node bridge
bridge_env["NKN_MAX_QUEUE"]     = os.environ.get("NKN_MAX_QUEUE", "4000")   # max pending sends
bridge_env["NKN_HEAP_SOFT_MB"]  = os.environ.get("NKN_HEAP_SOFT_MB", "640") # attempt GC, trim queue
bridge_env["NKN_HEAP_HARD_MB"]  = os.environ.get("NKN_HEAP_HARD_MB", "896") # exit → watchdog restarts
# Make crashes predictable & fast; expose GC so bridge can try to recover before restart
node_opts = bridge_env.get("NODE_OPTIONS", "")
extra = "--unhandled-rejections=strict --heapsnapshot-signal=SIGUSR2 --max-old-space-size=1024"
bridge_env["NODE_OPTIONS"] = (node_opts + " " + extra).strip()

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
            line = json.dumps(obj) + "\n"
            with _bridge_write_lock:
                bridge.stdin.write(line)
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
                addr = msg.get("address")
                if state.get("nkn_address") == addr and _ready_at > 0:
                    continue  # ← ignore duplicate ready
                state["nkn_address"]  = addr
                state["topic_prefix"] = msg.get("topicPrefix") or TOPIC_PREFIX
                _ready_at = time.time()
                print(f"→ NKN ready: {state['nkn_address']}  (topics prefix: {state['topic_prefix']})")

            elif msg.get("type") == "hb":
                _last_hb = time.time()
                try:
                    state["bridge_ready"] = bool(msg.get("ready"))
                    state["bridge_q"]     = int(msg.get("q") or 0)
                    state["bridge_mb"]    = int(msg.get("mb") or 0)
                    state["bridge_hb_ts"] = int(msg.get("ts") or _now_ms())
                except Exception:
                    pass

            elif msg.get("type") == "wsclosed":
                print("⚠️ bridge reported wsclosed — pausing sends until reconnect …")
                try:
                    now = time.time()
                    with _ws_flap_lock:
                        _ws_flap_times.append(now)
                        cutoff = now - WS_FLAP_WINDOW
                        while _ws_flap_times and _ws_flap_times[0] < cutoff:
                            _ws_flap_times.pop(0)
                except Exception:
                    pass

            elif msg.get("type") == "nkn-dm":
                src = msg.get("src") or ""
                body = msg.get("msg") or {}
                _handle_dm(src, body)

            elif msg.get("type") in ("queue.drop", "queue.trim"):
                mb = msg.get("mb")
                if mb is not None:
                    print(f"⚠️ bridge backpressure: {msg['type']} drop={msg.get('drop')} q={msg.get('q')} mb={mb}")
                else:
                    print(f"⚠️ bridge backpressure: {msg['type']} drop={msg.get('drop')} q={msg.get('q')}")

            elif msg.get("type") in ("crit.heap", "crit.uncaught", "crit.unhandled"):
                print(f"‼️ bridge critical: {msg.get('type')} mb={msg.get('mb')} q={msg.get('q')}")
                globals()["_FORCE_RESTART"] = True
        except Exception:
            time.sleep(0.2)

def _bridge_reader_stderr():
    while True:
        try:
            if bridge and bridge.stderr:
                line = bridge.stderr.readline()
                if line:
                    sys.stderr.write(line)
                    low = line.lower()
                    if any(pat.lower() in low for pat in _CRIT_ERR_PATTERNS):
                        globals()["_FORCE_RESTART"] = True
                else:
                    time.sleep(0.1)
            else:
                time.sleep(0.2)
        except Exception:
            time.sleep(0.2)

def _restart_bridge_locked():
    global bridge, _last_restart_ts
    now = time.time()
    if (now - _last_restart_ts) < 2.0 and bridge:
        return
    _last_restart_ts = now
    try:
        if bridge:
            try:
                bridge.terminate()
                for _ in range(15):
                    if bridge.poll() is not None:
                        break
                    time.sleep(0.1)
                if bridge.poll() is None:
                    bridge.kill()
            except Exception:
                try: bridge.kill()
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

                if not bridge or bridge.poll() is not None:
                    need_restart = True
                elif getattr(bridge, "start_time", 0) and _ready_at == 0.0:
                    if now - getattr(bridge, "start_time", 0) > READY_GRACE_SECS:
                        need_restart = True
                elif _ready_at > 0.0 and (now - _last_hb) > HB_STALL_SECS:
                    need_restart = True
                else:
                    try:
                        with _ws_flap_lock:
                            flaps = len([t for t in _ws_flap_times if now - t <= WS_FLAP_WINDOW])
                        if flaps >= WS_FLAP_MAX:
                            print(f"↻ restarting NKN bridge (ws flap storm: {flaps}/{WS_FLAP_WINDOW}s) …")
                            need_restart = True
                    except Exception:
                        pass

                if globals().get("_FORCE_RESTART"):
                    print("↻ restarting NKN bridge (critical stderr pattern) …")
                    globals()["_FORCE_RESTART"] = False
                    need_restart = True

                if need_restart:
                    with _ws_flap_lock:
                        _ws_flap_times.clear()
                    _restart_bridge_locked()
            time.sleep(1.0)
        except Exception:
            time.sleep(1.0)

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

# ── Blob (multi-part) ingest ──────────────────────────────────────────
BLOB_DIR = BASE_DIR / "blobs"
BLOB_DIR.mkdir(parents=True, exist_ok=True)

BLOB_TTL_SECS = 60 * 60      # keep finished blobs 1h
BLOB_STALL_SECS = 15 * 60    # kill half-finished uploads after 15m
_BLOBS_LOCK = threading.RLock()
# key: (src_addr, blob_id) -> dict(meta)
_BLOBS: Dict[tuple, dict] = {}

def _blob_key(src: str, blob_id: str): return (src, blob_id)
def _blob_path(blob_id: str) -> Path: return BLOB_DIR / f"{blob_id}.bin"
def _blob_tmp_path(blob_id: str) -> Path: return BLOB_DIR / f"{blob_id}.tmp"
def _blob_touch(meta: dict): meta["updated_at"] = time.time()

def _blob_gc_loop():
    while True:
        time.sleep(30)
        now = time.time()
        with _BLOBS_LOCK:
            dead = []
            for k, m in list(_BLOBS.items()):
                created = m.get("created_at", now)
                updated = m.get("updated_at", created)
                done = bool(m.get("done"))
                if (not done and (now - updated) > BLOB_STALL_SECS) or \
                   (done and (now - updated) > BLOB_TTL_SECS):
                    try:
                        _blob_tmp_path(m["id"]).unlink(missing_ok=True)
                        _blob_path(m["id"]).unlink(missing_ok=True)
                    except Exception:
                        pass
                    dead.append(k)
            for k in dead:
                _BLOBS.pop(k, None)
threading.Thread(target=_blob_gc_loop, daemon=True).start()

def _dur_to_seconds(v) -> float:
    try:
        x = float(v)
    except Exception:
        return 0.0
    if x > 3_000_000:   # >3ms → probably ns
        return x / 1_000_000_000.0
    if x > 3_000:       # >3s if ms, or 3k seconds; assume ms
        return x / 1_000.0
    return x

def _now_ms() -> int: return int(time.time() * 1000)

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

# Monotonic per-peer cooldown + tiny cache to avoid spam during reconnects
_last_models_sent: Dict[str, float] = {}  # addr -> monotonic seconds
_models_cache: Dict[str, Any] = {"t": 0.0, "data": None}

def _ollama_list_cached(ttl: float = 5.0) -> dict:
    now = time.monotonic()
    cached = _models_cache.get("data")
    if cached is not None and (now - _models_cache.get("t", 0.0)) < ttl:
        return cached
    data = _ollama_list()
    _models_cache["t"] = now
    _models_cache["data"] = data
    return data

def _send_models_list(to: str, req_id: Optional[str] = None):
    """
    Idempotent, id-tagged models push.
    Always replies to each request (no digest suppression or cooldown),
    but still uses the cached Ollama list when available. Emits both
    `llm.models` and `llm.result` for compatibility, and also `ctrl.models`.
    """
    g = globals()
    if "_models_send_lock" not in g:
        g["_models_send_lock"] = threading.Lock()

    with _models_send_lock:
        getter = g.get("_ollama_list_cached") or g.get("_ollama_list")
        if not getter:
            def getter():  # type: ignore
                return {"models": []}
        try:
            data = getter()
        except Exception as e:
            _dm(to, {
                "event": "llm.error",
                "id": (req_id or "models"),
                "message": f"models list failed: {e}",
                "kind": "models",
                "ts": _now_ms(),
            })
            return

        rid = req_id or f"models:{_now_ms()}"
        ts = _now_ms()
        _dm(to, {"event": "llm.models", "id": rid, "data": data, "ts": ts})
        _dm(to, {"event": "llm.result", "id": rid, "data": data, "ts": _now_ms()})
        _dm(to, {"event": "ctrl.models", "id": rid, "data": data, "ts": _now_ms()})
        _log("models.sent", f"→ {to} id={rid} count={len((data or {}).get('models', []))}")

import base64 as _b64

# Hardened _to_jsonable (overload safe for bytes/datetimes/sets)
def _to_jsonable2(x):
    if isinstance(x, (str, int, float, bool)) or x is None:
        return x
    if isinstance(x, datetime):
        return x.isoformat()
    if isinstance(x, (bytes, bytearray, memoryview)):
        try: return _b64.b64encode(bytes(x)).decode("ascii")
        except Exception: return str(x)
    if isinstance(x, (set, frozenset)):
        return [_to_jsonable2(i) for i in sorted(list(x), key=lambda t: str(t))]
    if isinstance(x, (list, tuple)):
        return [_to_jsonable2(i) for i in x]
    if isinstance(x, dict):
        return {k: _to_jsonable2(v) for k, v in x.items()}
    if hasattr(x, "model_dump") and callable(getattr(x, "model_dump")):
        return _to_jsonable2(x.model_dump())
    if hasattr(x, "dict") and callable(getattr(x, "dict")):
        return _to_jsonable2(x.dict())
    if hasattr(x, "__dict__"):
        return _to_jsonable2({k: v for k, v in x.__dict__.items() if not k.startswith("_")})
    return str(x)

# strip LICENSE directive inside an Ollama Modelfile, but keep everything else
_LICENSE_TRIPLE_DQ = re.compile(r'(?is)^[ \t]*LICENSE[ \t]+"""[\s\S]*?"""[ \t]*\n?', re.MULTILINE)
_LICENSE_TRIPLE_SQ = re.compile(r"(?is)^[ \t]*LICENSE[ \t]+'''[\s\S]*?'''[ \t]*\n?", re.MULTILINE)
_LICENSE_SINGLE_DQ = re.compile(r'(?im)^[ \t]*LICENSE[ \t]+"[^"\n]*"[ \t]*\n?', re.MULTILINE)
_LICENSE_SINGLE_SQ = re.compile(r"(?im)^[ \t]*LICENSE[ \t]+'[^'\n]*'[ \t]*\n?", re.MULTILINE)
_LICENSE_ASSIGN     = re.compile(r'(?is)^[ \t]*license[ \t]*=[ \t]*(?:"""[\s\S]*?"""|\'\'\'[\s\S]*?\'\'\'|"[^"\n]*"|\'[^\n\']*\')[ \t]*\n?', re.MULTILINE)

def _strip_license_from_modelfile(s: str) -> str:
    if not isinstance(s, str): return s
    s = _LICENSE_TRIPLE_DQ.sub("", s)
    s = _LICENSE_TRIPLE_SQ.sub("", s)
    s = _LICENSE_SINGLE_DQ.sub("", s)
    s = _LICENSE_SINGLE_SQ.sub("", s)
    s = _LICENSE_ASSIGN.sub("", s)
    return s

def _slim_show_payload_keep_modelfile_no_license(raw):
    d = _to_jsonable2(raw)
    if not isinstance(d, dict):
        return d
    for k in ("license", "License", "LICENSE", "license_url", "license_text"):
        d.pop(k, None)
    det = d.get("details")
    if isinstance(det, dict):
        for k in ("license", "License", "LICENSE", "license_url", "license_text"):
            det.pop(k, None)
    mf = d.get("modelfile")
    if isinstance(mf, str):
        d["modelfile"] = _strip_license_from_modelfile(mf)
    return d

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
    def touch(self): self.last_used = time.time()

SESSIONS: Dict[tuple, Session] = {}  # (src_addr, sid) -> Session
SESSIONS_HARD_CAP = int(os.environ.get("SESSIONS_HARD_CAP", "500"))

def _evict_oldest_sessions_if_needed():
    if len(SESSIONS) <= SESSIONS_HARD_CAP: return
    k_sorted = sorted(SESSIONS.items(), key=lambda kv: kv[1].last_used)
    to_evict = max(1, len(SESSIONS) - int(SESSIONS_HARD_CAP * 0.9))
    for k, _ in k_sorted[:to_evict]:
        SESSIONS.pop(k, None)

def _sess_key(src_addr: str, sid: str): return (src_addr, sid or "")
_VALID_ROLES = {"system", "user", "assistant", "tool"}

def _sanitize_msg(m: dict) -> Dict[str, Any]:
    role = str(m.get("role", "")).strip().lower()
    if role not in _VALID_ROLES: role = "user"
    content = m.get("content", "")
    if not isinstance(content, str): content = "" if content is None else str(content)
    out: Dict[str, Any] = {"role": role, "content": content}
    if "name" in m: out["name"] = str(m["name"])
    if "tool_call_id" in m: out["tool_call_id"] = str(m["tool_call_id"])
    if "tool_call_id".replace("_", "") in m: out["tool_call_id"] = str(m["toolCallId"])
    if isinstance(m.get("tool_calls"), list): out["tool_calls"] = m["tool_calls"]
    return out

def _sanitize_history(history) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    if not isinstance(history, (list, tuple)): return out
    for m in history:
        if isinstance(m, dict) and ("role" in m or "content" in m or "tool_calls" in m):
            out.append(_sanitize_msg(m))
    return out

def _open_session(src_addr:str, sid:str, model:str=None, system:str=None, options:dict=None,
                  history: Optional[List[Dict[str,str]]]=None, replace: bool=False) -> Session:
    if not isinstance(sid, str) or not sid:
        raise ValueError("invalid sid")
    with SESSION_LOCK:
        _evict_oldest_sessions_if_needed()
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
                        s.system = m["content"]; break
            turns = [{"role": m["role"], "content": m["content"], "ts": _now_ms()}
                     for m in hist if m["role"] in ("user","assistant")]
            if replace: s.messages = turns
            else: s.messages.extend(turns)
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

ACTIVE_WINDOW_MS = 30_000
PRUNE_WINDOW_MS  = 120_000

def _send_world_snapshot(targets=None):
    now = _now_ms()
    for addr, st in list(peers_by_addr.items()):
        if st.get("ts", 0) < (now - PRUNE_WINDOW_MS):
            _log("prune", f"{addr} idle")
            peers_by_addr.pop(addr, None)
            clients.discard(addr)
    client_snapshot = tuple(clients)
    peers_snapshot = dict(peers_by_addr)
    if targets is None:
        targets = [
            a for a in client_snapshot
            if peers_snapshot.get(a, {}).get("ts", 0) >= (now - ACTIVE_WINDOW_MS)
        ]
    if not targets:
        return
    peers_obj = {
        addr: {"lat": st["lat"], "lon": st["lon"], "ts": st["ts"]}
        for addr, st in peers_snapshot.items()
    }
    pkt = {"event": "world", "peers": peers_obj, "ts": now}
    for to in targets:
        _dm(to, pkt)

def _broadcaster_loop():
    while True:
        try:
            _send_world_snapshot()
        except Exception as e:
            _log("broadcast-error", repr(e))
        time.sleep(2.0)

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
        return json.dumps(_to_jsonable2(data))
    except Exception:
        return str(data)

_DATAURL_PREFIX = "data:"

def _decode_b64_to_bytes(s: str) -> Optional[bytes]:
    try:
        if not isinstance(s, str): return None
        b64 = s
        if s.startswith(_DATAURL_PREFIX):
            _, _, tail = s.partition(",")
            b64 = tail or s
        return base64.b64decode(b64, validate=False)
    except Exception:
        return None

def _resolve_blob_ref(src_addr: str, ref: str) -> Optional[bytes]:
    if not isinstance(ref, str): return None
    bid = ref.split(":", 1)[1] if ref.startswith("blob:") else ref
    key = _blob_key(src_addr, bid)
    with _BLOBS_LOCK:
        meta = _BLOBS.get(key)
        if not meta or not meta.get("done"): return None
    p = _blob_path(bid)
    try:
        return p.read_bytes()
    except Exception:
        return None

INLINE_IMAGE_MAX_B = int(os.environ.get("INLINE_IMAGE_MAX_B", str(256 * 1024)))
MAX_BLOB_BYTES     = int(os.environ.get("MAX_BLOB_BYTES", str(25 * 1024 * 1024)))

def _massage_images_in_messages(
    msgs: List[Dict[str, Any]],
    src_addr: Optional[str] = None,
    *,
    log_prefix: str = ""
) -> List[Dict[str, Any]]:
    g = globals()
    INLINE_IMAGE_MAX_B = int(g.get("INLINE_IMAGE_MAX_B", 64 * 1024))
    MAX_REASSEMBLED_IMAGE_BYTES = int(g.get("MAX_BLOB_BYTES", 10 * 1024 * 1024))

    def _strip_dataurl_prefix(s: str) -> str:
        if isinstance(s, str) and s.startswith(_DATAURL_PREFIX):
            _, _, tail = s.partition(",")
            return tail or ""
        return s or ""

    def _is_chunk_obj(x: Any) -> bool:
        if not isinstance(x, dict): return False
        if isinstance(x.get("parts") or x.get("chunks"), (list, tuple)): return True
        has_seq = ("seq" in x or "chunkIndex" in x)
        has_total = ("total" in x or "chunkTotal" in x)
        has_id = ("id" in x or "chunkId" in x)
        return has_seq and has_total and has_id and ("data" in x)

    def _collect_chunks_from_list(items: List[Any]) -> List[bytes]:
        bin_out: List[bytes] = []
        buckets: Dict[str, Dict[str, Any]] = {}
        seen_hashes: set = set()
        import hashlib

        for it in items:
            if isinstance(it, (bytes, bytearray)):
                b = bytes(it)
                if b:
                    h = hashlib.sha256(b).hexdigest()
                    if h not in seen_hashes:
                        seen_hashes.add(h); bin_out.append(b)
                continue

            if isinstance(it, str):
                b = _decode_b64_to_bytes(it)
                if b:
                    if len(b) > INLINE_IMAGE_MAX_B:
                        _log("vision.drop", f"inline image too large ({len(b)} bytes) — dropped")
                        continue
                    h = hashlib.sha256(b).hexdigest()
                    if h not in seen_hashes:
                        seen_hashes.add(h); bin_out.append(b)
                continue

            if _is_chunk_obj(it):
                parts_arr = it.get("parts") or it.get("chunks")
                if isinstance(parts_arr, (list, tuple)) and parts_arr:
                    try:
                        joined_b64 = "".join(_strip_dataurl_prefix(str(p) or "") for p in parts_arr)
                        b = _decode_b64_to_bytes(joined_b64)
                        if b:
                            if len(b) > MAX_REASSEMBLED_IMAGE_BYTES:
                                _log("vision.drop", f"⚠️ chunked image too large ({len(b)} bytes) — dropped")
                            else:
                                h = hashlib.sha256(b).hexdigest()
                                if h not in seen_hashes:
                                    seen_hashes.add(h); bin_out.append(b)
                    except Exception as e:
                        _log("vision.chunk.err", f"parts join/decode failed: {e}")
                    continue

                cid = str(it.get("id") or it.get("chunkId") or "")
                if not cid:
                    b = _decode_b64_to_bytes(it.get("data") or "")
                    if b:
                        h = hashlib.sha256(b).hexdigest()
                        if h not in seen_hashes:
                            seen_hashes.add(h); bin_out.append(b)
                    continue

                total = int(it.get("total") or it.get("chunkTotal") or 0)
                seq = int(it.get("seq") if it.get("seq") is not None else it.get("chunkIndex") or 0)
                data_b64 = _strip_dataurl_prefix(str(it.get("data") or ""))

                bkt = buckets.setdefault(cid, {"total": max(0, total), "parts": {}})
                if bkt["total"] <= 0 and total > 0:
                    bkt["total"] = total
                if seq not in bkt["parts"] and data_b64:
                    bkt["parts"][seq] = data_b64
                continue

            try:
                s = str(it)
                b = _decode_b64_to_bytes(s)
                if b:
                    h = hashlib.sha256(b).hexdigest()
                    if h not in seen_hashes:
                        seen_hashes.add(h); bin_out.append(b)
            except Exception:
                pass

        for cid, rec in buckets.items():
            total = int(rec.get("total") or 0)
            parts: Dict[int, str] = rec.get("parts") or {}
            if total <= 0:
                total = max(parts.keys(), default=-1) + 1
            if total > 0 and len(parts) >= total:
                try:
                    joined_b64 = "".join(parts[i] for i in range(total))
                except Exception:
                    missing = [i for i in range(total) if i not in parts]
                    _log("vision.chunk.warn", f"{cid} missing sequences: {missing[:8]}{'…' if len(missing)>8 else ''}")
                    joined_b64 = None

                if joined_b64:
                    try:
                        b = _decode_b64_to_bytes(joined_b64)
                        if b:
                            if len(b) > MAX_REASSEMBLED_IMAGE_BYTES:
                                _log("vision.drop", f"⚠️ chunked image {cid} too large ({len(b)} bytes) — dropped")
                            else:
                                h = hashlib.sha256(b).hexdigest()
                                if h not in seen_hashes:
                                    seen_hashes.add(h); bin_out.append(b)
                    except Exception as e:
                        _log("vision.chunk.err", f"{cid} join/decode failed: {e}")
            else:
                _log("vision.chunk.warn", f"{cid} incomplete: have {len(parts)}/{total}")

        return bin_out

    out: List[Dict[str, Any]] = []
    inline_ct = 0; inline_chunked_ct = 0; ref_ct = 0; total_bytes = 0

    for i, m in enumerate(msgs or []):
        m2 = dict(m); bin_list: List[bytes] = []
        imgs = m2.get("images") or m2.get("images_b64")
        if isinstance(imgs, (list, tuple)) and imgs:
            for it in imgs:
                if isinstance(it, dict) and ("parts" in it or "chunks" in it or "seq" in it or "chunkIndex" in it):
                    inline_chunked_ct += 1
                else:
                    inline_ct += 1
            try:
                decoded = _collect_chunks_from_list(list(imgs))
                if decoded:
                    bin_list.extend(decoded); total_bytes += sum(len(b) for b in decoded)
            except Exception as e:
                _log("vision.inline.err", f"decode/reassemble failed: {e}")

        if src_addr and isinstance(m2.get("images_ref"), (list, tuple)):
            for ref in m2["images_ref"]:
                b = _resolve_blob_ref(src_addr, ref)
                if b:
                    bin_list.append(b); ref_ct += 1; total_bytes += len(b)

        if bin_list:
            m2["images"] = [bytes(x) for x in bin_list]
            m2.pop("images_b64", None); m2.pop("images_ref", None)
        out.append(m2)

    if inline_ct or inline_chunked_ct or ref_ct:
        _log("vision.attach",
             f"🖼️ {log_prefix} inline={inline_ct} inline_chunked={inline_chunked_ct} blobs={ref_ct} bytes≈{total_bytes}")
    return out

class _LLMStream:
    RESEND_INTERVAL = 1
    WINDOW_LIMIT    = 512
    FINAL_SWEEPS    = 3
    FINAL_GAP_SECS  = 0.12
    ACK_STALL_SECS   = 25.0
    MAX_WINDOW_PKTS  = 1024
    BULK_UNACK_THRESHOLD = 256
    BULK_MAX_MS          = 120
    BULK_MAX_PARTS       = 16
    STATS_INTERVAL_SECS = 0.5
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
        self._last_ack_change = time.monotonic()
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
        self.sid     = self.client_cfg.get("sid") or self.kwargs.get("sid") or req.get("sid")
        self._stats = {
            "t0": None, "tlast": None, "chars": 0,
            "eval_count": None, "eval_duration_s": None,
        }
        self._last_stats_emit = 0.0
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
        try:
            if isinstance(data, dict):
                if "eval_count" in data: self._stats["eval_count"] = int(data["eval_count"])
                if "eval_duration" in data: self._stats["eval_duration_s"] = _dur_to_seconds(data["eval_duration"])
            now = time.monotonic()
            if self._stats["t0"] is None: self._stats["t0"] = now
            self._stats["tlast"] = now
        except Exception:
            pass
        self._dm({"event":"llm.result","id":self.id,"data":_to_jsonable2(data),"ts":_now_ms()})
        try:
            if callable(self.on_done):
                txt = _extract_full_text(self.api, _to_jsonable2(data))
                self.on_done(txt)
        except Exception as e:
            _log("llm.on_done.err", str(e))
        try: self._emit_stats("nonstream")
        except Exception: pass

    def _send_chunk(self, data: dict):
        with self._lock:
            self.seq += 1
            seq = self.seq
            pkt = {"event":"llm.chunk","id":self.id,"seq":seq,"data":_to_jsonable2(data),"ts":_now_ms()}
            self.window[seq] = pkt
            self.window_order.append(seq)
            if len(self.window_order) > self.WINDOW_LIMIT:
                oldest = self.window_order.pop(0)
                if oldest <= self.last_acked:
                    self.window.pop(oldest, None)
                else:
                    self.window_order.insert(0, oldest)
            if len(self.window_order) > self.MAX_WINDOW_PKTS:
                self._send_error("backpressure: client not ACKing fast enough; aborting stream", kind="backpressure")
                self.cancelled = True
                return
            self._dm(pkt)

        try:
            self._update_stats_from_part(data)
            now = time.monotonic()
            if (now - self._last_stats_emit) >= self.STATS_INTERVAL_SECS:
                self._emit_stats("stream")
                self._last_stats_emit = now
        except Exception:
            pass

        try:
            if callable(self.on_delta):
                delta = _extract_delta_from_part(self.api, data)
                if delta: self.on_delta(delta)
        except Exception as e:
            _log("llm.on_delta.err", str(e))

    def _sweep_resend_unacked(self):
        with self._lock:
            start = self.last_acked + 1
            end   = self.seq
            if end < start: return
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
            if callable(self.on_done) and self.stream:
                self.on_done(None)
        except Exception as e:
            _log("llm.on_done.err", str(e))

    def ack(self, upto: int):
        with self._lock:
            if upto > self.last_acked:
                self.last_acked = upto
                self._last_ack_change = time.monotonic()
                for s in [s for s in list(self.window.keys()) if s <= self.last_acked]:
                    self.window.pop(s, None)
                self.window_order = [s for s in self.window_order if s > self.last_acked]

    def cancel(self):
        with self._lock:
            self.cancelled = True

    def _resend_loop(self):
        while True:
            now = time.monotonic()
            with self._lock:
                stalled = (now - self._last_ack_change) > self.ACK_STALL_SECS
                unacked = self.seq - self.last_acked
            if stalled and unacked > (self.MAX_WINDOW_PKTS // 2):
                self._send_error("ack stall: aborting stream to prevent memory pressure", kind="ack_stall")
                self.cancelled = True
                return
            if self.cancelled: return
            time.sleep(self.RESEND_INTERVAL)
            with self._lock:
                target_hi = self.seq
                start = self.last_acked + 1
                for s in range(start, target_hi + 1):
                    pkt = self.window.get(s)
                    if pkt is not None:
                        self._dm(pkt)
                if self.done_sent and self.last_acked >= self.done_seq:
                    return

    def _send_coalesced_text(self, text: str):
        if not text: return
        if self.api == "chat":
            data = {"message": {"content": text}}
        else:
            data = {"response": text}
        self._send_chunk(data)

    def _update_stats_from_part(self, part: dict):
        now = time.monotonic()
        delta = _extract_delta_from_part(self.api, part)
        if delta:
            if self._stats["t0"] is None: self._stats["t0"] = now
            self._stats["tlast"] = now
            self._stats["chars"] += len(delta)
        try:
            if isinstance(part, dict):
                if "eval_count" in part: self._stats["eval_count"] = int(part["eval_count"])
                if "eval_duration" in part: self._stats["eval_duration_s"] = _dur_to_seconds(part["eval_duration"])
                if (not self._stats["eval_duration_s"]) and ("total_duration" in part):
                    self._stats["eval_duration_s"] = _dur_to_seconds(part["total_duration"])
                if "prompt_eval_count" in part: self._stats["prompt_eval_count"] = int(part["prompt_eval_count"])
                if "prompt_eval_duration" in part: self._stats["prompt_eval_duration_s"] = _dur_to_seconds(part["prompt_eval_duration"])
        except Exception:
            pass

    def _emit_stats(self, phase: str = "stream"):
        t0 = self._stats["t0"]; tlast = self._stats["tlast"]
        elapsed_s = max(0.0, (tlast - t0)) if (t0 is not None and tlast is not None) else 0.0
        cps = (self._stats["chars"] / elapsed_s) if elapsed_s > 0 else 0.0
        tps_real = None
        if (self._stats.get("eval_count") is not None) and (self._stats.get("eval_duration_s") not in (None, 0.0)):
            tps_real = float(self._stats["eval_count"]) / float(self._stats["eval_duration_s"])
        approx_tokens = int(self._stats["chars"] / 4.0) if self._stats["chars"] else 0
        approx_tps = (approx_tokens / elapsed_s) if elapsed_s > 0 else None
        payload = {
            "event": "llm.stats",
            "id": self.id,
            "sid": self.sid,
            "model": self.model,
            "phase": phase,
            "ts": _now_ms(),
            "elapsed_ms": int(elapsed_s * 1000.0) if elapsed_s else 0,
            "approx": {
                "chars": self._stats["chars"],
                "chars_per_sec": cps,
                "tokens": approx_tokens,
                "tokens_per_sec": approx_tps,
            },
            "ollama": {
                "eval_count": self._stats.get("eval_count"),
                "eval_duration_ms": int((self._stats.get("eval_duration_s") or 0.0) * 1000.0) or None,
                "tokens_per_sec": tps_real,
            },
            "net": {
                "last_acked": self.last_acked,
                "last_seq": self.seq,
                "unacked": max(0, self.seq - self.last_acked),
            },
        }
        self._dm(payload)

    def run(self):
        if self.client is None: return
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
                self._send_done({"mode":"non-stream","api":self.api})
                return

            gen, meta = self._invoke_stream(self.api)

            bulk_buf: list[str] = []
            last_flush = time.monotonic()

            def _bulk_flush():
                nonlocal bulk_buf, last_flush
                if bulk_buf:
                    self._send_coalesced_text("".join(bulk_buf))
                    bulk_buf = []
                    last_flush = time.monotonic()

            for part in gen:
                if self.cancelled: break
                now = time.monotonic()
                with self._lock:
                    unacked = self.seq - self.last_acked
                    ack_stale = (now - self._last_ack_change) > 0.8
                bulk_mode = (unacked >= self.BULK_UNACK_THRESHOLD) or ack_stale

                if bulk_mode:
                    delta = _extract_delta_from_part(self.api, part)
                    if delta:
                        bulk_buf.append(delta)
                        too_many = len(bulk_buf) >= self.BULK_MAX_PARTS
                        too_old  = (now - last_flush) * 1000.0 >= self.BULK_MAX_MS
                        if too_many or too_old:
                            _bulk_flush()
                        continue
                    else:
                        _bulk_flush()
                        self._send_chunk(part)
                else:
                    _bulk_flush()
                    self._send_chunk(part)

            if not self.cancelled:
                _bulk_flush()

            for _ in range(self.FINAL_SWEEPS):
                if self.cancelled: break
                self._sweep_resend_unacked()
                with self._lock:
                    if self.last_acked >= self.seq: break
                time.sleep(self.FINAL_GAP_SECS)

            self._emit_stats("final")
            self._send_done(meta or {"mode":"stream","api":self.api})

        except OllamaResponseError as e:
            self._send_error(f"Ollama error: {e.error}", kind=f"http_{getattr(e,'status_code', 'unknown')}")
        except Exception as e:
            self._send_error(f"{type(e).__name__}: {e}", kind="exception")

    def _invoke_once(self, api: str) -> dict:
        if api == "chat":
            if isinstance(self.kwargs.get("messages"), list):
                self.kwargs["messages"] = _massage_images_in_messages(
                    self.kwargs["messages"],
                    src_addr=self.src_addr,
                    log_prefix=f"{self.src_addr} → {self.model}"
                )
            return self.client.chat(model=self.model, stream=False, **self.kwargs)
        elif api == "generate":
            return self.client.generate(model=self.model, stream=False, **self.kwargs)
        elif api == "embed":
            return self.client.embed(model=self.model, **self.kwargs)
        elif api == "show":
            raw = self.client.show(self.model)
            return _slim_show_payload_keep_modelfile_no_license(raw)
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
            if isinstance(self.kwargs.get("messages"), list):
                self.kwargs["messages"] = _massage_images_in_messages(
                    self.kwargs["messages"],
                    src_addr=self.src_addr,
                    log_prefix=f"{self.src_addr} → {self.model}"
                )
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

# Advertised control-ops this node supports
CTRL_CAPS = {"models", "info", "peers", "caps"}

def _send_ctrl_caps(dst: str, req_id: Optional[str] = None):
    _dm(dst, {
        "event": "ctrl.caps",
        "id": req_id,
        "ts": _now_ms(),
        "version": 1,
        "supports": sorted(CTRL_CAPS)
    })

def _send_ctrl_peers(dst: str, detail: str = "count", req_id: Optional[str] = None):
    try:
        c = len(clients)
    except Exception:
        c = 0
    payload = {"event": "ctrl.peers", "id": req_id, "ts": _now_ms(), "count": c}
    det = (detail or "count").lower()
    if det == "addrs":
        try:
            payload["addrs"] = list(clients)
        except Exception:
            payload["addrs"] = []
    elif det == "geo":
        rows = []
        try:
            for addr in list(clients):
                p = peers_by_addr.get(addr, None) or {"lat": 0.0, "lon": 0.0, "ts": 0}
                rows.append({"addr": addr, "lat": p["lat"], "lon": p["lon"], "ts": p["ts"]})
        except Exception:
            pass
        payload["peers"] = rows
    _dm(dst, payload)

# ──────────────────────────────────────────────────────────────────────
# IX. DM handler (control + sessions + LLM + legacy globe)
# ──────────────────────────────────────────────────────────────────────
def _handle_dm(src_addr: str, body: dict):
    ev = (body.get("event") or "").strip()

    # Control
    if ev == "ctrl.ping":
        _dm(src_addr, {
            "event": "ctrl.pong",
            "id": body.get("id"),
            "sent": body.get("ts") or body.get("sent"),
            "ts": _now_ms(),
            "nknAddress": state.get("nkn_address"),
            "topicPrefix": state.get("topic_prefix"),
            "llmActive": len(_llm_streams),
            "sessions": len(SESSIONS),
        })
        return

    if not ev:
        _log("dm", f"from {src_addr}  event=<missing>  body={body}")
        return

    if ev == "ctrl.request":
        op = (body.get("op") or "").strip().lower()
        req_id = body.get("id")
        if op == "models":
            _send_models_list(src_addr, req_id)
            return
        elif op == "info":
            _dm(src_addr, {
                "event": "ctrl.info",
                "id": req_id,
                "ts": _now_ms(),
                "nknAddress": state.get("nkn_address"),
                "topicPrefix": state.get("topic_prefix"),
                "ollamaHost": OLLAMA_HOST
            })
            return
        elif op in ("caps", "hello", "capabilities"):
            _send_ctrl_caps(src_addr, req_id)
            return
        elif op == "peers":
            detail = (body.get("detail") or body.get("include") or "count")
            _send_ctrl_peers(src_addr, detail=detail, req_id=req_id)
            return
        else:
            _dm(src_addr, {
                "event": "ctrl.error",
                "id": req_id,
                "ts": _now_ms(),
                "op": op,
                "message": "unknown op",
                "supports": sorted(CTRL_CAPS)
            })
            return

    # BLOB ingest
    if ev.startswith("blob."):
        try: be = ev.split(".", 1)[1]
        except Exception: be = ""
        if be == "init":
            bid = str(body.get("id") or "")
            name = str(body.get("name") or "")
            mime = str(body.get("mime") or "")
            size = int(body.get("size") or 0)
            BLOB_MAX_SIZE = int(os.environ.get("BLOB_MAX_SIZE", str(200 * 1024 * 1024)))
            if size <= 0 or size > BLOB_MAX_SIZE:
                _dm(src_addr, {"event": "blob.error", "id": bid, "message": f"invalid size (max {BLOB_MAX_SIZE} bytes)"})
                return
            csz  = int(body.get("chunkSize") or 0)
            total= int(body.get("total") or 0)
            sha  = str(body.get("sha256") or "") or None
            if not (bid and size > 0 and csz > 0 and total > 0):
                _dm(src_addr, {"event":"blob.error","id":bid,"message":"invalid init"}); return
            key = _blob_key(src_addr, bid)
            with _BLOBS_LOCK:
                if key in _BLOBS:
                    meta = _BLOBS[key]; _blob_touch(meta)
                    resume_from = len(meta.get("received", set()))
                    _dm(src_addr, {"event":"blob.accept","id":bid,"resumeFrom":resume_from}); return
                tmp = _blob_tmp_path(bid)
                try:
                    with open(tmp, "wb") as f: f.truncate(size)
                except Exception as e:
                    _dm(src_addr, {"event":"blob.error","id":bid,"message":f"init write failed: {e}"}); return
                meta = {
                    "id": bid, "name": name, "mime": mime, "size": size,
                    "chunk": csz, "total": total, "received": set(),
                    "created_at": time.time(), "updated_at": time.time(),
                    "sha256_client": sha, "done": False
                }
                _BLOBS[key] = meta
            _dm(src_addr, {"event":"blob.accept","id":bid,"resumeFrom":0}); return

        if be == "part":
            bid = str(body.get("id") or "")
            seq = int(body.get("seq") or -1)
            data = body.get("data")
            key = _blob_key(src_addr, bid)
            with _BLOBS_LOCK: meta = _BLOBS.get(key)
            if not meta or seq < 0 or seq >= meta["total"]:
                _dm(src_addr, {"event":"blob.error","id":bid,"message":"bad part"}); return
            b = _decode_b64_to_bytes(data) if isinstance(data, str) else None
            if b is None:
                _dm(src_addr, {"event":"blob.error","id":bid,"message":"decode fail"}); return
            try:
                off = seq * meta["chunk"]
                with open(_blob_tmp_path(bid), "r+b") as f:
                    f.seek(off); f.write(b)
                with _BLOBS_LOCK:
                    meta["received"].add(seq); _blob_touch(meta)
                _dm(src_addr, {"event":"blob.ack","id":bid,"seq":seq})
            except Exception as e:
                _dm(src_addr, {"event":"blob.error","id":bid,"message":f"write fail: {e}"})
            return

        if be == "done":
            bid = str(body.get("id") or "")
            key = _blob_key(src_addr, bid)
            with _BLOBS_LOCK: meta = _BLOBS.get(key)
            if not meta:
                _dm(src_addr, {"event":"blob.error","id":bid,"message":"unknown id"}); return
            missing = [i for i in range(meta["total"]) if i not in meta["received"]]
            if missing:
                _dm(src_addr, {"event":"blob.error","id":bid,"message":f"missing parts: {len(missing)}"}); return
            try:
                tmp = _blob_tmp_path(bid); final = _blob_path(bid); tmp.replace(final)
                sha_hex = None
                try:
                    import hashlib; sha = hashlib.sha256()
                    with open(final, "rb") as f:
                        for chunk in iter(lambda: f.read(1024*1024), b""): sha.update(chunk)
                    sha_hex = sha.hexdigest()
                except Exception: pass
                with _BLOBS_LOCK:
                    meta["done"] = True; meta["sha256"] = sha_hex; _blob_touch(meta)
                _dm(src_addr, {
                    "event":"blob.ready","id": bid,"ref": f"blob:{bid}",
                    "name": meta["name"],"mime": meta["mime"],"size": meta["size"],"sha256": sha_hex
                })
            except Exception as e:
                _dm(src_addr, {"event":"blob.error","id":bid,"message":f"finalize fail: {e}"})
            return

        if be == "cancel":
            bid = str(body.get("id") or "")
            key = _blob_key(src_addr, bid)
            with _BLOBS_LOCK: meta = _BLOBS.pop(key, None)
            try:
                if meta:
                    _blob_tmp_path(bid).unlink(missing_ok=True)
                    _blob_path(bid).unlink(missing_ok=True)
            except Exception: pass
            _dm(src_addr, {"event":"blob.ack","id":bid,"seq":-1})
            return

        _dm(src_addr, {"event":"blob.error","id":body.get("id"),"message":"unknown blob op"})
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

        if api == "list":
            _send_models_list(src_addr, body.get("id"))
            _log("llm.list", f"{src_addr} id={body.get('id')} → models sent (stream={stream})")
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

            imgs_b64 = body.get("images") or body.get("images_b64")
            imgs_ref = body.get("images_ref")
            if (imgs_b64 or imgs_ref) and isinstance(req_messages, list) and req_messages:
                for j in range(len(req_messages) - 1, -1, -1):
                    if req_messages[j].get("role") == "user":
                        if imgs_b64: req_messages[j]["images_b64"] = imgs_b64
                        if imgs_ref: req_messages[j]["images_ref"] = imgs_ref
                        b64_ct = len(imgs_b64 or []); ref_ct = len(imgs_ref or [])
                        _log("vision.recv", f"📥 {src_addr} sid={sid} b64={b64_ct} blobs={ref_ct}")
                        break

            opts = _merge_options(s, (kwargs.get("options") or {}))
            call_kwargs = dict(kwargs); call_kwargs["messages"] = req_messages; call_kwargs["options"]  = opts

            def on_start():
                if stream: _start_assistant(src_addr, sid)

            def on_delta_fn(text:str):
                if text: _append_assistant_delta(src_addr, sid, text)

            def on_done_fn(final_text: Optional[str]):
                if not stream:
                    _start_assistant(src_addr, sid)
                    if final_text: _append_assistant_delta(src_addr, sid, final_text)
                _finish_assistant(src_addr, sid)

            req_body = {
                "event":"llm.request","api": api,"model": s.model,"stream": stream,
                "kwargs": call_kwargs,"id": body.get("id"),"sid": sid,
            }
            rid = _start_llm(src_addr, req_body, on_start=on_start, on_delta=on_delta_fn, on_done=on_done_fn)
            _log("llm.start", f"{src_addr} sid={sid} id={rid} api={api} model={s.model} stream={stream}")
            return
        else:
            top_msgs = body.get("messages")
            if isinstance(top_msgs, (list, tuple)):
                msgs = _sanitize_history(top_msgs)
                kw = body.get("kwargs") or {}
                kw["messages"] = msgs
                if "tools" in body and "tools" not in kw: kw["tools"] = body["tools"]
                if "tool_choice" in body and "tool_choice" not in kw: kw["tool_choice"] = body["tool_choice"]
                body["kwargs"] = kw
                body.pop("messages", None); body.pop("tools", None); body.pop("tool_choice", None)
            rid = _start_llm(src_addr, body)
            _log("llm.start", f"{src_addr} id={rid} api={api} model={model} stream={stream} (compat/messages={bool(top_msgs)})")
            return

    if ev == "llm.ack":
        _ack_llm(body); return

    if ev == "llm.cancel":
        _cancel_llm(body)
        _log("llm.cancel", f"{src_addr} id={body.get('id')}"); return

    # Legacy demo globe
    if ev == "ping":
        _dm(src_addr, {"event":"pong","ts":_now_ms()})
        _log("ping", f"{src_addr}"); return

    if ev == "join":
        st = _sanitize_state(body.get("state") or {})
        peers_by_addr[src_addr] = {"lat": st["lat"], "lon": st["lon"], "ts": _now_ms()}
        clients.add(src_addr)
        _log("join", f"{src_addr}  lat={st['lat']:.2f} lon={st['lon']:.2f}  clients={len(clients)}")
        _send_world_snapshot([src_addr]); return

    if ev == "state":
        st = _sanitize_state(body.get("state") or {})
        peers_by_addr[src_addr] = {"lat": st["lat"], "lon": st["lon"], "ts": _now_ms()}
        return

    if ev == "leave":
        peers_by_addr.pop(src_addr, None)
        clients.discard(src_addr)
        _log("leave", f"{src_addr}  clients={len(clients)}")
        _send_world_snapshot(); return

    if ev == "announce":
        peers_by_addr[src_addr] = {"lat": 0.0, "lon": 0.0, "ts": _now_ms()}
        clients.add(src_addr)
        _log("announce", f"{src_addr}")
        _send_world_snapshot([src_addr])
        # Present models like before (auto-push on announce)
        _send_models_list(src_addr, req_id=f"models:auto:{_now_ms()}")
        return

    if ev in ("frame-color", "frame-depth"):
        data = body.get("data"); size = 0
        if isinstance(data, str):
            try: size = len(base64.b64decode(data.encode("ascii"), validate=True))
            except Exception: size = len(data)
        _log(ev, f"{src_addr} bytes≈{size}"); return

    _log("dm-unknown", f"from {src_addr}  event={ev}  body={body}")

# ──────────────────────────────────────────────────────────────────────
# X. HTTP (metadata + models + bootstrap)
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

# --- Bootstrap oracle: provide rotating seedWsAddr + ICE servers ---
_BOOT_WS_SEEDS = [
    "wss://66-113-14-95.ipv4.nknlabs.io:30004",
    "wss://3-137-144-60.ipv4.nknlabs.io:30004",
    "wss://144-202-102-11.ipv4.nknlabs.io:30004",
    # add more here...
]
_boot_idx = 0
_boot_lock = threading.Lock()

def _rotate_ws_seeds(k=6):
    global _boot_idx
    with _boot_lock:
        if not _BOOT_WS_SEEDS:
            return []
        out = []
        n = len(_BOOT_WS_SEEDS)
        for i in range(min(k, n)):
            out.append(_BOOT_WS_SEEDS[(_boot_idx + i) % n])
        _boot_idx = (_boot_idx + 1) % n
        return out

_DEFAULT_ICE = (
    {
        "iceServers": [
            {"urls": "stun:stun.l.google.com:19302"},
            {"urls": "stun:global.stun.twilio.com:3478"},
            # {"urls": "turn:your.turn.server:3478", "username": "user", "credential": "pass"}
        ],
        "iceCandidatePoolSize": 2,
    }
    if not DISABLE_WEBRTC
    else {"iceServers": [], "iceCandidatePoolSize": 0}
)

@app.route("/bootstrap")
def bootstrap():
    try:
        k = int(request.args.get("k", "6"))
        if k < 2: k = 2
        if k > 16: k = 16
    except Exception:
        k = 6
    seeds = _rotate_ws_seeds(k)
    policy = "websocket-only" if DISABLE_WEBRTC else "auto"
    payload = {
        "ok": True,
        "nknAddress": state.get("nkn_address"),
        "topicPrefix": state.get("topic_prefix"),
        "seeds_ws": seeds,
        "seedWsAddr": seeds,
        "wsOnly": (policy == "websocket-only"),
        "rtc": _DEFAULT_ICE,
        "retry": {
            "reconnectIntervalMin": 1000,
            "reconnectIntervalMax": 10000,
            "wsConnHeartbeatTimeout": 120000,
            "connectTimeoutMs": 15000
        },
        "ts": _now_ms()
    }
    return jsonify(payload)

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
