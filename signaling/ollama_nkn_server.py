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
/* nkn_bridge.js — robust DM-only bridge for hub ⇄ clients
   - Always-enqueue, concurrent drain
   - Head coalescing of llm.chunk → llm.bulk (bounded by bytes/age/count)
   - Splits oversize bulks on size preflight or send error and retries
   - Hardened against WS flaps; watchdog heartbeats
   - Detailed crit.* reports (includes mb, q, stack)

   Drop-in replacement for BRIDGE_SRC.
*/
'use strict';

const nkn = require('nkn-sdk');
const readline = require('readline');

/* ────────────────────────── ENV / CONFIG ────────────────────────── */
const SEED_HEX   = (process.env.NKN_SEED_HEX || '').toLowerCase().replace(/^0x/, '');
const TOPIC_NS   = process.env.NKN_TOPIC_PREFIX || 'client';
const NUM_SUBCLIENTS = parseInt(process.env.NKN_NUM_SUBCLIENTS || '4', 10) || 4;
const SEED_WS = (process.env.NKN_BRIDGE_SEED_WS || '')
  .split(',').map(s => s.trim()).filter(Boolean);

/* Queue & memory */
const MAX_QUEUE   = parseInt(process.env.NKN_MAX_QUEUE || '4000', 10);
const HEAP_SOFT   = parseInt(process.env.NKN_HEAP_SOFT_MB || '640', 10);
const HEAP_HARD   = parseInt(process.env.NKN_HEAP_HARD_MB || '896', 10);

// Drain behavior
const DRAIN_BASE_BATCH = parseInt(process.env.NKN_DRAIN_BASE || '192', 10);
const DRAIN_MAX_BATCH  = parseInt(process.env.NKN_DRAIN_MAX_BATCH || '1536', 10);
const CATCHUP_AGE_MS   = parseInt(process.env.NKN_CATCHUP_AGE_MS || '500', 10);
// 🔧 was '1' — this starves the pump
const SEND_CONCURRENCY = parseInt(process.env.NKN_SEND_CONC || '16', 10);

// Coalescing / payload sizing
const COALESCE_HEAD       = (process.env.NKN_COALESCE_HEAD || '0') !== '0';
const COALESCE_MAX_COUNT  = parseInt(process.env.NKN_COALESCE_MAX_COUNT || '9999999', 10);
// keep safe < 1MB including envelope; 900k avoids re-sends on some relays
const COALESCE_MAX_BYTES  = parseInt(process.env.NKN_COALESCE_MAX_BYTES || '900000', 10);
// 🔧 widen the window; 40ms is too twitchy
const COALESCE_MAX_AGE_MS = parseInt(process.env.NKN_COALESCE_MAX_AGE_MS || '250', 10);
// go "force" coalesce on backlog even if head is fresh
const COALESCE_BACKLOG_TRIGGER = parseInt(process.env.NKN_COALESCE_BACKLOG || '256', 10);
const DYNAMIC_COALESCE_LAT_MS = parseInt(process.env.NKN_COALESCE_LAT_MS || '1000', 10);

// Safety cap for NKN payloads; envelope headroom for JSON
const MAX_PAYLOAD_BYTES = Number(process.env.NKN_MAX_PAYLOAD_BYTES || 900_000);
const ENVELOPE_OVERHEAD = 512;

/* Send defaults */
const SEND_OPTS = { noReply: true, maxHoldingSeconds: 120 };

/* ────────────────────────── STATE ────────────────────────── */
let clientRef = null;
let READY = false;
let draining = false;
let suspendUntil = 0; // pause drain until ts (on WS flap)
const queue = [];     // items: { to, data } or { isPub:true, topic, data }

/* ────────────────────────── UTIL ────────────────────────── */
function sendToPy(obj) {
  try { process.stdout.write(JSON.stringify(obj) + '\n'); } catch { /* ignore */ }
}
function log(...args) { console.error('[bridge]', ...args); }
function heapMB() { const m = process.memoryUsage(); return Math.round((m.rss || 0) / 1024 / 1024); }
function rateLimit(fn, ms) { let last = 0; return (...a) => { const t = Date.now(); if (t - last > ms) { last = t; fn(...a); } }; }
const logSendErr = rateLimit((msg) => log('send warn', msg), 1500);

function suspend(ms = 2000) { suspendUntil = Date.now() + ms; READY = false; }

/* Safe stringify size estimator */
function jsonSizeBytes(obj) {
  try { return Buffer.byteLength(JSON.stringify(obj), 'utf8'); }
  catch { return 0; }
}

/* ────────────────────────── ENQUEUE ────────────────────────── */
function enqueue(item) {
  if (!item || typeof item !== 'object') return;
  if (queue.length >= MAX_QUEUE) {
    // Drop oldest 10% to survive bursts
    const drop = queue.splice(0, Math.ceil(MAX_QUEUE * 0.10));
    sendToPy({ type: 'queue.drop', drop: drop.length, q: queue.length });
  }
  // Stamp generator time for adaptive coalescing/catch-up
  if (!item.data) item.data = {};
  if (!item.data.gen_ts) item.data.gen_ts = item.data.ts || Date.now();

  queue.push(item);
  if (READY && Date.now() >= suspendUntil) kickDrain();
}

/* ────────────────────────── COALESCE HEAD ──────────────────────────
   Merge consecutive llm.chunk for the same stream/to into one llm.bulk.
   Under "severe" backlog (old head or long queue), we relax caps a bit.
--------------------------------------------------------------------- */
function tryCoalesceHead(force = false) {
  if (!COALESCE_HEAD && !force) return null;
  if (queue.length < 2) return null;

  const first = queue[0];
  if (!first) return null;
  const d0 = first.data || {};
  if (d0.event !== 'llm.chunk') return null;

  const to = first.to;
  const streamId = d0.id;
  const startTs = d0.gen_ts || Date.now();

  const countLimit = force ? Number.POSITIVE_INFINITY : COALESCE_MAX_COUNT;
const hardCap = Math.min(COALESCE_MAX_BYTES || MAX_PAYLOAD_BYTES, MAX_PAYLOAD_BYTES - ENVELOPE_OVERHEAD);
const byteLimit = force ? hardCap : hardCap;

  let bytes = jsonSizeBytes(d0);
  let count = 1;
  const items = [d0];

  for (let i = 1; i < queue.length && count < countLimit; i++) {
    const it = queue[i]; if (!it || it.to !== to) break;
    const dx = it.data || {};
    if (dx.event !== 'llm.chunk' || dx.id !== streamId) break;

    const sz = jsonSizeBytes(dx);
    const projected = bytes + sz + ENVELOPE_OVERHEAD;
    if (!force && projected > byteLimit) break;

    if (!force) {
      const age = Date.now() - startTs;
      if (age > COALESCE_MAX_AGE_MS) break;
    }

    items.push(dx);
    bytes += sz;
    count++;
  }

  if (count <= 1) return null;

  // Remove merged items from head
  queue.splice(0, count);

  const last = items[items.length - 1];
  return {
    to,
    data: {
      event: 'llm.bulk',
      id: streamId,
      items,                             // original llm.chunk payloads
      first_seq: items[0]?.seq,
      last_seq: last?.seq,
      count: items.length,
      seq: last?.seq,                    // helps simple clients that just read .seq
      ts: Date.now(),
      gen_ts: d0.gen_ts
    }
  };
}

/* ────────────────────────── SEND HELPERS ────────────────────────── */
async function safeSendDM(client, to, data) {
  try {
    // Preflight size protection to avoid NKN/WS close on oversize
    const bytes = jsonSizeBytes(data);
    if (bytes > MAX_PAYLOAD_BYTES) {
      if (data?.event === 'llm.bulk' && Array.isArray(data.items) && data.items.length > 1) {
        // Split in half and re-enqueue both parts
        const mid = Math.floor(data.items.length / 2);
        const aItems = data.items.slice(0, mid);
        const bItems = data.items.slice(mid);
        const a = { ...data, items: aItems, first_seq: aItems[0]?.seq, last_seq: aItems[aItems.length - 1]?.seq, count: aItems.length, seq: aItems[aItems.length - 1]?.seq };
        const b = { ...data, items: bItems, first_seq: bItems[0]?.seq, last_seq: bItems[bItems.length - 1]?.seq, count: bItems.length, seq: bItems[bItems.length - 1]?.seq };
        enqueue({ to, data: a });
        enqueue({ to, data: b });
        return true;
      }
      // If a single chunk exceeds limit (rare), fall through and try anyway.
    }

    await client.send(to, JSON.stringify(data), SEND_OPTS);
    return true;
  } catch (e) {
    const msg = (e && e.message) || String(e || '');
    // If bulk failed, split and retry
    if (data?.event === 'llm.bulk' && Array.isArray(data.items) && data.items.length > 1) {
      const mid = Math.floor(data.items.length / 2);
      const aItems = data.items.slice(0, mid);
      const bItems = data.items.slice(mid);
      const a = { ...data, items: aItems, first_seq: aItems[0]?.seq, last_seq: aItems[aItems.length - 1]?.seq, count: aItems.length, seq: aItems[aItems.length - 1]?.seq };
      const b = { ...data, items: bItems, first_seq: bItems[0]?.seq, last_seq: bItems[bItems.length - 1]?.seq, count: bItems.length, seq: bItems[bItems.length - 1]?.seq };
      enqueue({ to, data: a });
      enqueue({ to, data: b });
      return false;
    }
    logSendErr(`${to} → ${msg}`);
    if (msg.includes('WebSocket unexpectedly closed') || msg.includes('socket hang up')) {
      suspend(2500);
      sendToPy({ type: 'wsclosed', reason: msg, ts: Date.now() });
      setTimeout(() => { if (READY) kickDrain(); }, 2600);
    }
    return false;
  }
}
async function safePub(client, topic, data) {
  try {
    await client.publish(TOPIC_NS + '.' + topic, JSON.stringify(data));
    return true;
  } catch (e) {
    const msg = (e && e.message) || String(e || '');
    logSendErr(`pub ${topic} → ${msg}`);
    if (msg.includes('WebSocket unexpectedly closed') || msg.includes('socket hang up')) {
      suspend(2500);
      sendToPy({ type: 'wsclosed', reason: msg, ts: Date.now() });
      setTimeout(() => { if (READY) kickDrain(); }, 2600);
    }
    return false;
  }
}

/* ────────────────────────── DRAIN LOOP ────────────────────────── */
async function drain(client) {
  if (!READY || Date.now() < suspendUntil || queue.length === 0) return;

  const now = Date.now();
  const headTs = (queue[0] && queue[0].data && (queue[0].data.gen_ts || queue[0].data.ts)) || now;
  const age = Math.max(0, now - headTs);
  const backlog = queue.length;
  // 🔧 previously only age; add backlog trigger
  const severe = (age >= DYNAMIC_COALESCE_LAT_MS) || (backlog >= COALESCE_BACKLOG_TRIGGER);


  // Adaptive take size
  let take = DRAIN_BASE_BATCH;
  if (backlog > 256 || age > CATCHUP_AGE_MS) take = Math.min(DRAIN_MAX_BATCH, DRAIN_BASE_BATCH * 2);
  if (backlog > 512 || age > 2 * CATCHUP_AGE_MS) take = Math.min(DRAIN_MAX_BATCH, Math.floor(DRAIN_BASE_BATCH * 3));
  if (backlog > 1024 || age > 3 * CATCHUP_AGE_MS) take = DRAIN_MAX_BATCH;
  if (severe) take = DRAIN_MAX_BATCH; // go wide under pressure

  const batch = [];
  while (batch.length < take && queue.length > 0) {
    const merged = tryCoalesceHead(severe);
    if (merged) { batch.push(merged); continue; }
    batch.push(queue.shift());
  }

  // Higher parallelism while catching up hard
  const workers = severe ? SEND_CONCURRENCY
                         : Math.min(SEND_CONCURRENCY, Math.max(1, Math.ceil(batch.length / 8)));

  let idx = 0;
  async function worker() {
    while (true) {
      const i = idx++; if (i >= batch.length) break;
      const it = batch[i];
      const ok = it.isPub ? await safePub(client, it.topic, it.data)
                          : await safeSendDM(client, it.to, it.data);
      if (!ok) queue.unshift(it); // retry after transient flap
    }
  }
  await Promise.all(new Array(workers).fill(0).map(() => worker()));
}

async function kickDrain() {
  if (draining) return;
  draining = true;
  try {
    while (queue.length > 0 && READY && Date.now() >= suspendUntil) {
      await drain(clientRef);
      const q = queue.length;

      // If head very old, don't sleep at all
      const head = queue[0];
      const headAge = head && head.data ? (Date.now() - (head.data.gen_ts || head.data.ts || Date.now())) : 0;
      const severe = headAge >= DYNAMIC_COALESCE_LAT_MS;

      if (severe) { await new Promise(r => setImmediate(r)); continue; }

      const wait = q > 1500 ? 0 : q > 300 ? 1 : 3;
      if (wait > 0) await new Promise(r => setTimeout(r, wait));
      else await new Promise(r => setImmediate(r));
    }
  } finally {
    draining = false;
  }
}

/* ────────────────────────── BOOTSTRAP ────────────────────────── */
(async () => {
  try {
    if (!/^[0-9a-f]{64}$/.test(SEED_HEX)) {
      sendToPy({ type: 'crit.config', msg: 'Invalid NKN_SEED_HEX (need 64 hex chars)', mb: heapMB() });
      process.exit(1);
      return;
    }

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
      READY = true;
      sendToPy({ type: 'ready', address: client.addr, topicPrefix: TOPIC_NS, ts: Date.now() });
      log('ready at', client.addr);
      kickDrain();
    });

    client.on('error', (e) => {
      const msg = (e && e.message) || String(e || '');
      log('client error', msg);
      if (msg.includes('WebSocket unexpectedly closed') || msg.includes('VERIFY SIGNATURE ERROR')) {
        sendToPy({ type: 'wsclosed', reason: msg, ts: Date.now() });
        suspend(2500);
        setTimeout(() => { if (READY) kickDrain(); }, 2600);
      }
    });

    client.on('connectFailed', (e) => {
      log('connect failed', (e && e.message) || e);
    });

    const onMessage = (a, b) => {
      let src, payload;
      if (a && typeof a === 'object' && a.payload !== undefined) { src = a.src; payload = a.payload; }
      else { src = a; payload = b; }
      try {
        const asStr = Buffer.isBuffer(payload) ? payload.toString('utf8') : String(payload || '');
        try {
          const msg = JSON.parse(asStr);
          sendToPy({ type: 'nkn-dm', src, msg });
        } catch {
          // Non-JSON message; ignore silently (or forward raw if desired)
        }
      } catch { /* ignore */ }
    };
    client.on('message', onMessage);

    // ALWAYS enqueue from stdin; drain handles sending with concurrency
    const rl = readline.createInterface({ input: process.stdin });
    rl.on('line', (line) => {
      if (!line) return;
      let cmd;
      try { cmd = JSON.parse(line); } catch { return; }

      if (cmd && cmd.type === 'dm' && cmd.to && cmd.data) {
        if (typeof cmd.data === 'object' && cmd.data && !cmd.data.ts) cmd.data.ts = Date.now();
        enqueue({ to: cmd.to, data: cmd.data });
      } else if (cmd && cmd.type === 'pub' && cmd.topic && cmd.data) {
        if (typeof cmd.data === 'object' && cmd.data && !cmd.data.ts) cmd.data.ts = Date.now();
        enqueue({ isPub: true, topic: cmd.topic, data: cmd.data });
      }
    });
    rl.on('close', () => { process.exit(0); });

    // Heap guard: soft trim, then hard exit (watchdog restarts)
    setInterval(() => {
      const mb = heapMB();
      if (mb >= HEAP_SOFT) {
        try { if (global.gc) global.gc(); } catch {}
        if (queue.length > Math.floor(MAX_QUEUE * 0.6)) {
          const drop = queue.splice(0, Math.max(1, queue.length - Math.floor(MAX_QUEUE * 0.6)));
          sendToPy({ type: 'queue.trim', drop: drop.length, q: queue.length, mb });
        }
      }
      if (mb >= HEAP_HARD) {
        sendToPy({ type: 'crit.heap', mb, q: queue.length });
        process.exitCode = 137;
        setTimeout(() => process.exit(137), 10);
      }
    }, 2000);

    // Bridge heartbeat to Python watchdog
    setInterval(() => {
      sendToPy({ type: 'hb', ts: Date.now(), ready: READY, q: queue.length, mb: heapMB() });
    }, 5000);

    // Safety net: if anything left in queue and we're READY, keep nudging the pump
    setInterval(() => {
      if (queue.length && READY && Date.now() >= suspendUntil) kickDrain();
    }, 25);

    // Graceful shutdown
    const shutdown = (sig) => {
      try { sendToPy({ type: 'shutdown', sig, q: queue.length, mb: heapMB(), ts: Date.now() }); } catch {}
      try { rl.close(); } catch {}
      try { client && client.close && client.close(); } catch {}
      setTimeout(() => process.exit(0), 20);
    };
    process.on('SIGINT', () => shutdown('SIGINT'));
    process.on('SIGTERM', () => shutdown('SIGTERM'));

    // Fail fast on unexpected errors — INCLUDE mb/q/stack so Python won’t show mb=None
    process.on('uncaughtException', (e) => {
      sendToPy({
        type: 'crit.uncaught',
        msg: String(e && e.message || e),
        stack: String(e && e.stack || ''),
        mb: heapMB(),
        q: queue.length,
        ts: Date.now()
      });
      process.exit(1);
    });
    process.on('unhandledRejection', (e) => {
      sendToPy({
        type: 'crit.unhandled',
        msg: String((e && e.message) || e),
        stack: String((e && e.stack) || ''),
        mb: heapMB(),
        q: queue.length,
        ts: Date.now()
      });
      process.exit(1);
    });

  } catch (bootErr) {
    sendToPy({
      type: 'crit.boot',
      msg: String(bootErr && bootErr.message || bootErr),
      stack: String(bootErr && bootErr.stack || ''),
      mb: heapMB(),
      ts: Date.now()
    });
    process.exit(1);
  }
})();
"""
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

# Refresh bridge source if needed
if not BRIDGE_JS.exists() or BRIDGE_JS.read_text() != BRIDGE_SRC:
    BRIDGE_JS.write_text(BRIDGE_SRC)

# ── Bridge env (tuned for backlog coalescing) ─────────────────────────
bridge_env = os.environ.copy()
bridge_env["NKN_SEED_HEX"]       = NKN_SEED_HEX
bridge_env["NKN_TOPIC_PREFIX"]   = TOPIC_PREFIX
bridge_env["NKN_NUM_SUBCLIENTS"] = str(NKN_NUM_SUBCLIENTS)

# Queue & memory
bridge_env["NKN_MAX_QUEUE"]     = os.environ.get("NKN_MAX_QUEUE", "4000")
bridge_env["NKN_HEAP_SOFT_MB"]  = os.environ.get("NKN_HEAP_SOFT_MB", "640")
bridge_env["NKN_HEAP_HARD_MB"]  = os.environ.get("NKN_HEAP_HARD_MB", "896")

# Drain behavior (moderate parallelism; avoid per-send thrash)
bridge_env["NKN_DRAIN_BASE"]       = os.environ.get("NKN_DRAIN_BASE", "192")
bridge_env["NKN_DRAIN_MAX_BATCH"]  = os.environ.get("NKN_DRAIN_MAX_BATCH", "2048")
bridge_env["NKN_CATCHUP_AGE_MS"]   = os.environ.get("NKN_CATCHUP_AGE_MS", "500")
bridge_env["NKN_SEND_CONC"]        = os.environ.get("NKN_SEND_CONC", "4")   # ↓ from 16

# Coalescing — IMPORTANT:
#  - Remove age as a limiter (set huge), so backlog can merge immediately.
#  - Enter “severe” (force merge) quickly so we bulk on backlog instead of dribbling.
bridge_env["NKN_COALESCE_HEAD"]       = os.environ.get("NKN_COALESCE_HEAD", "1")
bridge_env["NKN_COALESCE_MAX_COUNT"]  = os.environ.get("NKN_COALESCE_MAX_COUNT", "9999999")
bridge_env["NKN_COALESCE_MAX_BYTES"]  = os.environ.get("NKN_COALESCE_MAX_BYTES", "720000")   # ~700–720 KB
bridge_env["NKN_COALESCE_MAX_AGE_MS"] = os.environ.get("NKN_COALESCE_MAX_AGE_MS", "10000")   # effectively off
bridge_env["NKN_COALESCE_LAT_MS"]     = os.environ.get("NKN_COALESCE_LAT_MS", "80")          # severe much sooner

# Payload cap with JSON headroom; keep below NKN/WS 1MB
bridge_env["NKN_MAX_PAYLOAD_BYTES"] = os.environ.get("NKN_MAX_PAYLOAD_BYTES", "760000")

# Node options
node_opts = bridge_env.get("NODE_OPTIONS", "")
extra = "--unhandled-rejections=strict --heapsnapshot-signal=SIGUSR2 --max-old-space-size=1024"
bridge_env["NODE_OPTIONS"] = (node_opts + " " + extra).strip()

# Bridge process + watchdog state (unchanged)
bridge = None
state: Dict[str, Any] = {"nkn_address": None, "topic_prefix": TOPIC_PREFIX}
_last_hb = 0.0
_ready_at = 0.0
_bridge_starts = 0
_bridge_lock = threading.Lock()
_bridge_write_lock = threading.Lock()
_last_restart_ts = 0.0
_FORCE_RESTART = False
_CRIT_ERR_PATTERNS = (
    "FATAL ERROR: Ineffective mark-compacts",
    "JavaScript heap out of memory",
    "FatalProcessOutOfMemory",
    "node::Abort()",
    "v8::internal::V8::FatalProcessOutOfMemory",
)
_ws_flap_times: List[float] = []
_ws_flap_lock = threading.Lock()
WS_FLAP_WINDOW = 60.0
WS_FLAP_MAX = 6

def _spawn_bridge() -> Popen:
    global _last_hb, _ready_at, _bridge_starts
    _last_hb = time.time()
    _ready_at = 0.0
    _bridge_starts += 1
    proc = Popen(
        [str(shutil.which("node")), str(BRIDGE_JS)],
        cwd=NODE_DIR, env=bridge_env,
        stdin=PIPE, stdout=PIPE, stderr=PIPE,
        text=True, bufsize=1
    )
    setattr(proc, "start_time", time.time())
    try:
        print("→ bridge env:",
              f"queue={bridge_env['NKN_MAX_QUEUE']}, conc={bridge_env['NKN_SEND_CONC']}, "
              f"bulk_max={bridge_env['NKN_COALESCE_MAX_BYTES']}, "
              f"cap={bridge_env['NKN_MAX_PAYLOAD_BYTES']}, "
              f"coalesce_age={bridge_env['NKN_COALESCE_MAX_AGE_MS']}, "
              f"severe_lat={bridge_env['NKN_COALESCE_LAT_MS']}")
    except Exception:
        pass
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

            t = msg.get("type")
            if t == "ready":
                addr = msg.get("address")
                if state.get("nkn_address") == addr and _ready_at > 0:
                    continue
                state["nkn_address"]  = addr
                state["topic_prefix"] = msg.get("topicPrefix") or TOPIC_PREFIX
                _ready_at = time.time()
                print(f"→ NKN ready: {state['nkn_address']}  (topics prefix: {state['topic_prefix']})")

            elif t == "hb":
                _last_hb = time.time()
                try:
                    state["bridge_ready"] = bool(msg.get("ready"))
                    state["bridge_q"]     = int(msg.get("q") or 0)
                    state["bridge_mb"]    = int(msg.get("mb") or 0)
                    state["bridge_hb_ts"] = int(msg.get("ts") or _now_ms())
                except Exception:
                    pass

            elif t == "wsclosed":
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

            elif t == "nkn-dm":
                src = msg.get("src") or ""
                body = msg.get("msg") or {}
                _handle_dm(src, body)

            elif t in ("queue.drop", "queue.trim"):
                mb = msg.get("mb")
                if mb is not None:
                    print(f"⚠️ bridge backpressure: {t} drop={msg.get('drop')} q={msg.get('q')} mb={mb}")
                else:
                    print(f"⚠️ bridge backpressure: {t} drop={msg.get('drop')} q={msg.get('q')}")

            elif t in ("crit.heap", "crit.uncaught", "crit.unhandled"):
                print(f"‼️ bridge critical: {t} mb={msg.get('mb')} q={msg.get('q')}")
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
    # ── timing / windows ─────────────────────────────────────────────
    RESEND_INTERVAL = 0.75                 # seconds between resend sweeps
    WINDOW_LIMIT    = 1024                 # soft eviction threshold
    MAX_WINDOW_PKTS = 4096                 # hard safety cap (no abort on hit)
    FINAL_SWEEPS    = 3
    FINAL_GAP_SECS  = 0.12
    ACK_STALL_SECS   = 25.0                # “overall” stall (for watchdog)
    SEVERE_ACK_STALE_SECS = 2.0            # ↓ enter panic sooner

    # ── coalescing thresholds (normal/panic) ─────────────────────────
    BULK_UNACK_THRESHOLD = 6               # ↓ start bulking quickly
    BULK_MAX_MS          = 90              # normal flush cadence (ms)
    BULK_MAX_PARTS       = 300             # normal max parts per flush
    BULK_PANIC_MS        = 120             # panic cadence (ms)
    BULK_PANIC_PARTS     = 9999            # huge bulks while behind

    # ── freeze (hold-then-burst) gating ──────────────────────────────
    FREEZE_E2E_MS         = int(os.environ.get("LLM_FREEZE_E2E_MS", "1500"))
    FREEZE_UNACK          = int(os.environ.get("LLM_FREEZE_UNACK", "256"))
    FREEZE_ACK_STALE_S    = float(os.environ.get("LLM_FREEZE_ACK_STALE_S", "1.2"))
    FREEZE_RENDERED_GAP   = int(os.environ.get("LLM_FREEZE_RENDERED_GAP", "128"))
    FREEZE_MAX_HOLD_S     = float(os.environ.get("LLM_FREEZE_MAX_HOLD_S", "0.25"))
    FREEZE_MAX_BYTES      = int(os.environ.get("LLM_FREEZE_MAX_BYTES", str(2 * 1024 * 1024)))

    # ── resend tail-bulk controls ────────────────────────────────────
    RESEND_TAIL_MAX       = int(os.environ.get("LLM_RESEND_TAIL_MAX", "256"))   # resend at most last N
    RESEND_BULK_MAX_BYTES = int(os.environ.get("LLM_RESEND_BULK_MAX_BYTES", str(700_000)))  # ~< 1MB
    DONE_RESEND_MS       = float(os.environ.get("LLM_DONE_RESEND_MS", "350"))  # resend llm.done cadence
    DONE_EXIT_GRACE_S    = float(os.environ.get("LLM_DONE_EXIT_GRACE_S", "0")) # 0 = wait forever

    STATS_INTERVAL_SECS = 0.5

    _ALLOWED_CHAT_KW = {"messages", "format", "options", "keep_alive", "tools", "tool_choice"}
    _ALLOWED_GEN_KW  = {"prompt", "images", "options", "keep_alive", "template", "system"}

    def __init__(self, src_addr: str, req: dict,
                 on_start=None, on_delta=None, on_done=None):
        self.src_addr = src_addr
        self.id       = req.get("id") or secrets.token_hex(8)
        self.api      = (req.get("api") or "chat").strip().lower()
        self.model    = req.get("model")
        self.think   = bool(req.get("think", False))
        self.stream   = bool(req.get("stream", False))
        self.client_cfg = req.get("client") or {}
        self.kwargs   = req.get("kwargs") or {}

        self.seq      = 0
        self.last_acked = 0
        self._last_ack_change = time.monotonic()

        self.last_recv_seq = 0
        self.last_rendered_seq = 0
        self._last_recv_change = time.monotonic()
        self._e2e_latency_ema_ms = None   # client_now - pkt.ts (ms)
        self._ack_rtt_ema_ms = None       # server_now - pkt.ts (fallback if client_now missing)

        self.window   = {}                # seq -> pkt (llm.chunk)
        self.window_order = []            # seq in send order
        self.done_sent  = False
        self.done_seq   = 0
        self.cancelled  = False
        self._lock    = threading.Lock()
        self._resender = threading.Thread(target=self._resend_loop, daemon=True)

        # callbacks
        self.on_start = on_start
        self.on_delta = on_delta
        self.on_done  = on_done
        self.done_sent       = False
        self.done_seq        = 0
        self.done_seen       = False         # ← set True only when client acks *done*
        self._last_done_send = 0.0
        self._first_done_at  = 0.0
        
        self.sid     = self.client_cfg.get("sid") or self.kwargs.get("sid") or req.get("sid")

        # stats book
        self._stats = {
            "t0": None, "tlast": None, "chars": 0,
            "eval_count": None, "eval_duration_s": None,
        }
        self._last_stats_emit = 0.0

        # client / Ollama
        host = self.client_cfg.get("host") or OLLAMA_HOST
        headers = self.client_cfg.get("headers") or {}
        try:
            self.client = OllamaClient(host=host, headers=headers)
        except Exception as e:
            self.client = None
            self._send_error(f"ollama client init failed: {e}", kind="client_init")

        # ── escalation hint overrides from client ACKs ────────────────
        self._panic_until = 0.0
        self._force_freeze_until = 0.0
        self._bulk_max_ms_override = None   # type: Optional[int]
        self._bulk_parts_override = None    # type: Optional[int]

    # ────────────────────────────────────────────────────────────────
    # DM helpers
    # ────────────────────────────────────────────────────────────────
    def _dm(self, payload: dict):
        _dm(self.src_addr, payload)

    def _send_start(self):
        self._dm({"event":"llm.start","id":self.id,"api":self.api,"model":self.model,"stream":self.stream,"think": bool(getattr(self, "think", False)),})
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
        """
        Send a single llm.chunk and track it in the window for resend/bulk replay.
        """
        with self._lock:
            self.seq += 1
            seq = self.seq
            pkt = {"event":"llm.chunk","id":self.id,"seq":seq,"data":_to_jsonable2(data),"ts":_now_ms()}
            self.window[seq] = pkt
            self.window_order.append(seq)
            # Soft eviction (only for already-acked seqs)
            if len(self.window_order) > self.WINDOW_LIMIT:
                oldest = self.window_order[0]
                if oldest <= self.last_acked:
                    self.window.pop(oldest, None)
                    self.window_order.pop(0)
            # NO aborts on window growth; we rely on bulk/freeze/resend-tail to depressurize
            self._dm(pkt)

        # stats + callbacks
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

    def _send_bulk(self, seqs: list[int]):
        """
        Send a single llm.bulk with the specified seqs (must exist in window).
        Items are the original llm.chunk envelopes.
        """
        if not seqs:
            return
        items = []
        total_bytes = 0
        for s in seqs:
            pkt = self.window.get(s)
            if not pkt:
                continue
            # cheap size estimate
            try:
                b = len(json.dumps(pkt).encode("utf-8"))
            except Exception:
                b = 0
            if self.RESEND_BULK_MAX_BYTES and (total_bytes + b) > self.RESEND_BULK_MAX_BYTES:
                break
            items.append(pkt)
            total_bytes += b

        if not items:
            return

        first = items[0]["seq"]; last = items[-1]["seq"]
        self._dm({
            "event": "llm.bulk",
            "id": self.id,
            "items": items,
            "first_seq": first,
            "last_seq": last,
            "ts": _now_ms()
        })

    def _sweep_resend_unacked_tail_bulk(self, force=False):
        with self._lock:
            start = self.last_acked + 1
            end   = self.seq
            if end < start and not (self.done_sent and not self.done_seen):
                return
            if end >= start:
                unacked = end - start + 1
                if not force and unacked <= 2:
                    pass
                else:
                    take = min(self.RESEND_TAIL_MAX, unacked)
                    seqs = list(range(end - take + 1, end + 1))
                    self._send_bulk(seqs)

            # also re-send the done frame if it hasn't been seen
            if self.done_sent and not self.done_seen:
                # small guard to avoid spamming at microsecond intervals
                if (time.monotonic() - self._last_done_send) * 1000.0 >= self.DONE_RESEND_MS:
                    self._send_done({"mode": "stream", "api": self.api, "retry": True})


    def _send_done(self, meta: dict):
        with self._lock:
            if not self.done_sent:
                self.done_seq = self.seq
                self.done_sent = True
                self._first_done_at = time.monotonic()
            frame = {
                "event": "llm.done",
                "id": self.id,
                "last_seq": self.done_seq,
                "meta": meta,
                "ts": _now_ms(),
            }
            self._dm(frame)
            self._last_done_send = time.monotonic()


    # ────────────────────────────────────────────────────────────────
    # ACK + pressure hints from client
    # ────────────────────────────────────────────────────────────────
    def ack(self, upto: int, meta: Optional[dict] = None):
        client_now = None
        rendered = None
        pressure = None
        panic_for_ms = 0
        bulk_max_ms = None
        bulk_parts = None
        if isinstance(meta, dict):
            client_now   = meta.get("client_now")
            rendered     = meta.get("rendered")
            pressure     = meta.get("pressure")
            panic_for_ms = int(meta.get("panic_for_ms", 0) or 0)
            bulk_max_ms  = meta.get("bulk_max_ms")
            bulk_parts   = meta.get("bulk_parts")
            seen_done    = bool(meta.get("seen_done"))           # ← NEW

        with self._lock:
            # track receive progress
            if upto > self.last_recv_seq:
                self.last_recv_seq = upto
                self._last_recv_change = time.monotonic()

            # delivery ACK (kept for eviction/backpressure logic)
            if upto > self.last_acked:
                self.last_acked = upto
                self._last_ack_change = time.monotonic()

            # compute latency using the packet timestamp we sent
            pkt = self.window.get(upto)
            pkt_ts = pkt.get("ts") if isinstance(pkt, dict) else None
            now_ms = int(time.time() * 1000)

            # E2E with client clock
            if pkt_ts and isinstance(client_now, (int, float)):
                sample = max(0, float(client_now) - float(pkt_ts))
                self._e2e_latency_ema_ms = sample if self._e2e_latency_ema_ms is None \
                    else (0.8 * self._e2e_latency_ema_ms + 0.2 * sample)

            # RTT-ish fallback
            if pkt_ts:
                sample_rtt = max(0, now_ms - int(pkt_ts))
                self._ack_rtt_ema_ms = sample_rtt if self._ack_rtt_ema_ms is None \
                    else (0.8 * self._ack_rtt_ema_ms + 0.2 * sample_rtt)

            # rendered progress (optional)
            if isinstance(rendered, int) and rendered > self.last_rendered_seq:
                self.last_rendered_seq = rendered

            if seen_done:
                self.done_seen = True

            # evict anything <= max(received, acked)
            hi = max(self.last_recv_seq, self.last_acked)
            for s in [s for s in list(self.window.keys()) if s <= hi]:
                self.window.pop(s, None)
            self.window_order = [s for s in self.window_order if s > hi]

            # pressure hints
            if pressure in ("panic", "bulk"):
                dur_s = (panic_for_ms or 2500) / 1000.0
                self._panic_until = max(self._panic_until, time.monotonic() + dur_s)
            elif pressure == "freeze":
                dur_s = (panic_for_ms or 2000) / 1000.0
                self._force_freeze_until = max(self._force_freeze_until, time.monotonic() + dur_s)

            if isinstance(bulk_max_ms, (int, float)) and bulk_max_ms > 0:
                self._bulk_max_ms_override = int(bulk_max_ms)
            if isinstance(bulk_parts, int) and bulk_parts > 0:
                self._bulk_parts_override = int(bulk_parts)

    def cancel(self):
        with self._lock:
            self.cancelled = True

    # ────────────────────────────────────────────────────────────────
    # Resender: tail-bulk instead of flooding
    # ────────────────────────────────────────────────────────────────
    def _resend_loop(self):
        while True:
            if self.cancelled:
                return
            time.sleep(self.RESEND_INTERVAL)
            now = time.monotonic()
            with self._lock:
                ack_stalled  = (now - self._last_ack_change) > self.ACK_STALL_SECS
                recv_stalled = (now - self._last_recv_change) > self.ACK_STALL_SECS
                unacked      = self.seq - self.last_acked
                need_done    = self.done_sent and not self.done_seen

            # keep resending llm.done until the client confirms it
            if need_done:
                if (now - self._last_done_send) * 1000.0 >= self.DONE_RESEND_MS:
                    self._send_done({"mode": "stream", "api": self.api, "retry": True})
                # optional escape if you set DONE_EXIT_GRACE_S > 0
                if self.DONE_EXIT_GRACE_S > 0 and (now - self._first_done_at) >= self.DONE_EXIT_GRACE_S:
                    return
                # continue nudging tail while we wait for the done ack
                if unacked > 0:
                    self._sweep_resend_unacked_tail_bulk(force=False)
                continue

            # watchdog-ish bulk resend when both sides look stalled
            if ack_stalled and recv_stalled and unacked > 0:
                self._sweep_resend_unacked_tail_bulk(force=True)
                continue

            # normal pressure resend
            if unacked > 4:
                self._sweep_resend_unacked_tail_bulk(force=False)

            # only exit after all chunks acked AND done seen
            with self._lock:
                if self.done_sent and self.last_acked >= self.done_seq and self.done_seen:
                    return


    # ────────────────────────────────────────────────────────────────
    # Send helpers for coalesced text
    # ────────────────────────────────────────────────────────────────
    def _send_coalesced_text(self, text: str):
        if not text: return
        if self.api == "chat":
            data = {"message": {"content": text}}
        else:
            data = {"response": text}
        self._send_chunk(data)

    # ────────────────────────────────────────────────────────────────
    # Stats
    # ────────────────────────────────────────────────────────────────
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

        with self._lock:
            unacked = max(0, self.seq - self.last_acked)
            recv_gap = max(0, self.seq - self.last_recv_seq)
            rendered_gap = max(0, self.seq - self.last_rendered_seq)
            e2e = self._e2e_latency_ema_ms
            rtt = self._ack_rtt_ema_ms

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
                "last_seq": self.seq,
                "last_acked": self.last_acked,
                "last_recv": self.last_recv_seq,
                "last_rendered": self.last_rendered_seq,
                "unacked": unacked,
                "recv_gap": recv_gap,
                "rendered_gap": rendered_gap,
                "e2e_latency_ema_ms": e2e,
                "ack_rtt_ema_ms": rtt,
            },
        }
        self._dm(payload)

    # ────────────────────────────────────────────────────────────────
    # Main loop
    # ────────────────────────────────────────────────────────────────
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

            # freeze state
            freeze_mode = False
            freeze_started = 0.0
            freeze_buf: list[str] = []
            freeze_bytes = 0

            def _bulk_flush():
                nonlocal bulk_buf, last_flush
                if bulk_buf:
                    self._send_coalesced_text("".join(bulk_buf))
                    bulk_buf = []
                    last_flush = time.monotonic()

            def _freeze_flush():
                nonlocal freeze_buf, freeze_bytes
                if freeze_buf:
                    self._send_coalesced_text("".join(freeze_buf))
                    freeze_buf = []
                    freeze_bytes = 0

            for part in gen:
                if self.cancelled:
                    break

                now = time.monotonic()
                with self._lock:
                    unacked = self.seq - self.last_acked
                    ack_stalled_s = (now - self._last_ack_change)
                    rendered_gap = self.seq - self.last_rendered_seq
                    e2e_ms = (self._e2e_latency_ema_ms or 0.0)
                    rtt_ms = (self._ack_rtt_ema_ms or 0.0)
                # Client-requested overrides
                in_panic_hint  = (now < self._panic_until)
                in_freeze_hint = (now < self._force_freeze_until)

                # Should we FREEZE?
                want_freeze = (
                    in_freeze_hint or
                    (unacked >= self.FREEZE_UNACK) or
                    ((ack_stalled_s >= self.FREEZE_ACK_STALE_S) and (e2e_ms >= self.FREEZE_E2E_MS or rtt_ms >= self.FREEZE_E2E_MS)) or
                    (rendered_gap >= self.FREEZE_RENDERED_GAP)
                )

                if want_freeze and not freeze_mode:
                    freeze_mode = True
                    freeze_started = now
                    _bulk_flush()

                # extract once
                delta = _extract_delta_from_part(self.api, part)

                if freeze_mode:
                    if delta:
                        freeze_buf.append(delta)
                        freeze_bytes += len(delta.encode("utf-8", "ignore"))
                        if freeze_bytes >= self.FREEZE_MAX_BYTES:
                            _freeze_flush()
                    else:
                        _freeze_flush()
                        self._send_chunk(part)

                    with self._lock:
                        recovered = (self.seq - self.last_acked) <= max(8, self.FREEZE_UNACK // 4)
                    held_too_long = (now - freeze_started) >= self.FREEZE_MAX_HOLD_S

                    if recovered or held_too_long:
                        _freeze_flush()
                        freeze_mode = False
                    continue  # skip normal bulk

                # ── Normal / Panic bulk logic ─────────────────────────
                with self._lock:
                    unacked = self.seq - self.last_acked
                    ack_stalled_s = (now - self._last_ack_change)

                ack_stale = ack_stalled_s > 0.8
                panic = in_panic_hint or (ack_stalled_s >= self.SEVERE_ACK_STALE_SECS)

                # pick caps (allow client override while in panic)
                max_parts = (self._bulk_parts_override if panic and self._bulk_parts_override else
                             (self.BULK_PANIC_PARTS if panic else self.BULK_MAX_PARTS))
                max_ms    = (self._bulk_max_ms_override if panic and self._bulk_max_ms_override else
                             (self.BULK_PANIC_MS if panic else self.BULK_MAX_MS))

                if (panic or (unacked >= self.BULK_UNACK_THRESHOLD) or ack_stale):
                    if delta:
                        bulk_buf.append(delta)
                        too_many = len(bulk_buf) >= max_parts
                        too_old  = (now - last_flush) * 1000.0 >= max_ms
                        if too_many or too_old:
                            _bulk_flush()
                        continue
                    else:
                        _bulk_flush()
                        self._send_chunk(part)
                else:
                    _bulk_flush()
                    self._send_chunk(part)

            # end of stream — flush
            if not self.cancelled:
                _freeze_flush()
                _bulk_flush()

            for _ in range(self.FINAL_SWEEPS):
                if self.cancelled: break
                # use tail-bulk sweep instead of spamming
                self._sweep_resend_unacked_tail_bulk(force=False)
                with self._lock:
                    if self.last_acked >= self.seq: break
                time.sleep(self.FINAL_GAP_SECS)

            self._emit_stats("final")
            self._send_done(meta or {"mode":"stream","api":self.api})

        except OllamaResponseError as e:
            self._send_error(f"Ollama error: {e.error}", kind=f"http_{getattr(e,'status_code', 'unknown')}")
        except Exception as e:
            self._send_error(f"{type(e).__name__}: {e}", kind="exception")


    def _kw_for_client(self, api: str) -> dict:
        kw = dict(self.kwargs or {})
        # Never pass 'think' into the Ollama client
        kw.pop("think", None)
        allow = self._ALLOWED_CHAT_KW if api == "chat" else self._ALLOWED_GEN_KW
        return {k: v for k, v in kw.items() if k in allow}

    def _invoke_once(self, api: str) -> dict:
        kw = self._kw_for_client(api)
        if api == "chat":
            if isinstance(kw.get("messages"), list):
                kw["messages"] = _massage_images_in_messages(
                    kw["messages"], src_addr=self.src_addr,
                    log_prefix=f"{self.src_addr} → {self.model}"
                )
            return self.client.chat(model=self.model, stream=False, **kw)
        elif api == "generate":
            return self.client.generate(model=self.model, stream=False, **kw)
        elif api == "embed":
            return self.client.embed(model=self.model, **kw)
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
            dst = kw.get("dst") or kw.get("to")
            return self.client.copy(src, dst)
        elif api == "create":
            return self.client.create(model=self.model, **kw)
        else:
            raise ValueError(f"unsupported api '{api}'")

    def _invoke_stream(self, api: str):
        meta = {"mode": "stream", "api": api}
        kw = self._kw_for_client(api)
        if api == "chat":
            if isinstance(kw.get("messages"), list):
                kw["messages"] = _massage_images_in_messages(
                    kw["messages"], src_addr=self.src_addr,
                    log_prefix=f"{self.src_addr} → {self.model}"
                )
            gen = self.client.chat(model=self.model, stream=True, **kw)
            return gen, meta
        elif api == "generate":
            gen = self.client.generate(model=self.model, stream=True, **kw)
            return gen, meta
        else:
            raise ValueError(f"streaming not supported for api '{api}'")

# Registry of active LLM streams by id
_llm_streams: Dict[str, _LLMStream] = {}

def _normalize_think_on_body(body: dict) -> dict:
    """
    Normalize 'think' so downstream code sees it consistently.
    - Keep a top-level 'think' (bool) for telemetry.
    - DO NOT add 'think' to kwargs (Ollama client doesn't accept it).
    - If think=True, set kwargs.options.thinking=True (silently ignored if unsupported).
    """
    out = dict(body or {})
    # accept either top-level or kwargs.think coming from older clients
    incoming_kw = (out.get("kwargs") or {})
    think = bool(out.get("think") or incoming_kw.get("think") or False)

    out["think"] = think
    kw = out.setdefault("kwargs", {})
    # ensure no stray kwarg that would break the Ollama client call
    kw.pop("think", None)

    if think:
        opts = kw.setdefault("options", {})
        opts["thinking"] = True  # harmless if model ignores it

    return out

def _start_llm(src_addr: str, body: dict,
               on_start=None, on_delta=None, on_done=None):
    # Normalize 'think' into kwargs/options so the client library sees it.
    body = _normalize_think_on_body(body)

    stream = _LLMStream(src_addr, body, on_start=on_start, on_delta=on_delta, on_done=on_done)
    _llm_streams[stream.id] = stream
    threading.Thread(target=stream.run, daemon=True).start()
    return stream.id

def _ack_llm(body: dict):
    sid = body.get("id")
    upto = body.get("upto")                 # highest contiguous RECEIVED
    upto_recv = body.get("upto_recv")       # alias; prefer this if present
    upto_rendered = body.get("upto_rendered")  # highest CONTIGUOUS RENDERED (optional)
    client_now = body.get("clientNow")      # client ms clock (optional)
    seen_done = bool(body.get("seen_done"))            # ← NEW
    if not isinstance(sid, str): return
    seq = upto_recv if isinstance(upto_recv, int) else upto
    if not isinstance(seq, int): return
    stream = _llm_streams.get(sid)
    if stream:
        stream.ack(seq, {"rendered": upto_rendered if isinstance(upto_rendered, int) else None,
                         "client_now": client_now if isinstance(client_now, (int, float)) else None,
                         "seen_done":     seen_done,                          # ← NEW
                         })

def _cancel_llm(body: dict):
    sid = body.get("id")
    if not isinstance(sid, str): return
    stream = _llm_streams.get(sid)
    if stream: stream.cancel()

# Advertised control-ops this node supports
CTRL_CAPS = {"models", "info", "peers", "caps", "llm.bulk"}

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
            call_kwargs = dict(kwargs)
            call_kwargs["messages"] = req_messages
            call_kwargs["options"]  = opts

            # Preserve and apply 'think' from the incoming body/kwargs
            think_flag = bool(body.get("think") or kwargs.get("think"))
            # never pass as a raw kwarg to the client
            call_kwargs.pop("think", None)
            if think_flag:
                call_kwargs.setdefault("options", {})["thinking"] = True

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
                "event": "llm.request",
                "api":   api,
                "model": s.model,
                "stream": stream,
                "kwargs": call_kwargs,
                "id": body.get("id"),
                "sid": sid,
                "think": think_flag,   # ← carry to _start_llm → _normalize_think_on_body
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

    import errno
    from eventlet import wsgi

    def _bind_http(host: str = "0.0.0.0", base_port: int = None, tries: int = 50):
        """Bind to base_port or the next available. If all tried, use ephemeral."""
        if base_port is None:
            try:
                base_port = int(os.environ.get("PORT", "3000") or 3000)
            except Exception:
                base_port = 3000

        # Try base_port .. base_port+tries-1
        for i in range(max(1, tries)):
            port = base_port + i
            try:
                listener = eventlet.listen((host, port))
                return listener, port
            except OSError as e:
                # EADDRINUSE (Linux=98, macOS=48, Windows=10048). Keep scanning.
                if getattr(e, "errno", None) in (errno.EADDRINUSE, 48, 98, 10048):
                    continue
                # Permission denied or other fatal errors -> re-raise.
                raise

        # Fallback: ephemeral port
        listener = eventlet.listen((host, 0))
        port = listener.getsockname()[1]
        return listener, port

    listener, http_port = _bind_http()
    state["http_port"] = http_port  # optional: expose chosen port in / metadata
    print(f"   HTTP listening on http://0.0.0.0:{http_port}")

    wsgi.server(listener, app)
