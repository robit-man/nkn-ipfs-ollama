#!/usr/bin/env python3
import os
import sys
import subprocess
import json
import shutil
import threading
import itertools
import time
import socket
import ssl
import urllib.request
import errno
from datetime import datetime, timedelta, timezone
from http.server import HTTPServer, SimpleHTTPRequestHandler

# ─── Constants ───────────────────────────────────────────────────────────────
SCRIPT_DIR  = os.path.dirname(os.path.abspath(__file__))
CONFIG_PATH = os.path.join(SCRIPT_DIR, "config.json")
VENV_FLAG   = "--in-venv"
VENV_DIR    = os.path.join(SCRIPT_DIR, "venv")
HTTPS_PORT  = 443

# ─── Spinner for long operations ──────────────────────────────────────────────
class Spinner:
    def __init__(self, msg):
        self.msg   = msg
        self.spin  = itertools.cycle("|/-\\")
        self._stop = threading.Event()
        self._thr  = threading.Thread(target=self._run, daemon=True)
    def _run(self):
        while not self._stop.is_set():
            sys.stdout.write(f"\r{self.msg} {next(self.spin)}")
            sys.stdout.flush()
            time.sleep(0.1)
        sys.stdout.write("\r" + " "*(len(self.msg)+2) + "\r")
        sys.stdout.flush()
    def __enter__(self): self._thr.start()
    def __exit__(self, exc_type, exc, tb):
        self._stop.set(); self._thr.join()

# ─── Virtualenv bootstrap ─────────────────────────────────────────────────────
def bootstrap_and_run():
    if VENV_FLAG not in sys.argv:
        if not os.path.isdir(VENV_DIR):
            with Spinner("Creating virtualenv…"):
                subprocess.check_call([sys.executable, "-m", "venv", VENV_DIR])
        pip = os.path.join(VENV_DIR, "Scripts" if os.name=="nt" else "bin", "pip")
        with Spinner("Installing dependencies…"):
            subprocess.check_call([pip, "install", "--upgrade", "pip", "cryptography"])
        py = os.path.join(VENV_DIR, "Scripts" if os.name=="nt" else "bin", "python")
        os.execv(py, [py, __file__, VENV_FLAG])
    else:
        sys.argv.remove(VENV_FLAG)
        main()

# ─── Config I/O ───────────────────────────────────────────────────────────────
def load_config():
    if os.path.exists(CONFIG_PATH):
        try:
            return json.load(open(CONFIG_PATH))
        except Exception:
            pass
    return {"serve_path": os.getcwd(), "extra_dns_sans": []}  # optional extra hostnames

def save_config(cfg):
    with open(CONFIG_PATH, "w") as f:
        json.dump(cfg, f, indent=4)

# ─── Networking Helpers ──────────────────────────────────────────────────────
def get_lan_ip():
    # robust local IP discovery without external traffic
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        s.connect(("192.0.2.1", 80))  # TEST-NET-1, no packets sent
        ip = s.getsockname()[0]
    finally:
        s.close()
    return ip

def get_public_ip(timeout=3):
    try:
        return urllib.request.urlopen("https://api.ipify.org", timeout=timeout).read().decode().strip()
    except Exception:
        return None

# ─── ASCII Banner ────────────────────────────────────────────────────────────
def print_banner(lan, public, port):
    lines = [
        f"  Local : https://{lan}:{port}",
        f"  Public: https://{public}:{port}" if public else "  Public: <none>"
    ]
    w = max(len(l) for l in lines) + 4
    print("\n╔" + "═"*w + "╗")
    for l in lines:
        print("║" + l.ljust(w) + "║")
    print("╚" + "═"*w + "╝\n")

# ─── Certificate Generation (fix IP SANs) ────────────────────────────────────
def generate_cert(cert_file, key_file, cfg):
    lan_ip    = get_lan_ip()
    public_ip = get_public_ip()

    # Always generate a self-signed cert (no mkcert)
    from cryptography.hazmat.primitives import serialization, hashes
    from cryptography.hazmat.primitives.asymmetric import rsa
    from cryptography.x509 import NameOID, SubjectAlternativeName, DNSName, IPAddress
    import cryptography.x509 as x509
    import ipaddress as ipa

    keyobj = rsa.generate_private_key(public_exponent=65537, key_size=2048)

    # Build SAN list: IPs must be IPAddress; hostnames DNSName
    san_list = [
        DNSName("localhost"),
        IPAddress(ipa.ip_address("127.0.0.1")),
    ]
    # LAN IP
    try:
        san_list.append(IPAddress(ipa.ip_address(lan_ip)))
    except ValueError:
        pass
    # Public IP
    if public_ip:
        try:
            san_list.append(IPAddress(ipa.ip_address(public_ip)))
        except ValueError:
            pass
    # Optional extra hostnames from config
    for host in (cfg.get("extra_dns_sans") or []):
        host = str(host).strip()
        if host:
            san_list.append(DNSName(host))

    san  = SubjectAlternativeName(san_list)
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, lan_ip)])

    not_before = datetime.now(timezone.utc) - timedelta(minutes=5)
    not_after  = not_before + timedelta(days=365)

    with Spinner("Generating self-signed certificate…"):
        cert = (
            x509.CertificateBuilder()
               .subject_name(name)
               .issuer_name(name)
               .public_key(keyobj.public_key())
               .serial_number(x509.random_serial_number())
               .not_valid_before(not_before)
               .not_valid_after(not_after)
               .add_extension(san, critical=False)
               .sign(keyobj, hashes.SHA256())
        )

    with open(key_file, "wb") as f:
        f.write(keyobj.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.TraditionalOpenSSL,
            encryption_algorithm=serialization.NoEncryption()
        ))
    with open(cert_file, "wb") as f:
        f.write(cert.public_bytes(serialization.Encoding.PEM))

# ─── Main Flow ────────────────────────────────────────────────────────────────
def main():
    # 1) Need root to bind :443 (Linux). Re-run under sudo if necessary.
    if hasattr(os, "geteuid") and os.geteuid() != 0:
        print("⚠ Need root to bind port 443; re-running with sudo…")
        os.execvp("sudo", ["sudo", sys.executable] + sys.argv)

    # 2) Load config & prompt once for serve_path if missing
    cfg = load_config()
    updated = False
    if not os.path.exists(CONFIG_PATH):
        default_path = cfg.get("serve_path") or os.getcwd()
        entered = input(f"Serve path [{default_path}]: ").strip() or default_path
        cfg["serve_path"] = entered
        # optional extra DNS SANs (comma-separated)
        extra = (input("Extra DNS SANs (comma-separated, optional): ").strip() or "")
        if extra:
            cfg["extra_dns_sans"] = [h.strip() for h in extra.split(",") if h.strip()]
        updated = True
    for key, default in {"serve_path": os.getcwd(), "extra_dns_sans": []}.items():
        if key not in cfg:
            cfg[key] = default
            updated = True
    if updated:
        save_config(cfg)

    # 3) cd into serve directory
    os.chdir(cfg["serve_path"])

    # 4) Generate cert.pem & key.pem (always refresh to include current IPs)
    cert_file = os.path.join(os.getcwd(), "cert.pem")
    key_file  = os.path.join(os.getcwd(), "key.pem")
    generate_cert(cert_file, key_file, cfg)

    # 5) Build SSL context
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.options |= ssl.OP_NO_SSLv2 | ssl.OP_NO_SSLv3
    context.load_cert_chain(certfile=cert_file, keyfile=key_file)

    # 6) Print LAN & public URLs
    lan_ip    = get_lan_ip()
    public_ip = get_public_ip()
    bind_port = HTTPS_PORT  # may change if taken
    print_banner(lan_ip, public_ip, bind_port)

    # 7) If Node project present, start Node with robust TLS shim
    node_path = shutil.which("node")
    if os.path.exists("package.json") and os.path.exists("server.js") and node_path:
        patch_path = os.path.join(os.getcwd(), "tls_patch.js")
        # absolute cert paths
        CERT_ABS = os.path.abspath(cert_file)
        KEY_ABS  = os.path.abspath(key_file)
        with open(patch_path, "w") as f:
            f.write(f"""\
const fs = require('fs');
const path = require('path');
const https = require('https');
const http = require('http');

const CERT = {json.dumps(CERT_ABS)};
const KEY  = {json.dumps(KEY_ABS)};

// Patch createServer
const _create = http.createServer;
http.createServer = function (opts, listener) {{
  if (typeof opts === 'function') listener = opts;
  return https.createServer({{ key: fs.readFileSync(KEY), cert: fs.readFileSync(CERT) }}, listener);
}};

// Patch Server constructor (covers new http.Server(...))
const _Server = http.Server;
http.Server = function (...args) {{
  return https.Server({{ key: fs.readFileSync(KEY), cert: fs.readFileSync(CERT) }}, ...args);
}};
http.Server.prototype = _Server.prototype;
""")

        env = os.environ.copy()
        env["PORT"] = str(HTTPS_PORT)  # many apps respect PORT
        # Use -r to require the patch; prints strong message if 443 is taken
        cmd = [node_path, "-r", patch_path, "server.js"]
        with Spinner(f"Starting Node.js (TLS on port {HTTPS_PORT})…"):
            try:
                # Try binding 443 quickly to detect conflicts before spawn
                test_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                test_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                test_sock.bind(("0.0.0.0", HTTPS_PORT))
                test_sock.close()
            except OSError as e:
                print(f"⚠ Port {HTTPS_PORT} unavailable (reason: {e}). Your Node app may choose another port.")
            proc = subprocess.Popen(cmd, env=env, cwd=os.getcwd())
        try:
            proc.wait()
        except KeyboardInterrupt:
            proc.terminate()
        return

    # 8) Python HTTPS fallback: try :443, else next free port
    port = HTTPS_PORT
    httpd = None
    for p in range(HTTPS_PORT, HTTPS_PORT + 10):
        try:
            httpd = HTTPServer(("0.0.0.0", p), SimpleHTTPRequestHandler)
            port = p
            break
        except OSError as e:
            if e.errno == errno.EADDRINUSE:
                continue
            raise
    if httpd is None:
        raise RuntimeError("Unable to bind any port in 443..452")

    httpd.socket = context.wrap_socket(httpd.socket, server_side=True)
    if port != HTTPS_PORT:
        print(f"⚠ Port {HTTPS_PORT} in use; serving on port {port}")

    print(f"→ Serving HTTPS from {os.getcwd()} on 0.0.0.0:{port}")
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass

if __name__ == "__main__":
    bootstrap_and_run()
