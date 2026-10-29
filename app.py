"""
app.py - RFTP Studio backend.  Serves the web frontend and runs the REAL experiments:
   POST /api/run       real UDP-socket transfer (RFTP or plain UDP) -> packet event log + stats
   POST /api/ns2       NS2 simulation -> trace file converted to the same event format
   GET  /api/results   comparison.csv from ns2_analysis.py
   POST /api/analysis  re-run ns2_analysis.py --quick
Run:  python app.py        (opens http://127.0.0.1:8000)     Only the standard library is needed.
"""
import argparse
import csv
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import threading
import time
import webbrowser
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse, parse_qs

import hashlib
import inspect
from rftp_common import build_packet, parse_packet, HEADER, T_META, T_DATA, T_ACK, F_EOF, DEFAULT_CHUNK
from rftp_server import RFTPServer
from rftp_client import send_file

HERE = os.path.dirname(os.path.abspath(__file__))
WEB = os.path.join(HERE, "web")
TCL = os.path.join(HERE, "ns2", "rftp_sim.tcl")
RESULTS = os.path.join(HERE, "results")
MAX_UPLOAD = 20 * 1024 * 1024
MAX_EVENTS = 8000


# --------------------------------------------------------- real socket transfer
def do_run(q, body, filename):
    f = lambda k, d: float(q.get(k, [d])[0])
    loss, corrupt = f("loss", 0) / 100, f("corrupt", 0) / 100
    delay, timeout = f("delay", 0) / 1000, f("timeout", 30) / 1000
    chunk = int(f("chunk", 1024))
    reliable = q.get("reliable", ["1"])[0] == "1"
    tmp = tempfile.mkdtemp()
    try:
        name = re.sub(r"[^\w.\-]", "_", filename or "sample.bin")
        src = os.path.join(tmp, name)
        with open(src, "wb") as fh:
            fh.write(body if body else os.urandom(int(f("sample_kb", 40) * 1024)))
        events = []
        srv = RFTPServer("127.0.0.1", 0, os.path.join(tmp, "out"), loss=loss, corrupt=corrupt,
                         delay=delay, log=lambda m: None, on_event=events.append)
        port = srv.raw_sock.getsockname()[1]
        srv.start()
        time.sleep(0.05)
        stats = send_file(src, "127.0.0.1", port, chunk=chunk, timeout=timeout, max_retries=40,
                          loss=loss, corrupt=corrupt, delay=delay, reliable=reliable,
                          on_event=events.append, log=lambda m: None)
        for _ in range(40):                       # wait for server to finish reconstruction
            if srv.last_result:
                break
            time.sleep(0.1)
        result = srv.last_result or {"intact": False, "packets_received": 0, "packets_expected": stats["packets"],
                                     "packets_missing": stats["packets"], "missing_seqs": [], "duplicates": 0}
        srv.stop()
        return {"events": events[:MAX_EVENTS], "truncated": len(events) > MAX_EVENTS,
                "stats": stats, "result": result, "reliable": reliable, "source": "sockets"}
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


# --------------------------------------------------------------------- NS2 run
def ns_available():
    return shutil.which("ns") is not None


def trace_to_events(path, npkts):
    """Convert an NS2 trace into RFTP packet events.  Stop-and-wait means only one
    packet is outstanding, so the protocol sequence number can be reconstructed:
    data events carry `cur`; a delivered ACK advances `cur`."""
    rows = []
    with open(path) as fh:
        for line in fh:
            p = line.split()
            if len(p) > 5 and p[0] in ("r", "d"):
                rows.append((float(p[1]), p[0], int(p[5])))
    rows.sort(key=lambda r: r[0])
    events, cur, seen, delivered = [], 0, set(), {}
    for t, e, size in rows:
        if size != 40:                                   # DATA / META packet
            retrans = cur in seen
            if retrans:
                events.append({"kind": "timeout", "seq": cur, "t": t})
            events.append({"kind": "data", "seq": cur, "retrans": retrans, "t": t,
                           "status": "delivered" if e == "r" else "dropped"})
            seen.add(cur)
            if e == "r":
                delivered[cur] = delivered.get(cur, 0) + 1
        else:                                            # ACK packet
            events.append({"kind": "ack", "seq": cur, "t": t, "dup": delivered.get(cur, 0) > 1,
                           "status": "delivered" if e == "r" else "dropped"})
            if e == "r":
                cur += 1
    return events


def do_ns2(params):
    if not ns_available():
        sample = os.path.join(RESULTS, "ns2_sample.json")      # recorded real NS2 run
        if os.path.exists(sample):
            with open(sample) as fh:
                rec = json.load(fh)
            rec["recorded"] = True
            return rec
        return {"available": False,
                "hint": "NS2 is not installed on this machine. On Ubuntu/WSL: sudo apt install ns2 nam"}
    loss = float(params.get("loss", 20)) / 100
    npk = int(params.get("packets", 40))
    timeout = float(params.get("timeout", 0.3))
    bw, delay, chunk = params.get("bw", "1Mb"), params.get("delay", "10ms"), int(params.get("chunk", 1024))
    seed = int(params.get("seed", int(time.time()) % 1000))
    work = tempfile.mkdtemp()
    try:
        cmd = ["ns", TCL, str(loss), str(npk), str(timeout), str(bw), str(delay), str(chunk), str(seed)]
        out = subprocess.run(cmd, cwd=work, capture_output=True, text=True, timeout=300)
        m = re.search(r"RFTP_STATS (.*)", out.stdout)
        if not m:
            return {"available": True, "error": "NS2 produced no output:\n" + out.stdout[-500:] + out.stderr[-500:]}
        raw = dict(kv.split("=") for kv in m.group(1).split())
        trace_path = os.path.join(work, "rftp_ns2.tr")
        events = trace_to_events(trace_path, npk)
        with open(trace_path) as fh:
            trace = fh.read()
        t = float(raw["transfer_time"])
        stats = {"file_name": f"{npk} packets x {chunk} B", "file_size": npk * chunk, "packets": npk,
                 "retransmissions": int(raw["retransmissions"]), "transfer_time_s": round(t, 3),
                 "speed_KBps": round(npk * chunk / 1024 / t, 1) if t else 0, "status": raw["status"],
                 "packets_sent_total": int(raw["sent_total"]), "acks_received": int(raw["acks_received"])}
        result = {"intact": raw["status"] == "COMPLETED" and int(raw["server_unique"]) == npk + 1,
                  "packets_received": int(raw["server_unique"]), "packets_expected": npk + 1,
                  "packets_missing": npk + 1 - int(raw["server_unique"]), "duplicates": int(raw["server_duplicates"]),
                  "missing_seqs": []}
        lines = trace.splitlines()
        return {"available": True, "events": events, "stats": stats, "result": result, "reliable": True,
                "source": "ns2", "seed": seed, "trace_excerpt": "\n".join(lines[:40]),
                "trace_lines": len(lines), "trace_full": trace[:400000],
                "params": {"loss": loss, "bw": bw, "delay": delay, "timeout": timeout}}
    finally:
        shutil.rmtree(work, ignore_errors=True)



# ------------------------------------------------------ protocol description
def protocol_info():
    """Describe the RFTP protocol using the real packet builder (single source of truth)."""
    chunks = [b"Reliable data!", b"over UDP."]
    whole = b"".join(chunks)
    meta = {"tid": "3f9c1e7a52b84d06", "name": "demo.txt", "size": len(whole), "total": len(chunks),
            "chunk": len(chunks[0]), "sha256": hashlib.sha256(whole).hexdigest(), "reliable": True}
    samples = []

    def add(name, desc, effect, pkt, flip=None):
        ptype, flags, seq, crc = HEADER.unpack(pkt[:HEADER.size])
        payload = pkt[HEADER.size:]
        samples.append({"name": name, "desc": desc, "effect": effect, "hex": pkt.hex(), "length": len(pkt),
                        "type": ptype, "flags": flags, "seq": seq, "crc": crc, "flip": flip,
                        "payload_text": payload.decode("utf-8", "replace"), "valid": parse_packet(pkt) is not None})

    add("META", "First packet of a transfer (sequence 0). The data field is a small JSON record: transfer id, "
        "file name, size, packet count, chunk size and the SHA-256 of the file.",
        "Server creates a session for this transfer and answers ACK 0. A repeated META (same transfer id) is only re-acknowledged.",
        build_packet(T_META, 0, json.dumps(meta, separators=(",", ":")).encode()))
    add("DATA", "A data packet: sequence number 1 and one chunk of the file (here 14 bytes; normally up to 1024).",
        "CRC-32 is valid and the sequence number is new, so the chunk is stored and ACK 1 is sent.",
        build_packet(T_DATA, 1, chunks[0]))
    add("DATA (last)", "The final data packet. Flags = 0x01 marks End-Of-File.",
        "Server stores it, sends ACK 2, and since every chunk has arrived it rebuilds the file and verifies the SHA-256.",
        build_packet(T_DATA, 2, chunks[1], F_EOF))
    add("ACK", "Acknowledgement for sequence 1. Only the 10-byte header, no data.",
        "Client sees a valid ACK with the expected sequence number, stops the timer and sends the next packet.",
        build_packet(T_ACK, 1))
    bad = bytearray(build_packet(T_DATA, 1, chunks[0]))
    bad[12] ^= 0xFF
    add("DATA corrupted", "The same DATA packet after one byte (shown in red) was flipped in the network.",
        "CRC-32 does not match, so the server discards the packet and sends NO ACK. The client's timer expires and it retransmits.",
        bytes(bad), flip=12)
    sig = inspect.signature(send_file).parameters
    return {"constants": {"header_bytes": HEADER.size, "header_format": "!BBII  (type, flags, seq, crc32)",
                          "types": {"META": T_META, "DATA": T_DATA, "ACK": T_ACK}, "eof_flag": F_EOF,
                          "chunk": DEFAULT_CHUNK, "timeout_default": sig["timeout"].default,
                          "max_retries": sig["max_retries"].default, "default_port": 9000},
            "samples": samples}

# -------------------------------------------------------------- analysis results
def read_results():
    path = os.path.join(RESULTS, "comparison.csv")
    if not os.path.exists(path):
        return []
    rows = []
    with open(path, newline="") as fh:
        for r in csv.DictReader(fh):
            row = {}
            for k, v in r.items():
                try:
                    row[k] = float(v)
                except (TypeError, ValueError):
                    row[k] = v
            rows.append(row)
    return rows


# ------------------------------------------------------------------ HTTP layer
class Handler(BaseHTTPRequestHandler):
    def log_message(self, *a):
        pass

    def _json(self, obj, code=200):
        data = json.dumps(obj).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self):
        u = urlparse(self.path)
        if u.path in ("/", "/index.html"):
            with open(os.path.join(WEB, "index.html"), "rb") as fh:
                data = fh.read()
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)
        elif u.path == "/api/info":
            self._json({"ns2": ns_available(), "python": sys.version.split()[0]})
        elif u.path == "/api/protocol":
            self._json(protocol_info())
        elif u.path == "/api/results":
            self._json({"rows": read_results()})
        else:
            self.send_error(404)

    def do_POST(self):
        u = urlparse(self.path)
        n = int(self.headers.get("Content-Length") or 0)
        if n > MAX_UPLOAD:
            return self._json({"error": "File too large (max 20 MB)"}, 413)
        body = self.rfile.read(n) if n else b""
        try:
            if u.path == "/api/run":
                self._json(do_run(parse_qs(u.query), body, self.headers.get("X-Filename", "sample.bin")))
            elif u.path == "/api/ns2":
                self._json(do_ns2(json.loads(body or b"{}")))
            elif u.path == "/api/analysis":
                if not ns_available():
                    return self._json({"error": "NS2 not installed - cannot re-run the analysis here."})
                subprocess.run([sys.executable, os.path.join(HERE, "ns2_analysis.py"), "--quick"],
                               cwd=HERE, capture_output=True, timeout=900)
                self._json({"rows": read_results()})
            else:
                self.send_error(404)
        except Exception as e:                               # report problems to the UI
            self._json({"error": f"{type(e).__name__}: {e}"}, 500)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--port", type=int, default=8000)
    ap.add_argument("--no-browser", action="store_true")
    a = ap.parse_args()
    httpd = ThreadingHTTPServer(("127.0.0.1", a.port), Handler)
    url = f"http://127.0.0.1:{a.port}"
    print(f"RFTP Studio running at {url}   (NS2 {'found' if ns_available() else 'NOT found'})   Ctrl+C to stop")
    if not a.no_browser:
        threading.Timer(0.8, lambda: webbrowser.open(url)).start()
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        print("\nbye")
