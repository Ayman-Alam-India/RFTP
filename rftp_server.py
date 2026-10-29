"""
rftp_server.py  -  RFTP receiver
Covers: 7.4 UDP Communication, 7.5 Acknowledgement, 7.8 Server,
        7.9 File Reconstruction, 7.10 Monitoring/Statistics (server side)

Run:  python rftp_server.py --port 9000 --out received
Optional simulation on the ACK path:  --loss 0.2 --corrupt 0.05 --delay 0.01
"""
import argparse
import hashlib
import json
import os
import socket
import threading
import time

from rftp_common import (T_META, T_DATA, T_ACK, F_EOF, LossySocket,
                         build_packet, parse_packet)


class Session:
    """State of one incoming file transfer."""

    def __init__(self, tid, meta, addr):
        self.tid = tid
        self.meta = meta
        self.addr = addr
        self.chunks = {}            # seq -> bytes
        self.duplicates = 0
        self.eof_seen = False
        self.done = False
        self.start = time.time()
        self.last_seen = time.time()
        self.reliable = meta.get("reliable", True)


class RFTPServer:
    def __init__(self, host="0.0.0.0", port=9000, out_dir="received",
                 loss=0.0, corrupt=0.0, delay=0.0, log=print, on_done=None, on_event=None):
        self.host, self.port, self.out_dir = host, port, out_dir
        self.log = log
        self.on_done = on_done
        self.raw_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.raw_sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 4 * 1024 * 1024)
        self.raw_sock.bind((host, port))
        self.raw_sock.settimeout(0.5)
        self.sock = LossySocket(self.raw_sock, loss, corrupt, delay)   # ACK path
        self._dup = False
        self.last_result = None
        if on_event:
            def _hook(pkt, status, wire=None):
                w = pkt if wire is None else wire
                on_event({"kind": "ack", "seq": parse_packet(pkt)[2], "status": status, "dup": self._dup,
                          "hex": w[:26].hex(), "len": len(w), "crc_ok": parse_packet(w) is not None})
            self.sock.hook = _hook
        self.sessions = {}
        self.corrupted_rx = 0
        self._stop = threading.Event()
        self._thread = None
        os.makedirs(out_dir, exist_ok=True)

    # ------------------------------------------------------------- control
    def start(self):
        self._thread = threading.Thread(target=self.serve_forever, daemon=True)
        self._thread.start()

    def stop(self):
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=2)
        self.raw_sock.close()

    # ---------------------------------------------------------------- loop
    def serve_forever(self):
        self.log(f"[SERVER] Listening on UDP {self.host}:{self.port}  (saving to '{self.out_dir}')")
        while not self._stop.is_set():
            try:
                raw, addr = self.raw_sock.recvfrom(65535)
            except socket.timeout:
                self._idle_check()
                continue
            except ConnectionResetError:        # Windows: ICMP port-unreachable
                continue
            except OSError:
                break
            self._handle(raw, addr)
            self._idle_check()

    def _ack(self, seq, addr, dup=False):
        self._dup = dup
        self.sock.sendto(build_packet(T_ACK, seq), addr)

    # -------------------------------------------------------------- packets
    def _handle(self, raw, addr):
        pkt = parse_packet(raw)
        if pkt is None:                         # checksum failed -> discard, NO ACK
            self.corrupted_rx += 1
            self.log("[SERVER] Corrupted packet discarded (checksum mismatch) - no ACK sent")
            return
        ptype, flags, seq, payload = pkt
        s = self.sessions.get(addr)

        if ptype == T_META:
            meta = json.loads(payload.decode())
            if s and s.tid == meta["tid"]:      # duplicate META (our ACK was lost)
                s.duplicates += 1
                if s.reliable:
                    self._ack(0, addr, True)
                return
            s = Session(meta["tid"], meta, addr)
            self.sessions[addr] = s
            self.log(f"[SERVER] New transfer from {addr[0]}:{addr[1]} -> '{meta['name']}' "
                     f"({meta['size']} bytes, {meta['total']} packets, "
                     f"{'RFTP reliable' if s.reliable else 'plain UDP (no reliability)'})")
            if s.reliable:
                self._ack(0, addr)
            return

        if ptype == T_DATA and s:
            s.last_seen = time.time()
            was_dup = seq in s.chunks
            if seq in s.chunks:                 # duplicate: ignore data, but re-ACK
                s.duplicates += 1
                self.log(f"[SERVER] Duplicate packet {seq} ignored (re-sending ACK)")
            elif 1 <= seq <= s.meta["total"]:
                s.chunks[seq] = payload
            if s.reliable:
                self._ack(seq, addr, was_dup)
            if flags & F_EOF:
                s.eof_seen = True
            if not s.done:
                if len(s.chunks) == s.meta["total"]:
                    self._finalize(s)
                elif (not s.reliable) and s.eof_seen:
                    self._finalize(s)

    def _idle_check(self):
        """Plain-UDP mode has no recovery: if the sender goes quiet, finish with what we have."""
        now = time.time()
        for s in list(self.sessions.values()):
            if not s.done and not s.reliable and now - s.last_seen > 1.5:
                self._finalize(s)

    # ------------------------------------------------------- reconstruction
    def _finalize(self, s: Session):
        s.done = True
        total = s.meta["total"]
        missing = [i for i in range(1, total + 1) if i not in s.chunks]
        data = b"".join(s.chunks[i] for i in sorted(s.chunks))
        name = "received_" + os.path.basename(s.meta["name"])
        path = os.path.join(self.out_dir, name)
        with open(path, "wb") as f:
            f.write(data)
        ok = (not missing and hashlib.sha256(data).hexdigest() == s.meta["sha256"])
        result = {
            "file": path, "expected_size": s.meta["size"], "received_size": len(data),
            "packets_expected": total, "packets_received": len(s.chunks),
            "packets_missing": len(missing), "missing_seqs": missing, "duplicates": s.duplicates,
            "corrupted_discarded": self.corrupted_rx, "intact": ok,
            "ack_stats": {"sent": self.sock.sent, "dropped": self.sock.dropped,
                          "corrupted": self.sock.corrupted},
        }
        self.log("[SERVER] ---------- FILE RECONSTRUCTION ----------")
        self.log(f"[SERVER] Saved: {path}")
        self.log(f"[SERVER] Packets received {len(s.chunks)}/{total}, missing {len(missing)}, "
                 f"duplicates ignored {s.duplicates}")
        self.log(f"[SERVER] Size {len(data)}/{s.meta['size']} bytes   "
                 f"SHA-256 check: {'MATCH - file is intact' if ok else 'FAILED - file is damaged/incomplete'}")
        if self.on_done:
            self.on_done(result)
        self.last_result = result


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="RFTP server (receiver)")
    ap.add_argument("--host", default="0.0.0.0")
    ap.add_argument("--port", type=int, default=9000)
    ap.add_argument("--out", default="received")
    ap.add_argument("--loss", type=float, default=0.0, help="ACK loss probability (0-1)")
    ap.add_argument("--corrupt", type=float, default=0.0, help="ACK corruption probability (0-1)")
    ap.add_argument("--delay", type=float, default=0.0, help="max random ACK delay in seconds")
    a = ap.parse_args()
    srv = RFTPServer(a.host, a.port, a.out, a.loss, a.corrupt, a.delay)
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        print("\n[SERVER] stopped")
