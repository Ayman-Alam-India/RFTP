"""
rftp_client.py  -  RFTP sender
Covers: 7.1 Client, 7.2 File Segmentation, 7.3 Packet Management, 7.5 ACK handling,
        7.6 Timeout & Retransmission, 7.10 Statistics (client side)

Run:  python rftp_client.py myfile.pdf --host 127.0.0.1 --port 9000 --loss 0.2
      python rftp_client.py myfile.pdf --no-reliability --loss 0.2     (plain UDP baseline)
"""
import argparse
import hashlib
import json
import math
import os
import socket
import time
import uuid

from rftp_common import (T_META, T_DATA, T_ACK, F_EOF, DEFAULT_CHUNK, LossySocket,
                         build_packet, parse_packet)


def segment_file(data: bytes, chunk: int):
    """7.2 File Segmentation: fixed-size chunks, last one may be smaller."""
    if not data:
        return [b""]
    return [data[i:i + chunk] for i in range(0, len(data), chunk)]


def send_file(path, host="127.0.0.1", port=9000, chunk=DEFAULT_CHUNK, timeout=0.3,
              max_retries=15, loss=0.0, corrupt=0.0, delay=0.0, reliable=True,
              on_progress=None, log=print, on_event=None):
    with open(path, "rb") as f:
        data = f.read()
    chunks = segment_file(data, chunk)
    total = len(chunks)
    meta = {"tid": uuid.uuid4().hex, "name": os.path.basename(path), "size": len(data),
            "total": total, "chunk": chunk, "sha256": hashlib.sha256(data).hexdigest(),
            "reliable": reliable}

    raw = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    seen_seqs = set()

    def hook(pkt, status, wire=None):            # report every data/meta packet we send
        if on_event:
            w = pkt if wire is None else wire
            seq = parse_packet(pkt)[2]
            on_event({"kind": "data", "seq": seq, "status": status, "retrans": seq in seen_seqs,
                      "hex": w[:26].hex(), "len": len(w), "crc_ok": parse_packet(w) is not None})
            seen_seqs.add(seq)
    sock = LossySocket(raw, loss, corrupt, delay, hook=hook)
    server = (host, port)
    retrans = acks = 0
    status = "COMPLETED"
    t0 = time.time()
    log(f"[CLIENT] Sending '{meta['name']}' ({len(data)} bytes) as {total} packets of up to {chunk} bytes "
        f"-> {host}:{port}  [{'RFTP reliable' if reliable else 'plain UDP'}]  "
        f"(sim: loss={loss:.0%}, corrupt={corrupt:.0%}, delay<={delay}s)")

    # packet list: seq 0 = META, seq 1..N = DATA
    packets = [build_packet(T_META, 0, json.dumps(meta).encode())]
    for i, c in enumerate(chunks, start=1):
        packets.append(build_packet(T_DATA, i, c, F_EOF if i == total else 0))

    if not reliable:
        # ---- baseline: fire and forget, no ACKs, no retransmission
        raw.sendto(packets[0], server)          # metadata sent without simulated loss so the
        time.sleep(0.01)                        # baseline comparison is about DATA packets only
        for i, p in enumerate(packets):
            if i:
                sock.sendto(p, server)
            time.sleep(0.0003)
            if on_progress:
                on_progress(i, total)
        status = "SENT (no reliability - delivery unknown)"
    else:
        raw.settimeout(timeout)
        try:
            for seq, pkt in enumerate(packets):
                attempts = 0
                while True:
                    sock.sendto(pkt, server)                    # send + start timer
                    attempts += 1
                    deadline = time.time() + timeout
                    got = False
                    while time.time() < deadline:               # wait for matching ACK
                        raw.settimeout(max(0.001, deadline - time.time()))
                        try:
                            reply, _ = raw.recvfrom(2048)
                        except socket.timeout:
                            break
                        except ConnectionResetError:            # Windows ICMP quirk
                            time.sleep(0.01)
                            continue
                        r = parse_packet(reply)
                        if r and r[0] == T_ACK and r[2] == seq:
                            got = True
                            acks += 1
                            break
                        # corrupted / stale ACK -> ignore and keep waiting
                    if got:
                        break
                    retrans += 1
                    if on_event:
                        on_event({"kind": "timeout", "seq": seq})
                    log(f"[CLIENT] Timeout on packet {seq} - retransmitting (attempt {attempts + 1})")
                    if attempts > max_retries:
                        raise TimeoutError(f"packet {seq} not acknowledged after {max_retries} retries")
                if on_progress:
                    on_progress(seq, total)
        except TimeoutError as e:
            status = f"FAILED ({e})"
    elapsed = max(time.time() - t0, 1e-9)
    raw.close()

    stats = {
        "file_name": meta["name"], "file_size": len(data), "packets": total,
        "packets_sent_total": sock.sent, "retransmissions": retrans, "acks_received": acks,
        "dropped_by_simulation": sock.dropped, "corrupted_by_simulation": sock.corrupted,
        "transfer_time_s": round(elapsed, 3),
        "speed_KBps": round(len(data) / 1024 / elapsed, 1), "status": status,
    }
    log("[CLIENT] ---------------- TRANSFER STATISTICS ----------------")
    for k, v in stats.items():
        log(f"[CLIENT]  {k:25s}: {v}")
    return stats


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="RFTP client (sender)")
    ap.add_argument("file")
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=9000)
    ap.add_argument("--chunk", type=int, default=DEFAULT_CHUNK)
    ap.add_argument("--timeout", type=float, default=0.3, help="ACK timeout in seconds")
    ap.add_argument("--retries", type=int, default=15)
    ap.add_argument("--loss", type=float, default=0.0, help="data packet loss probability (0-1)")
    ap.add_argument("--corrupt", type=float, default=0.0, help="data corruption probability (0-1)")
    ap.add_argument("--delay", type=float, default=0.0, help="max random delay per packet (s)")
    ap.add_argument("--no-reliability", action="store_true", help="plain UDP baseline (no ACK/retransmit)")
    a = ap.parse_args()
    send_file(a.file, a.host, a.port, a.chunk, a.timeout, a.retries, a.loss, a.corrupt,
              a.delay, reliable=not a.no_reliability)
