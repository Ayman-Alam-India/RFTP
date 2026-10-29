"""
ns2_analysis.py - Runs the NS2 simulation AND the Python-socket implementation of RFTP
for several packet-loss rates, parses the NS2 trace file, and compares both against
the theoretical stop-and-wait model.  Produces results/comparison.csv and results/comparison.png

Run:  python ns2_analysis.py            (needs `ns` on PATH for the NS2 part)
      python ns2_analysis.py --ns-only  / --quick
"""
import argparse
import csv
import math
import os
import re
import shutil
import statistics
import subprocess
import tempfile
import time

HERE = os.path.dirname(os.path.abspath(__file__))
TCL = os.path.join(HERE, "ns2", "rftp_sim.tcl")


# ------------------------------------------------------------------- NS2 side
def parse_trace(path, chunk):
    """Count packet events in the NS2 trace file (+ enqueue, r receive, d drop)."""
    c = {"data_delivered": 0, "ack_delivered": 0, "data_dropped": 0, "ack_dropped": 0}
    with open(path) as f:
        for line in f:
            p = line.split()
            if len(p) < 6 or p[0] not in ("r", "d"):
                continue
            is_ack = int(p[5]) == 40               # ACKs are 40-byte packets
            key = ("ack_" if is_ack else "data_") + ("delivered" if p[0] == "r" else "dropped")
            c[key] += 1
    return c


def run_ns2(ns_bin, loss, packets, timeout, seed, bw="1Mb", delay="10ms", chunk=1024, nam=False):
    work = tempfile.mkdtemp()
    cmd = [ns_bin, TCL, str(loss), str(packets), str(timeout), bw, delay, str(chunk), str(seed)]
    if nam:
        cmd.append("nam")
    out = subprocess.run(cmd, cwd=work, capture_output=True, text=True, timeout=600)
    m = re.search(r"RFTP_STATS (.*)", out.stdout)
    if not m:
        raise RuntimeError("NS2 produced no stats:\n" + out.stdout + out.stderr)
    st = dict(kv.split("=") for kv in m.group(1).split())
    st = {k: (float(v) if re.fullmatch(r"-?[\d.]+(e-?\d+)?", v) else v) for k, v in st.items()}
    st.update(parse_trace(os.path.join(work, "rftp_ns2.tr"), chunk))
    if nam:
        for f in ("rftp.nam", "rftp_ns2.tr"):
            if os.path.exists(os.path.join(work, f)):
                os.makedirs(os.path.join(HERE, "results"), exist_ok=True)
                shutil.copy(os.path.join(work, f), os.path.join(HERE, "results", f))
    shutil.rmtree(work, ignore_errors=True)
    return st


# ---------------------------------------------------------------- socket side
def run_socket(loss, packets, port, timeout=0.02, chunk=1024):
    from rftp_server import RFTPServer
    from rftp_client import send_file
    tmp = tempfile.mkdtemp()
    src = os.path.join(tmp, "f.bin")
    with open(src, "wb") as f:
        f.write(os.urandom(packets * chunk))
    srv = RFTPServer("127.0.0.1", port, os.path.join(tmp, "out"), loss=loss, log=lambda m: None)
    srv.start()
    time.sleep(0.15)
    st = send_file(src, "127.0.0.1", port, chunk=chunk, timeout=timeout, max_retries=60,
                   loss=loss, log=lambda m: None)
    time.sleep(0.2)
    res = getattr(srv, "last_result", {"intact": False})
    srv.stop()
    shutil.rmtree(tmp, ignore_errors=True)
    st["intact"] = res["intact"]
    return st


# ------------------------------------------------------------------ theory
def theory_retrans(loss, packets):
    """Stop-and-wait, loss p on data path and ACK path: P(success/attempt)=(1-p)^2.
    Expected transmissions per packet = 1/(1-p)^2.  Total packets = data + 1 metadata."""
    q = (1 - loss) ** 2
    return (packets + 1) * (1 / q - 1)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--ns-bin", default="ns")
    ap.add_argument("--packets", type=int, default=50)
    ap.add_argument("--quick", action="store_true", help="fewer runs")
    ap.add_argument("--no-socket", action="store_true")
    ap.add_argument("--nam", action="store_true", help="also save a NAM animation trace")
    a = ap.parse_args()
    losses = [0.0, 0.05, 0.1, 0.2, 0.3, 0.4]
    seeds = [1, 2, 3] if a.quick else [1, 2, 3, 4, 5, 6, 7, 8]
    have_ns = shutil.which(a.ns_bin) is not None
    if not have_ns:
        print(f"!! '{a.ns_bin}' not found on PATH - NS2 columns will be skipped "
              f"(install NS2: sudo apt install ns2)")
    rows = []
    port = 9500
    for p in losses:
        row = {"loss": p, "theory_retrans": round(theory_retrans(p, a.packets), 1)}
        if have_ns:
            runs = [run_ns2(a.ns_bin, p, a.packets, 0.3, s) for s in seeds]
            row["ns2_retrans"] = round(statistics.mean(r["retransmissions"] for r in runs), 1)
            row["ns2_time_s"] = round(statistics.mean(r["transfer_time"] for r in runs), 2)
            row["ns2_data_dropped"] = round(statistics.mean(r["data_dropped"] for r in runs), 1)
            row["ns2_ack_dropped"] = round(statistics.mean(r["ack_dropped"] for r in runs), 1)
            row["ns2_completed"] = all(r["status"] == "COMPLETED" for r in runs)
            if a.nam and p == 0.2:
                run_ns2(a.ns_bin, p, a.packets, 0.3, 1, nam=True)
        if not a.no_socket:
            runs = []
            for s in range(3 if a.quick else 5):
                port += 1
                runs.append(run_socket(p, a.packets, port))
            row["socket_retrans"] = round(statistics.mean(r["retransmissions"] for r in runs), 1)
            row["socket_time_s"] = round(statistics.mean(r["transfer_time_s"] for r in runs), 2)
            row["socket_file_intact"] = all(r["intact"] for r in runs)
        rows.append(row)
        print(row)

    os.makedirs(os.path.join(HERE, "results"), exist_ok=True)
    keys = list(rows[0].keys())
    for r in rows:
        for k in r:
            if k not in keys:
                keys.append(k)
    with open(os.path.join(HERE, "results", "comparison.csv"), "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=keys)
        w.writeheader()
        w.writerows(rows)

    print("\n" + "=" * 86)
    print(f"RETRANSMISSIONS vs LOSS  ({a.packets} data packets + 1 metadata; loss on BOTH data and ACK path)")
    print("=" * 86)
    print(f"{'Loss':>6} | {'Theory':>8} | {'NS2 sim':>8} | {'Python socket':>13} | {'NS2 time(s)':>11} | File intact")
    for r in rows:
        print(f"{r['loss']:>6.0%} | {r['theory_retrans']:>8} | {r.get('ns2_retrans', '-'):>8} | "
              f"{r.get('socket_retrans', '-'):>13} | {r.get('ns2_time_s', '-'):>11} | "
              f"{r.get('socket_file_intact', '-')}")

    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        fig, ax = plt.subplots(1, 2, figsize=(12, 4.5))
        x = [r["loss"] * 100 for r in rows]
        ax[0].plot(x, [r["theory_retrans"] for r in rows], "k--", label="Theory  (N+1)(1/(1-p)^2 - 1)")
        if have_ns:
            ax[0].plot(x, [r["ns2_retrans"] for r in rows], "o-", label="NS2 simulation")
        if not a.no_socket:
            ax[0].plot(x, [r["socket_retrans"] for r in rows], "s-", label="Python sockets")
        ax[0].set_xlabel("Packet loss (%)"); ax[0].set_ylabel("Retransmissions")
        ax[0].set_title("RFTP: retransmissions vs packet loss"); ax[0].grid(alpha=.3); ax[0].legend()
        if have_ns:
            ax[1].plot(x, [r["ns2_time_s"] for r in rows], "o-", color="tab:red", label="NS2 (1 Mbps, 10 ms, timeout 0.3 s)")
        if not a.no_socket:
            ax[1].plot(x, [r["socket_time_s"] for r in rows], "s-", color="tab:green", label="Python sockets (localhost, timeout 0.02 s)")
        ax[1].set_xlabel("Packet loss (%)"); ax[1].set_ylabel("Transfer time (s)")
        ax[1].set_title("RFTP: transfer time vs packet loss"); ax[1].grid(alpha=.3); ax[1].legend()
        plt.tight_layout()
        plt.savefig(os.path.join(HERE, "results", "comparison.png"), dpi=130)
        print("\nSaved results/comparison.csv and results/comparison.png")
    except ImportError:
        print("\n(matplotlib not installed - chart skipped; CSV saved)")


if __name__ == "__main__":
    main()
