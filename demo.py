"""
demo.py - one-command demo: runs server + client on localhost and compares
plain UDP vs RFTP under packet loss / corruption.   Usage: python demo.py
"""
import os, time, random, tempfile
from rftp_server import RFTPServer
from rftp_client import send_file

random.seed(1)
tmp = tempfile.mkdtemp()
src = os.path.join(tmp, "sample.bin")
with open(src, "wb") as f:
    f.write(os.urandom(150 * 1024))          # 150 KB test file = 150 packets

def run(title, port, reliable, loss, corrupt):
    print("\n" + "=" * 72 + f"\n{title}\n" + "=" * 72)
    out = os.path.join(tmp, f"out{port}")
    srv = RFTPServer("127.0.0.1", port, out, loss=loss, corrupt=corrupt,
                     log=lambda m: None if "Duplicate" in m or "Corrupted" in m else print(m))
    srv.start(); time.sleep(0.2)
    st = send_file(src, "127.0.0.1", port, timeout=0.02, loss=loss, corrupt=corrupt,
                   reliable=reliable, log=lambda m: None if "Timeout on" in m else print(m))
    time.sleep(3.0 if not reliable else 0.3)
    res = getattr(srv, "last_result", None)
    srv.stop()
    return st, res

rows = []
st, r = run("TEST 1: Plain UDP, 0% loss (baseline)", 9101, False, 0.0, 0.0);  rows.append(("Plain UDP", "0%", st, r))
st, r = run("TEST 2: Plain UDP, 20% loss  (shows WHY reliability is needed)", 9102, False, 0.2, 0.0); rows.append(("Plain UDP", "20%", st, r))
st, r = run("TEST 3: RFTP, 20% loss + 5% corruption", 9103, True, 0.2, 0.05); rows.append(("RFTP", "20%+5%corr", st, r))
st, r = run("TEST 4: RFTP, 40% loss", 9104, True, 0.4, 0.0); rows.append(("RFTP", "40%", st, r))

print("\n" + "=" * 72 + "\nSUMMARY\n" + "=" * 72)
print(f"{'Mode':10}{'Loss':13}{'Pkts rcvd':>10}{'Missing':>9}{'Retrans':>9}{'Time(s)':>9}{'KB/s':>8}  File intact?")
for mode, loss, st, r in rows:
    print(f"{mode:10}{loss:13}{r['packets_received']:>7}/{r['packets_expected']:<3}{r['packets_missing']:>6}"
          f"{st['retransmissions']:>9}{st['transfer_time_s']:>9}{st['speed_KBps']:>8}  {'YES' if r['intact'] else 'NO'}")
