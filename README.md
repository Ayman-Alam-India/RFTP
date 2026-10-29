# RFTP Studio - Reliable File Transfer Protocol over UDP
Computer Networks (BCSE308P) - Python sockets + NS2 simulation + web frontend

## QUICK START (frontend)
    python app.py
Opens http://127.0.0.1:8000 (standard library only - no pip install needed).

| Tab | What it does |
|---|---|
| Live Transfer | Pick any file (or a random sample), choose RFTP or plain UDP, set loss / corruption / delay / timeout. A REAL UDP transfer runs between a client and a server socket; a Packet Inspector shows the real bytes of each packet in flight (decoded header, CRC-32 verdict); the page replays every packet: DATA out, ACK back, packet bursts when lost, timeout ring, amber retransmissions, live counters, packet map, event log and a SHA-256 verdict. "Compare" runs plain UDP and RFTP back to back. |
| RFTP Protocol | The protocol itself: specification chips (layer, ARQ type, CRC-32, timeout, retries, header), real packets from the real code shown byte by byte (META, DATA, last DATA, ACK, corrupted DATA), a CRC-32 check computed live in the browser, client and server state machines, and the sender/receiver algorithm. |
| NS2 Simulation | Runs ns2/rftp_sim.tcl in NS2, converts the NS2 trace file into packet events and replays them (simulated clock). Shows the raw trace lines and lets you download the .tr file. Needs NS2 (Ubuntu/WSL: sudo apt install ns2 nam). |
| Analysis | NS2 vs Python sockets vs theory charts + table (results/comparison.csv). Button re-runs the whole analysis. |
| How it works | Architecture, packet format, stop-and-wait steps. |

Files: app.py (backend), web/index.html (frontend). Everything below is the protocol code and command-line tools.

---
Computer Networks (BCSE308P) - Python sockets + NS2 simulation

## What is in the project
| File | Purpose |
|---|---|
| rftp_common.py | Packet format (seq + data + CRC-32), loss/corruption/delay simulator   [7.3, 7.4, 7.7] |
| rftp_client.py | Sender: segmentation, ACK wait, timeout & retransmission, stats   [7.1, 7.2, 7.5, 7.6, 7.10] |
| rftp_server.py | Receiver: checksum check, duplicate handling, ACKs, reconstruction + SHA-256 check   [7.4, 7.5, 7.8, 7.9] |
| rftp_gui.py | Tkinter GUI (file picker, progress bar, live log) |
| demo.py | Plain UDP vs RFTP under loss (real sockets, localhost) |
| ns2/rftp_sim.tcl | NS2 simulation of the same stop-and-wait protocol over a lossy link |
| ns2_analysis.py | Runs NS2 + sockets for loss 0-40%, parses the NS2 trace, compares with theory, makes CSV + graph |
| results/ | comparison.csv, comparison.png, rftp_ns2.tr (NS2 trace), rftp.nam (NAM animation) |

## Part A - Python socket implementation
    python demo.py                                            # quickest demo
    python rftp_server.py --port 9000 --out received          # terminal 1
    python rftp_client.py file.pdf --port 9000 --loss 0.2 --corrupt 0.05   # terminal 2
    python rftp_client.py file.pdf --port 9000 --loss 0.2 --no-reliability # plain UDP baseline
    python rftp_gui.py                                        # GUI

## Part B - NS2 simulation
NS2 needs Linux (Ubuntu / WSL / VM):   sudo apt install ns2 nam

    cd ns2
    ns rftp_sim.tcl 0.2 50 0.3 1Mb 10ms 1024 1 nam
    #  args: loss  packets  timeout  bandwidth  delay  chunk_bytes  seed  [nam]
    nam rftp.nam                                              # animation (packets dropping / retransmitted)

Topology: Client (n0) <-- 1 Mbps, 10 ms duplex link --> Server (n1), ErrorModel (random loss) on both directions.
The protocol is coded in Tcl on top of Agent/Message (UDP-style datagrams): DATA seq -> server stores/ignores dup
-> ACK seq -> client sends next, or on timer expiry retransmits. Output line `RFTP_STATS ...` gives the statistics.

## Part C - Comparison (NS2 vs sockets vs theory)
    python ns2_analysis.py            # full run (~1-2 min); add --quick for a faster one
Theory: with loss p on both data and ACK path, P(success per attempt) = (1-p)^2, so
expected retransmissions = (N+1) * (1/(1-p)^2 - 1).  Results from NS2 and from real sockets both follow this curve.

Sample result (50 packets): retransmissions at 20% loss: theory 28.7, NS2 31.7, sockets 24.7; at 40%: 90.7 / 100 / 97.7.
Transfer time differs between NS2 and sockets because NS2 models a 1 Mbps/10 ms link (timeout 0.3 s) while sockets run on localhost (timeout 0.02 s).

## Packet format (10-byte header)
type(1) | flags(1, bit0 = EOF) | sequence number(4) | CRC-32(4) | data     Types: META (seq 0), DATA (1..N), ACK

## Likely viva questions
- Why UDP? Connectionless, no built-in reliability, so reliability can be built and studied at the application layer.
- Why re-ACK duplicates? If the ACK was lost the client retransmits; without a re-ACK it would retry forever.
- Why stop-and-wait? Simplest ARQ, matches the report. One packet per RTT -> low throughput. Improvement: Go-Back-N / Selective Repeat.
- Why CRC-32? Detects corruption (incl. burst errors); corrupted packets are discarded with no ACK -> timeout -> retransmit.
- Why NS2 as well as sockets? Sockets prove it works on a real OS; NS2 lets us control bandwidth, delay and loss precisely, look at the trace file and animate with NAM.
- How is loss done in NS2? ErrorModel (unit = packet, rate = loss) attached to the link; dropped packets appear as `d` lines in the trace.
- How do you check correctness? Python: SHA-256 of received file == original. NS2: all N+1 packets acknowledged, server unique count = N+1.
- Not a TCP replacement: no congestion/flow control. Future work: sliding window, adaptive timeout (RTT estimation).
