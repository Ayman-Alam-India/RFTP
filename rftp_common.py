"""
rftp_common.py  -  Shared modules for RFTP (Reliable File Transfer Protocol over UDP)

Covers report modules:
  7.3 Packet Management Module   (build / parse packets)
  7.7 Checksum / Error Detection (CRC-32)
  7.4 UDP helper with optional packet loss / corruption / delay simulation
"""
import random
import socket
import struct
import time
import zlib

# ---------------------------------------------------------------- packet format
# Header (10 bytes, network byte order):
#   1 byte  packet type   (1=META, 2=DATA, 3=ACK)
#   1 byte  flags         (bit0 = End-Of-File)
#   4 bytes sequence number
#   4 bytes CRC-32 checksum (over type+flags+seq+data)
# followed by the data (payload).
HEADER = struct.Struct("!BBII")
T_META, T_DATA, T_ACK = 1, 2, 3
F_EOF = 0x01
DEFAULT_CHUNK = 1024

TYPE_NAMES = {T_META: "META", T_DATA: "DATA", T_ACK: "ACK"}


def checksum(ptype: int, flags: int, seq: int, payload: bytes) -> int:
    """CRC-32 over the packet contents (everything except the checksum field)."""
    return zlib.crc32(struct.pack("!BBI", ptype, flags, seq) + payload) & 0xFFFFFFFF


def build_packet(ptype: int, seq: int, payload: bytes = b"", flags: int = 0) -> bytes:
    """Sender side: put sequence number, data and checksum into one packet."""
    return HEADER.pack(ptype, flags, seq, checksum(ptype, flags, seq, payload)) + payload


def parse_packet(raw: bytes):
    """
    Receiver side: split the packet into fields and validate the checksum.
    Returns (ptype, flags, seq, payload) or None if the packet is corrupted.
    """
    if len(raw) < HEADER.size:
        return None
    ptype, flags, seq, crc = HEADER.unpack(raw[:HEADER.size])
    payload = raw[HEADER.size:]
    if ptype not in TYPE_NAMES or crc != checksum(ptype, flags, seq, payload):
        return None
    return ptype, flags, seq, payload


# ------------------------------------------------- network condition simulator
class LossySocket:
    """
    Wraps a UDP socket. Every outgoing datagram may be
      - dropped      (probability `loss`)      -> simulates packet loss
      - corrupted    (probability `corrupt`)   -> flips one byte, so CRC fails
      - delayed      (up to `delay` seconds)   -> simulates network latency
    """

    def __init__(self, sock: socket.socket, loss=0.0, corrupt=0.0, delay=0.0, hook=None):
        self.sock = sock
        self.loss = loss
        self.corrupt = corrupt
        self.delay = delay
        self.hook = hook            # hook(original_bytes, status) - used by the web UI
        self.sent = 0
        self.dropped = 0
        self.corrupted = 0

    def sendto(self, data: bytes, addr):
        self.sent += 1
        status = "delivered"
        wire = data
        if self.loss and random.random() < self.loss:
            status = "dropped"
        elif self.corrupt and data and random.random() < self.corrupt:
            status = "corrupted"
            b = bytearray(data)
            b[random.randrange(len(b))] ^= 0xFF
            wire = bytes(b)
        if self.hook:
            self.hook(data, status, wire)       # original bytes, fate, bytes on the wire
        if status == "dropped":
            self.dropped += 1
            return
        if status == "corrupted":
            self.corrupted += 1
        if self.delay:
            time.sleep(random.uniform(0, self.delay))
        self.sock.sendto(wire, addr)

    def __getattr__(self, name):          # recvfrom, settimeout, close, ...
        return getattr(self.sock, name)
