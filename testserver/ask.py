"""Tiny dig replacement for the lab.

    python testserver/ask.py www.lab.test                 # A record via the forwarder (port 5300)
    python testserver/ask.py www.lab.test AAAA
    python testserver/ask.py www.lab.test MX              # unsupported type -> NOTIMP
    python testserver/ask.py www.lab.test A --port 5301   # ask the primary directly
"""
import argparse
import socket
import sys
import time

from dnslib import DNSRecord, QTYPE, RCODE

ap = argparse.ArgumentParser()
ap.add_argument("name")
ap.add_argument("qtype", nargs="?", default="A")
ap.add_argument("--server", default="127.0.0.1")
ap.add_argument("--port", type=int, default=5300)
ap.add_argument("--timeout", type=float, default=3.0)
a = ap.parse_args()

query = DNSRecord.question(a.name, a.qtype)
sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
sock.settimeout(a.timeout)
start = time.time()
sock.sendto(query.pack(), (a.server, a.port))
try:
    data, addr = sock.recvfrom(4096)
except socket.timeout:
    print(f"NO RESPONSE from {a.server}:{a.port} after {a.timeout}s")
    sys.exit(1)
ms = (time.time() - start) * 1000
reply = DNSRecord.parse(data)
print(f"status={RCODE[reply.header.rcode]}  id={reply.header.id}  "
      f"answers={len(reply.rr)}  time={ms:.0f} ms  from={addr[0]}:{addr[1]}")
for rr in reply.rr:
    print(f"  {rr.rname}  TTL={rr.ttl}  {QTYPE[rr.rtype]}  {rr.rdata}")