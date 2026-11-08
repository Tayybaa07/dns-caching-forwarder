"""Controlled test DNS upstream (compulsory feature C11).

Serves A/AAAA records from a JSON file over UDP, with run-time control over a
local TCP port: change records/TTLs, add artificial delay, switch on/off.
"OFF" silently drops queries (the process stays up), which looks to the
forwarder exactly like an unreachable upstream (timeouts).

    python testserver/test_dns_server.py --port 5301 --records testserver/records_primary.json
    python testserver/ctl.py 6301 delay 100
"""
import argparse
import json
import socket
import socketserver
import threading
import time
from datetime import datetime

from dnslib import DNSRecord, QTYPE, RCODE, RR, A, AAAA


class State:
    def __init__(self, records_file, delay_ms=0):
        self.lock = threading.Lock()
        self.records = {}
        self.delay_ms = delay_ms
        self.enabled = True
        self.queries = 0
        if records_file:
            with open(records_file) as f:
                for name, rec in json.load(f).items():
                    self.records[name.lower().rstrip(".")] = rec


def log(msg):
    print(f"{datetime.now():%H:%M:%S.%f}"[:-3], msg, flush=True)


def answer(state, data):
    """Build the reply for a query, or None to stay silent."""
    try:
        req = DNSRecord.parse(data)
    except Exception:
        return None
    if req.header.qr != 0 or len(req.questions) != 1:
        return None
    reply = req.reply()
    qname = str(req.q.qname).lower().rstrip(".")
    qtype = QTYPE[req.q.qtype]
    with state.lock:
        entry = state.records.get(qname)
        if entry is None:
            reply.header.rcode = RCODE.NXDOMAIN
            return reply.pack(), qname, qtype, "NXDOMAIN"
        ttl = int(entry.get("ttl", 30))
        if qtype == "A":
            for ip in entry.get("A", []):
                reply.add_answer(RR(req.q.qname, QTYPE.A, ttl=ttl, rdata=A(ip)))
        elif qtype == "AAAA":
            for ip in entry.get("AAAA", []):
                reply.add_answer(RR(req.q.qname, QTYPE.AAAA, ttl=ttl, rdata=AAAA(ip)))
        else:
            reply.header.rcode = RCODE.NOTIMP
    return reply.pack(), qname, qtype, RCODE[reply.header.rcode]


def serve_udp(state, port, host):
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind((host, port))
    log(f"DNS test server on udp {host}:{port}")

    def handle(data, addr):
        with state.lock:
            enabled, delay = state.enabled, state.delay_ms
        if not enabled:
            log(f"DROPPED (server off) from {addr[0]}:{addr[1]}")
            return
        result = answer(state, data)
        if result is None:
            return
        packet, qname, qtype, rcode = result
        with state.lock:
            state.queries += 1
            n = state.queries
        if delay:
            time.sleep(delay / 1000.0)
        sock.sendto(packet, addr)
        log(f"#{n} {addr[0]}:{addr[1]} {qname} {qtype} -> {rcode} (delay {delay} ms)")

    while True:
        data, addr = sock.recvfrom(4096)
        threading.Thread(target=handle, args=(data, addr), daemon=True).start()


def control_command(state, line):
    p = line.split()
    if not p:
        return "error: empty command"
    cmd = p[0].lower()
    with state.lock:
        if cmd == "delay" and len(p) == 2:
            state.delay_ms = int(p[1]); return f"delay={state.delay_ms} ms"
        if cmd in ("stop", "off"):
            state.enabled = False; return "server OFF (dropping queries)"
        if cmd in ("start", "on"):
            state.enabled = True; return "server ON"
        if cmd == "set" and len(p) >= 4:           # set <name> <A|AAAA> <ip> [ttl]
            name, rtype, ip = p[1].lower().rstrip("."), p[2].upper(), p[3]
            entry = state.records.setdefault(name, {"ttl": 30})
            entry[rtype] = [ip]
            if len(p) > 4:
                entry["ttl"] = int(p[4])
            return f"{name} {rtype} {ip} ttl={entry['ttl']}"
        if cmd == "ttl" and len(p) == 3:            # ttl <name> <seconds>
            state.records.setdefault(p[1].lower(), {})["ttl"] = int(p[2])
            return "ok"
        if cmd == "del" and len(p) == 2:
            state.records.pop(p[1].lower().rstrip("."), None); return "deleted"
        if cmd == "status":
            return json.dumps({"enabled": state.enabled, "delay_ms": state.delay_ms,
                               "queries": state.queries, "records": state.records})
    return "error: commands are delay <ms> | stop | start | set <name> <A|AAAA> <ip> [ttl] | ttl <name> <s> | del <name> | status"


def serve_control(state, port):
    class Handler(socketserver.StreamRequestHandler):
        def handle(self):
            line = self.rfile.readline().decode().strip()
            reply = control_command(state, line)
            log(f"CONTROL '{line}' -> {reply}")
            self.wfile.write((reply + "\n").encode())

    socketserver.ThreadingTCPServer.allow_reuse_address = True
    with socketserver.ThreadingTCPServer(("127.0.0.1", port), Handler) as srv:
        log(f"control port tcp 127.0.0.1:{port}")
        srv.serve_forever()


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--port", type=int, default=5301)
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--control-port", type=int, default=None,
                    help="default: DNS port + 1000")
    ap.add_argument("--records", default="testserver/records_primary.json")
    ap.add_argument("--delay", type=int, default=0, help="artificial delay in ms")
    a = ap.parse_args()
    st = State(a.records, a.delay)
    threading.Thread(target=serve_control, args=(st, a.control_port or a.port + 1000),
                     daemon=True).start()
    try:
        serve_udp(st, a.port, a.host)
    except KeyboardInterrupt:
        pass
