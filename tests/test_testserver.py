import socket, threading, time
from dnslib import DNSRecord, QTYPE, RCODE
import test_dns_server as ts

REC = "testserver/records_primary.json"


def free_port():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM); s.bind(("127.0.0.1", 0))
    p = s.getsockname()[1]; s.close(); return p


def ask(port, name="www.lab.test", qtype="A", timeout=1.0):
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM); s.settimeout(timeout)
    s.sendto(DNSRecord.question(name, qtype).pack(), ("127.0.0.1", port))
    try:
        return DNSRecord.parse(s.recvfrom(4096)[0])
    except socket.timeout:
        return None


def start():
    import os
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    st = ts.State(os.path.join(root, REC)); port = free_port()
    threading.Thread(target=ts.serve_udp, args=(st, port, "127.0.0.1"), daemon=True).start()
    time.sleep(0.2); return st, port


def test_answers_and_nxdomain():
    st, port = start()
    r = ask(port)
    assert r.rr[0].ttl == 30 and str(r.rr[0].rdata) == "10.0.0.1"
    assert ask(port, qtype="AAAA").rr[0].rtype == QTYPE.AAAA
    assert ask(port, "nope.lab.test").header.rcode == RCODE.NXDOMAIN


def test_runtime_record_change_off_switch_and_delay():
    st, port = start()
    assert "ttl=10" in ts.control_command(st, "set www.lab.test A 10.0.0.5 10")
    r = ask(port)
    assert str(r.rr[0].rdata) == "10.0.0.5" and r.rr[0].ttl == 10
    ts.control_command(st, "stop")
    assert ask(port, timeout=0.5) is None          # silent = looks down
    ts.control_command(st, "start")
    ts.control_command(st, "delay 200")
    t0 = time.time(); assert ask(port) is not None
    assert time.time() - t0 >= 0.2
