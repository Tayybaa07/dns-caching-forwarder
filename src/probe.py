"""Recovery probe sender for the health prober (compulsory feature C9).

    from selector import UpstreamSelector, HealthProber
    from probe import make_probe_fn
    prober = HealthProber(selector, make_probe_fn("probe.lab.test"))
    prober.start()

The probe uses its own short-lived socket, so it never touches the listener's
pending table. It succeeds only if a NOERROR reply with an answer comes back
from the right address with the right ID.
"""
import random
import socket

from dnslib import DNSRecord, RCODE


def make_probe_fn(name="probe.lab.test", timeout=1.0):
    def send_probe(upstream):
        host, port = upstream
        query = DNSRecord.question(name)
        qid = random.randint(0, 0xFFFF)
        query.header.id = qid
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.settimeout(timeout)
        try:
            sock.sendto(query.pack(), (host, port))
            data, addr = sock.recvfrom(4096)
            if addr[0] != host or addr[1] != port:
                return False
            reply = DNSRecord.parse(data)
            return (reply.header.id == qid
                    and reply.header.rcode == RCODE.NOERROR
                    and len(reply.rr) > 0)
        except (socket.timeout, OSError, Exception):
            return False
        finally:
            sock.close()
    return send_probe
