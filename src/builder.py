"""Parser / reply builder (compulsory features C1, C2).

All functions work on raw bytes so the listener stays simple.
    q = parse_query(data)            # ParsedQuery, or None if malformed -> drop
    if q.supported: ...              # A/AAAA, class IN
    else: reply = notimp(data)
    reply = hit_reply(data, rrs)     # cache hit, client's ID, remaining TTL
    reply = servfail(data)
Replies never contain an OPT record (EDNS ignored).
"""
from dataclasses import dataclass

from dnslib import DNSRecord, QTYPE, CLASS, RCODE, OPCODE

SUPPORTED_TYPES = (QTYPE.A, QTYPE.AAAA)


@dataclass
class ParsedQuery:
    record: DNSRecord
    id: int
    name: str        # lowercased, no trailing dot
    qtype: int
    qclass: int

    @property
    def supported(self):
        return self.qtype in SUPPORTED_TYPES and self.qclass == CLASS.IN


def parse_query(data):
    """Return ParsedQuery for a well-formed standard query, else None."""
    try:
        rec = DNSRecord.parse(data)
    except Exception:
        return None
    h = rec.header
    if h.qr != 0 or h.opcode != OPCODE.QUERY or len(rec.questions) != 1:
        return None
    q = rec.q
    return ParsedQuery(rec, h.id, str(q.qname).lower().rstrip("."),
                       q.qtype, q.qclass)


def _base_reply(request_bytes, rcode):
    req = DNSRecord.parse(request_bytes)
    reply = req.reply()            # copies ID, RD and the question section
    reply.header.ra = 1
    reply.header.rcode = rcode
    return reply


def hit_reply(request_bytes, records):
    reply = _base_reply(request_bytes, RCODE.NOERROR)
    for rr in records:
        reply.add_answer(rr)
    return reply.pack()


def notimp(request_bytes):
    return _base_reply(request_bytes, RCODE.NOTIMP).pack()


def servfail(request_bytes):
    return _base_reply(request_bytes, RCODE.SERVFAIL).pack()


# ---- helpers for the miss path (used by the listener / pending table) ----

def set_id(packet, new_id):
    """Return packet bytes with the 16-bit DNS ID replaced."""
    return new_id.to_bytes(2, "big") + packet[2:]


def parse_response(data):
    """Return (id, rcode, answer_rrs, question_key) or None if malformed."""
    try:
        rec = DNSRecord.parse(data)
    except Exception:
        return None
    if rec.header.qr != 1 or len(rec.questions) != 1:
        return None
    q = rec.q
    return (rec.header.id, rec.header.rcode, rec.rr,
            (str(q.qname).lower().rstrip("."), q.qtype, q.qclass))
