from dnslib import DNSRecord, DNSQuestion, QTYPE, RCODE, RR, A, EDNS0
from builder import (parse_query, hit_reply, notimp, servfail, set_id,
                     parse_response)


def query(name="www.lab.test", qtype="A", edns=False, qid=0x1234):
    q = DNSRecord(q=DNSQuestion(name, getattr(QTYPE, qtype)))
    q.header.id = qid
    if edns:
        q.add_ar(EDNS0(udp_len=4096))
    return q.pack()


def test_parse_supported_and_unsupported():
    q = parse_query(query("WWW.Lab.Test", "A"))
    assert q.name == "www.lab.test" and q.supported and q.id == 0x1234
    assert parse_query(query(qtype="AAAA")).supported
    assert not parse_query(query(qtype="MX")).supported


def test_malformed_returns_none():
    assert parse_query(b"\x00\x01garbage") is None
    assert parse_query(b"") is None


def test_hit_reply_keeps_id_question_and_ttl_no_opt():
    data = query(edns=True)
    rr = RR("www.lab.test", QTYPE.A, ttl=7, rdata=A("10.0.0.1"))
    r = DNSRecord.parse(hit_reply(data, [rr]))
    assert r.header.id == 0x1234 and r.header.qr == 1 and r.header.ra == 1
    assert r.header.rcode == RCODE.NOERROR
    assert str(r.q.qname) == "www.lab.test." and r.rr[0].ttl == 7
    assert str(r.rr[0].rdata) == "10.0.0.1"
    assert r.ar == []                      # no OPT in the reply


def test_notimp_and_servfail():
    assert DNSRecord.parse(notimp(query(qtype="MX"))).header.rcode == RCODE.NOTIMP
    s = DNSRecord.parse(servfail(query()))
    assert s.header.rcode == RCODE.SERVFAIL and s.header.id == 0x1234


def test_set_id_and_parse_response():
    data = query()
    resp = DNSRecord.parse(data).reply()
    resp.add_answer(RR("www.lab.test", QTYPE.A, ttl=5, rdata=A("10.0.0.1")))
    packed = set_id(resp.pack(), 0xBEEF)
    rid, rcode, rrs, key = parse_response(packed)
    assert rid == 0xBEEF and rcode == RCODE.NOERROR and len(rrs) == 1
    assert key == ("www.lab.test", QTYPE.A, 1)
    assert parse_response(b"junk") is None
