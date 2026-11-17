import socket
import threading
import time
from dnslib import DNSRecord, DNSQuestion, QTYPE, RCODE, RR, A

from builder import parse_query, hit_reply, notimp, servfail, set_id
from cache import DNSCache
from listener import DNSForwarder, ForwarderStats, QueryLogger, _process_query, _handle_upstream_reply
from pending_table import PendingTable
from selector import UpstreamSelector


def test_forwarder_stats_and_logger(tmp_path):
    log_file = str(tmp_path / "queries.log")
    ql = QueryLogger(log_file=log_file)
    sel = UpstreamSelector(("127.0.0.1", 5301), ("127.0.0.1", 5302))
    stats = ForwarderStats(sel)

    stats.record_hit(1.2)
    stats.record_miss(15.4)
    stats.record_upstream_query(("127.0.0.1", 5301))
    stats.record_unsupported(0.5)

    assert stats.total_queries == 3
    assert stats.hits == 1
    assert stats.misses == 1
    assert stats.errors == 1

    summary = stats.get_summary()
    assert "DNS FORWARDER STATISTICS" in summary
    assert "Cache Hits:             1 (33.3%)" in summary
    assert "127.0.0.1:5301 (UP  ):  1" in summary

    ql.log(
        client=("127.0.0.1", 45678),
        name="www.lab.test",
        qtype="A",
        result="HIT",
        upstream=None,
        latency_ms=1.2,
        rcode="NOERROR",
    )
    with open(log_file) as f:
        content = f.read()
    assert "client=127.0.0.1:45678" in content
    assert "name=www.lab.test" in content
    assert "result=HIT" in content
    assert "rcode=NOERROR" in content


def test_process_query_malformed_and_unsupported():
    pending = PendingTable()
    cache = DNSCache()
    sel = UpstreamSelector(("127.0.0.1", 5301), ("127.0.0.1", 5302))
    stats = ForwarderStats(sel)

    # Malformed packet -> dropped
    client_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    upstream_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    _process_query(
        raw=b"\x00garbage",
        client_addr=("127.0.0.1", 1234),
        pending=pending,
        cache=cache,
        upstream_selector=sel,
        client_sock=client_sock,
        upstream_sock=upstream_sock,
        stats=stats,
    )
    assert stats.mismatch_drops == 1
    assert pending.size() == 0

    client_sock.close()
    upstream_sock.close()


def test_handle_upstream_reply_strict_matching():
    pending = PendingTable()
    cache = DNSCache()
    sel = UpstreamSelector(("127.0.0.1", 5301), ("127.0.0.1", 5302))
    stats = ForwarderStats(sel)

    uid = 0xABCD
    client_addr = ("127.0.0.1", 12345)
    pending.add(
        upstream_id=uid,
        client_addr=client_addr,
        client_id=0x1234,
        questions=[("www.lab.test", QTYPE.A, 1)],
        raw_query=b"",
        upstream_addr=("127.0.0.1", 5301),
        retried=False,
        qname="www.lab.test",
        qtype=QTYPE.A,
        qclass=1,
    )

    client_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    # 1. Mismatch source IP:port
    req = DNSRecord.question("www.lab.test", "A")
    reply = req.reply()
    reply.add_answer(RR("www.lab.test", QTYPE.A, ttl=30, rdata=A("10.0.0.1")))
    reply.header.id = uid
    raw_reply = reply.pack()

    _handle_upstream_reply(
        raw=raw_reply,
        upstream_addr=("127.0.0.1", 5302),  # Wrong upstream port!
        pending=pending,
        cache=cache,
        client_sock=client_sock,
        upstream_selector=sel,
        stats=stats,
    )
    assert stats.mismatch_drops == 1
    assert pending.get(uid) is not None  # Not removed!

    # 2. Mismatch question name
    wrong_q_reply = DNSRecord.question("other.lab.test", "A").reply()
    wrong_q_reply.header.id = uid
    _handle_upstream_reply(
        raw=wrong_q_reply.pack(),
        upstream_addr=("127.0.0.1", 5301),
        pending=pending,
        cache=cache,
        client_sock=client_sock,
        upstream_selector=sel,
        stats=stats,
    )
    assert stats.mismatch_drops == 2
    assert pending.get(uid) is not None

    client_sock.close()
