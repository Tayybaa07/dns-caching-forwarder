import threading
from dnslib import RR, QTYPE, A
from cache import DNSCache


class FakeClock:
    def __init__(self): self.t = 100.0
    def __call__(self): return self.t


def rr(name="www.lab.test", ttl=10, ip="10.0.0.1"):
    return RR(name, QTYPE.A, ttl=ttl, rdata=A(ip))


def test_miss_then_hit_with_remaining_ttl():
    clk = FakeClock(); c = DNSCache(clock=clk)
    assert c.get("www.lab.test", QTYPE.A) is None
    assert c.put("www.lab.test", QTYPE.A, [rr(ttl=10)])
    assert c.get("www.lab.test", QTYPE.A)[0].ttl == 10
    clk.t += 4
    assert c.get("www.lab.test", QTYPE.A)[0].ttl == 6      # decreases
    clk.t += 5.5
    assert c.get("www.lab.test", QTYPE.A)[0].ttl == 1      # never 0 while live


def test_expires_after_ttl():
    clk = FakeClock(); c = DNSCache(clock=clk)
    c.put("www.lab.test", QTYPE.A, [rr(ttl=10)])
    clk.t += 10
    assert c.get("www.lab.test", QTYPE.A) is None
    assert len(c) == 0


def test_ttl_never_exceeds_original():
    clk = FakeClock(); c = DNSCache(clock=clk)
    c.put("a.lab.test", QTYPE.A, [rr("a.lab.test", ttl=5)])
    assert all(r.ttl <= 5 for r in c.get("a.lab.test", QTYPE.A))


def test_min_ttl_of_records_is_lifetime():
    clk = FakeClock(); c = DNSCache(clock=clk)
    c.put("www.lab.test", QTYPE.A, [rr(ttl=30, ip="10.0.0.1"), rr(ttl=5, ip="10.0.0.2")])
    clk.t += 6
    assert c.get("www.lab.test", QTYPE.A) is None


def test_ttl_zero_and_empty_not_cached():
    c = DNSCache()
    assert not c.put("z.lab.test", QTYPE.A, [rr("z.lab.test", ttl=0)])
    assert not c.put("z.lab.test", QTYPE.A, [])
    assert c.get("z.lab.test", QTYPE.A) is None


def test_key_is_case_insensitive_and_per_type():
    c = DNSCache()
    c.put("WWW.Lab.Test.", QTYPE.A, [rr()])
    assert c.get("www.lab.test", QTYPE.A) is not None
    assert c.get("www.lab.test", QTYPE.AAAA) is None


def test_disabled_cache_for_baseline_b2():
    c = DNSCache(enabled=False)
    assert not c.put("www.lab.test", QTYPE.A, [rr()])
    assert c.get("www.lab.test", QTYPE.A) is None


def test_stored_entry_not_mutated_by_hits():
    clk = FakeClock(); c = DNSCache(clock=clk)
    c.put("www.lab.test", QTYPE.A, [rr(ttl=10)])
    clk.t += 5
    c.get("www.lab.test", QTYPE.A)
    clk.t -= 5
    assert c.get("www.lab.test", QTYPE.A)[0].ttl == 10


def test_concurrent_access():
    c = DNSCache(); errors = []
    def work(i):
        try:
            for j in range(300):
                c.put(f"h{i}.lab.test", QTYPE.A, [rr(f"h{i}.lab.test", ttl=30)])
                got = c.get(f"h{i}.lab.test", QTYPE.A)
                assert got is not None and str(got[0].rname) == f"h{i}.lab.test."
        except Exception as e:
            errors.append(e)
    ts = [threading.Thread(target=work, args=(i,)) for i in range(20)]
    [t.start() for t in ts]; [t.join() for t in ts]
    assert not errors
