import os, time
from selector import UpstreamSelector, HealthProber, UP, DOWN
from probe import make_probe_fn
import test_dns_server as ts
from test_testserver import start, free_port


def test_probe_succeeds_and_fails_with_server_state():
    st, port = start()
    probe = make_probe_fn(timeout=0.4)
    assert probe(("127.0.0.1", port)) is True
    ts.control_command(st, "stop")
    assert probe(("127.0.0.1", port)) is False
    ts.control_command(st, "start")
    assert probe(("127.0.0.1", port)) is True


def test_prober_brings_primary_back_after_two_successful_probes():
    st, port = start()
    P, B = ("127.0.0.1", port), ("127.0.0.1", free_port())
    sel = UpstreamSelector(P, B, probe_interval=0.2)
    for _ in range(3):
        sel.report_timeout(P)
    assert sel.current() == B and sel.pick() == B
    ts.control_command(st, "stop")                    # still down: stays DOWN
    prober = HealthProber(sel, make_probe_fn(timeout=0.2), tick=0.05)
    prober.start()
    time.sleep(0.8)
    assert sel.state(P) == DOWN
    ts.control_command(st, "start")                   # restored: 2 probes -> UP
    deadline = time.time() + 3
    while sel.state(P) != UP and time.time() < deadline:
        time.sleep(0.05)
    prober.stop()
    assert sel.state(P) == UP and sel.current() == P
