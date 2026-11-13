from selector import UpstreamSelector, UP, DOWN

P, B = ("127.0.0.1", 5301), ("127.0.0.1", 5302)


class FakeClock:
    def __init__(self): self.t = 0.0
    def __call__(self): return self.t


def make():
    clk = FakeClock()
    return UpstreamSelector(P, B, clock=clk), clk


def test_starts_on_primary():
    s, _ = make()
    assert s.current() == P and s.other(P) == B and s.other(B) == P


def test_three_consecutive_timeouts_mark_primary_down():
    s, _ = make()
    s.report_timeout(P); s.report_timeout(P)
    assert s.current() == P
    s.report_timeout(P)
    assert s.state(P) == DOWN and s.current() == B
    assert s.events[-1][1:] == (P, DOWN)


def test_success_resets_failure_counter():
    s, _ = make()
    s.report_timeout(P); s.report_timeout(P)
    s.report_success(P)
    s.report_timeout(P); s.report_timeout(P)
    assert s.state(P) == UP


def test_probe_due_every_5s_only_when_down():
    s, clk = make()
    assert not s.probe_due()
    for _ in range(3): s.report_timeout(P)
    assert not s.probe_due()
    clk.t += 5
    assert s.probe_due()


def test_two_successful_probes_bring_primary_up():
    s, clk = make()
    for _ in range(3): s.report_timeout(P)
    clk.t += 5; s.report_probe(True)
    assert s.state(P) == DOWN and s.current() == B
    clk.t += 5; s.report_probe(True)
    assert s.state(P) == UP and s.current() == P
    assert s.events[-1][1:] == (P, UP)


def test_failed_probe_resets_success_count():
    s, clk = make()
    for _ in range(3): s.report_timeout(P)
    clk.t += 5; s.report_probe(True)
    clk.t += 5; s.report_probe(False)
    clk.t += 5; s.report_probe(True)
    assert s.state(P) == DOWN
    clk.t += 5; s.report_probe(True)
    assert s.state(P) == UP


def test_recovered_primary_needs_three_fresh_timeouts_to_fail_again():
    s, clk = make()
    for _ in range(3): s.report_timeout(P)
    clk.t += 5; s.report_probe(True); clk.t += 5; s.report_probe(True)
    s.report_timeout(P); s.report_timeout(P)
    assert s.state(P) == UP


def test_backup_timeouts_do_not_change_selection_while_primary_down():
    s, _ = make()
    for _ in range(3): s.report_timeout(P)
    for _ in range(5): s.report_timeout(B)
    assert s.current() == B
