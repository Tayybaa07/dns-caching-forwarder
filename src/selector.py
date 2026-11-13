"""Upstream selector and health prober (compulsory features C8, C9).

Rules (all configurable):
  * N = 3 consecutive timed-out attempts to the primary -> primary DOWN.
  * While DOWN, a probe is sent every 5 s; 2 consecutive successes -> UP.
  * Every state change is logged with a timestamp.

The state logic (UpstreamSelector) does no networking, so it is unit-testable.
HealthProber is the thread that actually sends probes via a callable you give it.
"""
import logging
import threading
import time

log = logging.getLogger("selector")

UP, DOWN = "UP", "DOWN"


class UpstreamSelector:
    def __init__(self, primary, backup, fail_threshold=3, probe_interval=5.0,
                 probe_successes=2, clock=time.monotonic):
        self.primary, self.backup = primary, backup
        self.fail_threshold = fail_threshold
        self.probe_interval = probe_interval
        self.probe_successes = probe_successes
        self._clock = clock
        self._lock = threading.Lock()
        self._state = {primary: UP, backup: UP}
        self._fails = {primary: 0, backup: 0}
        self._probe_ok = 0
        self._next_probe = 0.0
        self.events = []             # (wall_time, upstream, new_state) for plots

    # ---- used by the query path ----
    def current(self):
        """Upstream new queries should go to."""
        with self._lock:
            return self.primary if self._state[self.primary] == UP else self.backup

    pick = current                   # alias: Tayyba's listener calls selector.pick()

    def other(self, upstream):
        """The upstream to use for the single retry."""
        return self.backup if upstream == self.primary else self.primary

    def report_timeout(self, upstream):
        with self._lock:
            if self._state[upstream] == DOWN:
                return
            self._fails[upstream] += 1
            if self._fails[upstream] >= self.fail_threshold:
                self._set_state(upstream, DOWN)
                if upstream == self.primary:
                    self._probe_ok = 0
                    self._next_probe = self._clock() + self.probe_interval

    def report_success(self, upstream):
        with self._lock:
            if self._state[upstream] == UP:
                self._fails[upstream] = 0

    def state(self, upstream):
        with self._lock:
            return self._state[upstream]

    # ---- used by the prober ----
    def probe_due(self):
        """True if the primary is DOWN and it is time to send a probe."""
        with self._lock:
            return (self._state[self.primary] == DOWN
                    and self._clock() >= self._next_probe)

    def report_probe(self, ok):
        with self._lock:
            if self._state[self.primary] != DOWN:
                return
            self._next_probe = self._clock() + self.probe_interval
            if not ok:
                self._probe_ok = 0
                return
            self._probe_ok += 1
            if self._probe_ok >= self.probe_successes:
                self._fails[self.primary] = 0
                self._probe_ok = 0
                self._set_state(self.primary, UP)

    def _set_state(self, upstream, new):
        self._state[upstream] = new
        self.events.append((time.time(), upstream, new))
        log.warning("UPSTREAM %s is now %s", upstream, new)


class HealthProber(threading.Thread):
    """Background thread. send_probe(upstream) -> True if a reply came back."""

    def __init__(self, selector, send_probe, tick=0.25):
        super().__init__(daemon=True)
        self.selector, self.send_probe, self.tick = selector, send_probe, tick
        self._stop_evt = threading.Event()

    def run(self):
        while not self._stop_evt.is_set():
            if self.selector.probe_due():
                self.selector.report_probe(self.send_probe(self.selector.primary))
            self._stop_evt.wait(self.tick)

    def stop(self):
        self._stop_evt.set()
