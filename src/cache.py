"""TTL cache for the DNS forwarder (compulsory feature C5).

Keyed by (lowercased name, type, class). An entry lives for the minimum TTL
among its answer records. Hits return copies of the records carrying the
*remaining* TTL. Records with TTL 0 are never cached. Thread-safe.

Usage (from the listener / worker):
    cache = DNSCache()
    records = cache.get("www.lab.test", QTYPE.A)      # list[RR] or None
    cache.put("www.lab.test", QTYPE.A, answer_rrs)    # only for NOERROR answers
"""
import math
import threading
import time

from dnslib import RR, CLASS


def _key(name, qtype, qclass=CLASS.IN):
    name = str(name).lower().rstrip(".")
    return (name, int(qtype), int(qclass))


class DNSCache:
    def __init__(self, clock=time.monotonic, enabled=True):
        self._clock = clock          # injectable so tests need no sleeping
        self.enabled = enabled       # False = baseline B2 (forwarder, no cache)
        self._lock = threading.Lock()
        self._entries = {}           # key -> (expiry_time, original_ttl, [RR])
        self.hits = 0
        self.misses = 0

    def get(self, name, qtype, qclass=CLASS.IN):
        """Return list of RR with remaining TTL, or None on miss/expiry."""
        if not self.enabled:
            return None
        key = _key(name, qtype, qclass)
        now = self._clock()
        with self._lock:
            entry = self._entries.get(key)
            if entry is None:
                self.misses += 1
                return None
            expiry, original_ttl, records = entry
            remaining = expiry - now
            if remaining <= 0:
                del self._entries[key]
                self.misses += 1
                return None
            self.hits += 1
            # round up so a live entry never reports TTL 0; never exceed original
            ttl = min(original_ttl, max(1, math.ceil(remaining)))
            return [RR(r.rname, r.rtype, r.rclass, ttl, r.rdata) for r in records]

    def put(self, name, qtype, records, qclass=CLASS.IN):
        """Cache answer records. Returns True if stored.

        Lifetime = minimum TTL of the records. Empty answers and TTL 0 are
        not cached. The caller must not pass SERVFAIL/error responses.
        """
        if not self.enabled or not records:
            return False
        ttl = min(r.ttl for r in records)
        if ttl <= 0:
            return False
        key = _key(name, qtype, qclass)
        stored = [RR(r.rname, r.rtype, r.rclass, r.ttl, r.rdata) for r in records]
        with self._lock:
            self._entries[key] = (self._clock() + ttl, ttl, stored)
        return True

    def purge_expired(self):
        now = self._clock()
        with self._lock:
            dead = [k for k, (exp, _, _) in self._entries.items() if exp <= now]
            for k in dead:
                del self._entries[k]
            return len(dead)

    def clear(self):
        with self._lock:
            self._entries.clear()

    def __len__(self):
        with self._lock:
            return len(self._entries)
