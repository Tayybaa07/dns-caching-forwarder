"""
listener.py
===========
UDP Listener & Query-ID Rewriter for the DNS Caching Forwarder.

Responsibilities (C1 / C3 / C4 / C6 / C7 – Group 8, Tayyba Bashir):
  - UDP socket listener on port 5300 (configurable) for client queries.
  - Worker thread pool for concurrent query handling.
  - Integration with Zainab's modules:
      • builder.py  – parse_query, parse_response, hit_reply, notimp, servfail
      • cache.py    – TTL cache (get / put)
      • selector.py – UpstreamSelector & HealthProber
      • probe.py    – make_probe_fn for background recovery probing
  - Socket Handling (Fix #1):
      Queries are forwarded to upstream servers using the dedicated upstream
      socket (_upstream_sock) so replies return to the upstream receive loop.
  - Cache & Builder Format (Fix #2):
      builder.parse_query -> cache.get -> on hit: builder.hit_reply -> client
      On miss: forward upstream -> on reply: builder.parse_response ->
      cache.put ONLY when rcode == NOERROR.
  - Packet Validation & Error Responses (Fix #3):
      Malformed queries dropped immediately without response.
      Unsupported query types (e.g. MX, TXT) return builder.notimp.
      Upstream failures / timeouts return builder.servfail.
  - Upstream Failover & Health Prober (Fix #4):
      HealthProber thread runs alongside server.
      Tracks upstream status (report_success / report_timeout).
      On 1.0s timeout, retries once on backup upstream (selector.other) before
      returning SERVFAIL (C7).
  - Response Matching Verification (Fix #5):
      Verifies Query ID, source IP:port, and question section match pending query.
      Counts and logs dropped mismatch packets.
"""

from __future__ import annotations

import argparse
import logging
import os
import queue
import socket
import sys
import threading
import time
from typing import Any, Optional, Tuple

# ---------------------------------------------------------------------------
# Sibling module imports & sys.path setup
# ---------------------------------------------------------------------------
_SRC_DIR = os.path.dirname(os.path.abspath(__file__))
if _SRC_DIR not in sys.path:
    sys.path.insert(0, _SRC_DIR)

# dnslib
try:
    import dnslib
    from dnslib import DNSRecord, DNSHeader, DNSError
    _HAS_DNSLIB = True
except ImportError:
    _HAS_DNSLIB = False

from pending_table import PendingEntry, PendingTable

# ---------------------------------------------------------------------------
# Module Integrations with Zainab's components & Fallbacks
# ---------------------------------------------------------------------------

# 1. Builder functions
try:
    from builder import (
        hit_reply as _builder_hit_reply,
        notimp as _builder_notimp,
        parse_query as _builder_parse_query,
        parse_response as _builder_parse_response,
        servfail as _builder_servfail,
    )
except ImportError:
    def _builder_parse_query(raw: bytes) -> Optional[Tuple[str, str]]:
        if not _HAS_DNSLIB:
            if len(raw) < 12:
                return None
            return ("unknown.test.", "A")
        try:
            d = DNSRecord.parse(raw)
            if not d.questions:
                return None
            q = d.questions[0]
            qname = str(q.qname).rstrip(".") + "."
            qtype = dnslib.QTYPE.get(q.qtype, str(q.qtype))
            return (qname, qtype)
        except Exception:
            return None

    def _builder_parse_response(
        raw: bytes,
    ) -> Tuple[Optional[int], Optional[int], list, Any]:
        if not _HAS_DNSLIB:
            if len(raw) < 12:
                return (None, None, [], None)
            rid = int.from_bytes(raw[:2], "big")
            return (rid, 0, [], None)
        try:
            d = DNSRecord.parse(raw)
            q = d.questions[0] if d.questions else None
            return (d.header.id, d.header.rcode, d.rr, q)
        except Exception:
            return (None, None, [], None)

    def _builder_hit_reply(raw: bytes, rrs: list) -> bytes:
        if not _HAS_DNSLIB:
            return raw
        d = DNSRecord.parse(raw)
        reply = DNSRecord(
            DNSHeader(id=d.header.id, qr=1, aa=0, ra=1, rcode=dnslib.RCODE.NOERROR),
            q=d.q,
        )
        for rr in rrs:
            reply.add_answer(rr)
        return reply.pack()

    def _builder_notimp(raw: bytes) -> bytes:
        if not _HAS_DNSLIB:
            cid = raw[:2] if len(raw) >= 2 else b"\x00\x00"
            return cid + b"\x81\x84\x00\x00\x00\x00\x00\x00\x00\x00"
        try:
            d = DNSRecord.parse(raw)
            reply = DNSRecord(
                DNSHeader(id=d.header.id, qr=1, ra=1, rcode=dnslib.RCODE.NOTIMP),
                q=d.q,
            )
            return reply.pack()
        except Exception:
            cid = int.from_bytes(raw[:2], "big") if len(raw) >= 2 else 0
            return DNSRecord(
                DNSHeader(id=cid, qr=1, ra=1, rcode=dnslib.RCODE.NOTIMP)
            ).pack()

    def _builder_servfail(raw: bytes) -> bytes:
        if not _HAS_DNSLIB:
            cid = raw[:2] if len(raw) >= 2 else b"\x00\x00"
            return cid + b"\x81\x82\x00\x00\x00\x00\x00\x00\x00\x00"
        try:
            d = DNSRecord.parse(raw)
            reply = DNSRecord(
                DNSHeader(id=d.header.id, qr=1, ra=1, rcode=dnslib.RCODE.SERVFAIL),
                q=d.q,
            )
            return reply.pack()
        except Exception:
            cid = int.from_bytes(raw[:2], "big") if len(raw) >= 2 else 0
            return DNSRecord(
                DNSHeader(id=cid, qr=1, ra=1, rcode=dnslib.RCODE.SERVFAIL)
            ).pack()


# 2. TTL Cache
try:
    from cache import DNSCache
except ImportError:
    class DNSCache:
        """Fallback in-memory TTL cache."""

        def __init__(self) -> None:
            self._cache: dict = {}
            self._lock = threading.Lock()

        def get(self, qname: str, qtype: Any) -> Optional[list]:
            key = (str(qname).rstrip(".").lower(), str(qtype).upper())
            with self._lock:
                item = self._cache.get(key)
                if not item:
                    return None
                expire_at, rrs = item
                rem = expire_at - time.monotonic()
                if rem <= 0:
                    del self._cache[key]
                    return None
                for rr in rrs:
                    if hasattr(rr, "ttl"):
                        rr.ttl = max(1, int(rem))
                return rrs

        def put(self, qname: str, qtype: Any, rrs: list) -> None:
            if not rrs:
                return
            min_ttl = min((getattr(rr, "ttl", 0) for rr in rrs), default=0)
            if min_ttl <= 0:
                return
            key = (str(qname).rstrip(".").lower(), str(qtype).upper())
            with self._lock:
                self._cache[key] = (time.monotonic() + min_ttl, rrs)


# 3. Upstream Selector & Prober
try:
    from selector import HealthProber, UpstreamSelector
except ImportError:
    class UpstreamSelector:
        """Fallback upstream selector with 3-timeout failover rule (C8)."""

        def __init__(
            self,
            primary: Tuple[str, int] = ("127.0.0.1", 5301),
            backup: Tuple[str, int] = ("127.0.0.1", 5302),
        ) -> None:
            self.primary = primary
            self.backup = backup
            self.primary_down = False
            self.fail_count = 0
            self._lock = threading.Lock()

        def pick(self) -> Tuple[str, int]:
            with self._lock:
                return self.backup if self.primary_down else self.primary

        def other(self, upstream: Tuple[str, int]) -> Tuple[str, int]:
            with self._lock:
                return self.backup if upstream == self.primary else self.primary

        def report_success(self, upstream: Tuple[str, int]) -> None:
            with self._lock:
                if upstream == self.primary:
                    self.fail_count = 0

        def report_timeout(self, upstream: Tuple[str, int]) -> None:
            with self._lock:
                if upstream == self.primary:
                    self.fail_count += 1
                    if self.fail_count >= 3 and not self.primary_down:
                        self.primary_down = True
                        logger.warning(
                            "Primary upstream %s marked DOWN after %d consecutive timeouts. Switching to backup %s (C8).",
                            self.primary,
                            self.fail_count,
                            self.backup,
                        )

        def mark_up(self, upstream: Tuple[str, int]) -> None:
            with self._lock:
                if upstream == self.primary and self.primary_down:
                    self.primary_down = False
                    self.fail_count = 0
                    logger.info(
                        "Primary upstream %s marked UP after successful recovery probes (C9).",
                        self.primary,
                    )

    class HealthProber:
        """Fallback health prober thread sending probes to DOWN primary every 5s (C9)."""

        def __init__(
            self, selector: Any, probe_fn: Any = None, interval: float = 5.0
        ) -> None:
            self.selector = selector
            self.probe_fn = probe_fn
            self.interval = interval
            self._running = False
            self._thread: Optional[threading.Thread] = None
            self.success_count = 0

        def start(self) -> None:
            self._running = True
            self._thread = threading.Thread(
                target=self._run, name="health-prober", daemon=True
            )
            self._thread.start()

        def _run(self) -> None:
            while self._running:
                time.sleep(self.interval)
                if not self._running:
                    break
                is_down = getattr(self.selector, "primary_down", False)
                if is_down and self.probe_fn:
                    ok = False
                    try:
                        ok = self.probe_fn()
                    except Exception:
                        ok = False
                    if ok:
                        self.success_count += 1
                        logger.info(
                            "Recovery probe to primary succeeded (%d/2)",
                            self.success_count,
                        )
                        if self.success_count >= 2:
                            if hasattr(self.selector, "mark_up"):
                                self.selector.mark_up(self.selector.primary)
                            self.success_count = 0
                    else:
                        self.success_count = 0

        def stop(self) -> None:
            self._running = False
            if self._thread:
                self._thread.join(timeout=2.0)
                self._thread = None


# 4. Probe factory
try:
    from probe import make_probe_fn
except ImportError:
    def make_probe_fn(
        target_addr: Tuple[str, int], test_name: str = "probe.test."
    ) -> Any:
        def probe() -> bool:
            try:
                sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                sock.settimeout(1.0)
                if _HAS_DNSLIB:
                    q = DNSRecord.question(test_name)
                    data = q.pack()
                else:
                    data = b"\x12\x34\x01\x00\x00\x01\x00\x00\x00\x00\x00\x00\x05probe\x04test\x00\x00\x01\x00\x01"
                sock.sendto(data, target_addr)
                reply, _ = sock.recvfrom(512)
                sock.close()
                return len(reply) >= 12
            except Exception:
                return False

        return probe


# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Helpers for Matching & Normalization (Fix #5 & Fix #3)
# ---------------------------------------------------------------------------

def _is_supported_type(qtype: Any) -> bool:
    """Supported query types are standard A (1) and AAAA (28), class IN (C1/C2)."""
    if isinstance(qtype, int):
        return qtype in (1, 28)
    q_str = str(qtype).strip().upper()
    return q_str in ("A", "AAAA")


def _match_addr(addr1: Tuple[str, int], addr2: Tuple[str, int]) -> bool:
    """Verify source address and port match expected upstream address."""
    if addr1[1] != addr2[1]:
        return False
    ip1, ip2 = addr1[0], addr2[0]
    if ip1 == ip2:
        return True
    localhosts = {"127.0.0.1", "localhost", "::1"}
    if ip1 in localhosts and ip2 in localhosts:
        return True
    try:
        return socket.gethostbyname(ip1) == socket.gethostbyname(ip2)
    except Exception:
        return False


def _match_question(reply_question: Any, exp_qname: str, exp_qtype: Any) -> bool:
    """Verify reply question matches the pending query's question."""
    if reply_question is None:
        return True

    rqname = None
    rqtype = None

    if isinstance(reply_question, tuple) and len(reply_question) >= 2:
        rqname, rqtype = reply_question[0], reply_question[1]
    elif hasattr(reply_question, "qname") and hasattr(reply_question, "qtype"):
        rqname = reply_question.qname
        rqtype = reply_question.qtype
    elif isinstance(reply_question, (list, tuple)) and len(reply_question) > 0:
        first = reply_question[0]
        if hasattr(first, "qname") and hasattr(first, "qtype"):
            rqname = first.qname
            rqtype = first.qtype

    if rqname is not None:
        norm_r = str(rqname).strip().rstrip(".").lower()
        norm_exp = str(exp_qname).strip().rstrip(".").lower()
        if norm_r != norm_exp:
            return False

    if rqtype is not None:
        if _HAS_DNSLIB and hasattr(dnslib, "QTYPE"):
            r_str = (
                dnslib.QTYPE.get(rqtype, str(rqtype))
                if isinstance(rqtype, int)
                else str(rqtype)
            )
            exp_str = (
                dnslib.QTYPE.get(exp_qtype, str(exp_qtype))
                if isinstance(exp_qtype, int)
                else str(exp_qtype)
            )
        else:
            r_str = str(rqtype)
            exp_str = str(exp_qtype)
        if r_str.upper() != exp_str.upper():
            return False

    return True


# ---------------------------------------------------------------------------
# Worker: processes one datagram from the queue
# ---------------------------------------------------------------------------

def _process_query(
    raw: bytes,
    client_addr: Tuple[str, int],
    pending: PendingTable,
    cache: Any,
    upstream_selector: Any,
    client_sock: socket.socket,
    upstream_sock: socket.socket,
) -> None:
    """
    Handle a single client DNS query in a worker thread.

    Steps
    -----
    1. Parse query using builder.parse_query.
       If None (malformed), drop packet immediately without response (Fix #3).
    2. Check supported query type (A/AAAA). If unsupported, return builder.notimp (Fix #3).
    3. Check cache using cache.get(qname, qtype).
       On hit: build reply using builder.hit_reply and send via client_sock (Fix #2).
    4. On miss: allocate upstream ID, record in PendingTable, rewrite query ID,
       and forward upstream using upstream_sock (Fix #1).
    """
    # 1. Parse query
    parsed = _builder_parse_query(raw)
    if parsed is None:
        logger.warning(
            "Malformed DNS query received from %s – dropped without response.",
            client_addr,
        )
        return

    qname, qtype = parsed
    logger.debug("Received query for %s %s from %s", qname, qtype, client_addr)

    # 2. Check query type support (C2)
    if not _is_supported_type(qtype):
        logger.info(
            "Unsupported query type %s for %s from %s – returning NOTIMP (C2)",
            qtype,
            qname,
            client_addr,
        )
        notimp_reply = _builder_notimp(raw)
        client_sock.sendto(notimp_reply, client_addr)
        return

    # 3. Cache lookup (C5)
    rrs = cache.get(qname, qtype)
    if rrs is not None:
        hit_reply = _builder_hit_reply(raw, rrs)
        client_sock.sendto(hit_reply, client_addr)
        logger.info("Cache HIT for %s %s → replied to %s", qname, qtype, client_addr)
        return

    # 4. Cache MISS – forward to selected upstream (Fix #1)
    up_host, up_port = upstream_selector.pick()
    upstream_addr = (up_host, up_port)

    upstream_id = pending.allocate_id()
    if upstream_id is None:
        logger.error(
            "Failed to allocate upstream ID for query from %s (table full) – sending SERVFAIL",
            client_addr,
        )
        servfail_reply = _builder_servfail(raw)
        client_sock.sendto(servfail_reply, client_addr)
        return

    client_id = int.from_bytes(raw[:2], "big")

    added = pending.add(
        upstream_id=upstream_id,
        client_addr=client_addr,
        client_id=client_id,
        questions=[(qname, qtype)],
        raw_query=raw,
        upstream_addr=upstream_addr,
        retried=False,
        qname=str(qname),
        qtype=qtype,
    )
    if not added:
        logger.warning(
            "PendingTable collision for upstream_id=%d – dropping query from %s",
            upstream_id,
            client_addr,
        )
        return

    rewritten = upstream_id.to_bytes(2, "big") + raw[2:]

    try:
        # Use upstream_sock so responses arrive on the dedicated upstream listener (Fix #1)
        upstream_sock.sendto(rewritten, upstream_addr)
        logger.info(
            "Cache MISS for %s %s (upstream_id=%d) → forwarded to %s:%d via upstream socket",
            qname,
            qtype,
            upstream_id,
            up_host,
            up_port,
        )
    except Exception as exc:
        logger.error("Failed to forward query to upstream %s:%d: %s", up_host, up_port, exc)
        pending.remove(upstream_id)
        servfail_reply = _builder_servfail(raw)
        client_sock.sendto(servfail_reply, client_addr)


# ---------------------------------------------------------------------------
# Upstream reply handler (Fix #5 & Fix #2)
# ---------------------------------------------------------------------------

def _handle_upstream_reply(
    raw: bytes,
    upstream_addr: Tuple[str, int],
    pending: PendingTable,
    cache: Any,
    client_sock: socket.socket,
    upstream_selector: Any,
    forwarder: Optional["DNSForwarder"] = None,
) -> None:
    """
    Process a reply datagram received from an upstream DNS server.

    Verification steps (Fix #5 / C4):
      1. Check packet length (>= 12 bytes).
      2. Match upstream Query ID to a pending query.
      3. Verify source IP:port matches expected upstream.
      4. Verify question section matches pending query.
      If any check fails, drop packet and increment mismatch counter.

    Delivery steps (Fix #2):
      5. Report success to upstream selector.
      6. Parse with builder.parse_response; cache ONLY when rcode == NOERROR.
      7. Restore original client ID and send response to client.
    """
    if len(raw) < 12:
        if forwarder:
            forwarder.mismatch_count += 1
        logger.warning(
            "Dropped upstream reply: packet too short (%d bytes) from %s (total mismatches: %d)",
            len(raw),
            upstream_addr,
            forwarder.mismatch_count if forwarder else 1,
        )
        return

    upstream_id = int.from_bytes(raw[:2], "big")
    entry = pending.get(upstream_id)

    # 1. Match Query ID
    if entry is None:
        if forwarder:
            forwarder.mismatch_count += 1
        logger.warning(
            "Reply mismatch: unknown or expired upstream_id=%d from %s (total mismatches: %d) – dropped",
            upstream_id,
            upstream_addr,
            forwarder.mismatch_count if forwarder else 1,
        )
        return

    # 2. Verify source IP and port
    if not _match_addr(upstream_addr, entry.upstream_addr):
        if forwarder:
            forwarder.mismatch_count += 1
        logger.warning(
            "Reply mismatch: source address %s does not match expected upstream %s for upstream_id=%d (total mismatches: %d) – dropped",
            upstream_addr,
            entry.upstream_addr,
            upstream_id,
            forwarder.mismatch_count if forwarder else 1,
        )
        return

    # 3. Parse response and verify question section
    try:
        resp_id, rcode, rrs, reply_question = _builder_parse_response(raw)
    except Exception as exc:
        if forwarder:
            forwarder.mismatch_count += 1
        logger.warning(
            "Reply mismatch: failed to parse upstream response for upstream_id=%d: %s (total mismatches: %d) – dropped",
            upstream_id,
            exc,
            forwarder.mismatch_count if forwarder else 1,
        )
        return

    if not _match_question(reply_question, entry.qname, entry.qtype):
        if forwarder:
            forwarder.mismatch_count += 1
        logger.warning(
            "Reply mismatch: question %s does not match expected (%s, %s) for upstream_id=%d (total mismatches: %d) – dropped",
            reply_question,
            entry.qname,
            entry.qtype,
            upstream_id,
            forwarder.mismatch_count if forwarder else 1,
        )
        return

    # All checks passed! Remove entry from pending table
    pending.remove(upstream_id)

    # Report success to upstream selector (Fix #4)
    if hasattr(upstream_selector, "report_success"):
        upstream_selector.report_success(entry.upstream_addr)

    # Cache only when rcode == NOERROR (Fix #2 / C5)
    is_noerror = rcode == 0 or (
        _HAS_DNSLIB and hasattr(dnslib, "RCODE") and rcode == dnslib.RCODE.NOERROR
    )
    if is_noerror and rrs:
        try:
            cache.put(entry.qname, entry.qtype, rrs)
            logger.debug(
                "Cached NOERROR response for %s %s (%d RRs)",
                entry.qname,
                entry.qtype,
                len(rrs),
            )
        except Exception as exc:
            logger.warning(
                "Failed to cache response for %s %s: %s",
                entry.qname,
                entry.qtype,
                exc,
            )

    # Restore original client ID and send reply to client
    client_response = entry.client_id.to_bytes(2, "big") + raw[2:]
    try:
        client_sock.sendto(client_response, entry.client_addr)
        logger.info(
            "Forwarded upstream reply to %s | client_id=%d | upstream_id=%d | rcode=%s",
            entry.client_addr,
            entry.client_id,
            upstream_id,
            rcode,
        )
    except Exception as exc:
        logger.error(
            "Failed to send reply to client %s: %s", entry.client_addr, exc
        )


# ---------------------------------------------------------------------------
# DNSForwarder – main server class
# ---------------------------------------------------------------------------

class DNSForwarder:
    """
    UDP DNS caching forwarder with upstream failover and health probing.
    """

    _RECV_BUFSIZE: int = 4096

    def __init__(
        self,
        listen_port: int = 5300,
        primary_host: str = "127.0.0.1",
        primary_port: int = 5301,
        backup_host: str = "127.0.0.1",
        backup_port: int = 5302,
        num_workers: int = 4,
        pending_timeout: float = 1.0,
        reaper_interval: float = 0.2,
    ) -> None:
        self.listen_port = listen_port
        self.primary_host = primary_host
        self.primary_port = primary_port
        self.backup_host = backup_host
        self.backup_port = backup_port
        self.num_workers = num_workers
        self.pending_timeout = pending_timeout
        self.reaper_interval = reaper_interval

        self.mismatch_count = 0

        # Pending-query table with timeout callback (C3 / C4 / C7)
        self._pending = PendingTable(on_expire=self._handle_pending_timeout)

        # TTL cache (C5)
        self._cache = DNSCache()

        # Upstream selector (C8)
        primary_addr = (self.primary_host, self.primary_port)
        backup_addr = (self.backup_host, self.backup_port)
        try:
            self._upstream_selector = UpstreamSelector(
                primary=primary_addr, backup=backup_addr
            )
        except TypeError:
            try:
                self._upstream_selector = UpstreamSelector()
            except Exception:
                self._upstream_selector = UpstreamSelector(
                    primary=primary_addr, backup=backup_addr
                )

        # Health prober (C9)
        try:
            probe_fn = make_probe_fn(primary_addr)
            self._health_prober = HealthProber(
                self._upstream_selector, probe_fn=probe_fn
            )
        except Exception:
            try:
                self._health_prober = HealthProber(self._upstream_selector)
            except Exception:
                self._health_prober = None

        # Work queue: (raw_bytes, client_addr)
        self._query_queue: queue.Queue[Tuple[bytes, Tuple[str, int]]] = queue.Queue()

        self._client_sock: Optional[socket.socket] = None
        self._upstream_sock: Optional[socket.socket] = None

        self._running = False
        self._worker_threads: list[threading.Thread] = []
        self._upstream_listener_thread: Optional[threading.Thread] = None

    # ------------------------------------------------------------------
    # Timeout & Failover handler (Fix #4 / C7)
    # ------------------------------------------------------------------

    def _handle_pending_timeout(self, entry: PendingEntry) -> None:
        """
        Handle timeout for an in-flight query (C7 requirement):
          - Report timeout to selector for the failed upstream.
          - If not yet retried, retry once on the backup/other upstream.
          - If already retried once, send SERVFAIL to the client.
        """
        upstream_addr = entry.upstream_addr
        logger.warning(
            "Query timeout after %.1fs: client=%s, upstream=%s, upstream_id=%d, retried=%s",
            self.pending_timeout,
            entry.client_addr,
            upstream_addr,
            entry.upstream_id,
            entry.retried,
        )

        if hasattr(self._upstream_selector, "report_timeout"):
            self._upstream_selector.report_timeout(upstream_addr)

        if not entry.retried:
            # Attempt other upstream exactly once (C7)
            if hasattr(self._upstream_selector, "other"):
                other_upstream = self._upstream_selector.other(upstream_addr)
            else:
                other_upstream = upstream_addr

            logger.info(
                "Retrying query for client %s (%s %s) on backup upstream %s (C7)",
                entry.client_addr,
                entry.qname,
                entry.qtype,
                other_upstream,
            )

            new_uid = self._pending.allocate_id()
            if new_uid is None:
                logger.error(
                    "Failed to allocate new ID for retry to %s – sending SERVFAIL",
                    entry.client_addr,
                )
                servfail_reply = _builder_servfail(entry.raw_query)
                self._client_sock.sendto(servfail_reply, entry.client_addr)
                return

            rewritten = new_uid.to_bytes(2, "big") + entry.raw_query[2:]
            added = self._pending.add(
                upstream_id=new_uid,
                client_addr=entry.client_addr,
                client_id=entry.client_id,
                questions=entry.questions,
                raw_query=entry.raw_query,
                upstream_addr=other_upstream,
                retried=True,
                qname=entry.qname,
                qtype=entry.qtype,
            )
            if not added:
                logger.warning(
                    "Collision adding retry entry upstream_id=%d – sending SERVFAIL",
                    new_uid,
                )
                servfail_reply = _builder_servfail(entry.raw_query)
                self._client_sock.sendto(servfail_reply, entry.client_addr)
                return

            try:
                self._upstream_sock.sendto(rewritten, other_upstream)
                logger.info(
                    "Sent retry query upstream_id=%d to backup upstream %s",
                    new_uid,
                    other_upstream,
                )
            except Exception as exc:
                logger.error(
                    "Failed to send retry packet to %s: %s", other_upstream, exc
                )
                self._pending.remove(new_uid)
                servfail_reply = _builder_servfail(entry.raw_query)
                self._client_sock.sendto(servfail_reply, entry.client_addr)
        else:
            # Both primary and backup upstreams have timed out (C7)
            logger.error(
                "Both upstreams timed out for client %s (%s %s) – sending SERVFAIL",
                entry.client_addr,
                entry.qname,
                entry.qtype,
            )
            servfail_reply = _builder_servfail(entry.raw_query)
            try:
                self._client_sock.sendto(servfail_reply, entry.client_addr)
            except Exception as exc:
                logger.error(
                    "Failed to send SERVFAIL to client %s: %s",
                    entry.client_addr,
                    exc,
                )

    # ------------------------------------------------------------------
    # Server lifecycle
    # ------------------------------------------------------------------

    def start(self) -> None:
        """Bind sockets, start worker pool, prober, and begin receive loop."""
        self._running = True

        # Client-facing socket
        self._client_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self._client_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._client_sock.bind(("0.0.0.0", self.listen_port))
        self._client_sock.settimeout(1.0)
        logger.info(
            "DNS forwarder listening on UDP 0.0.0.0:%d (primary: %s:%d, backup: %s:%d)",
            self.listen_port,
            self.primary_host,
            self.primary_port,
            self.backup_host,
            self.backup_port,
        )

        # Dedicated upstream socket (Fix #1)
        self._upstream_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self._upstream_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._upstream_sock.bind(("0.0.0.0", 0))
        self._upstream_sock.settimeout(1.0)
        up_ep = self._upstream_sock.getsockname()
        logger.info("Upstream socket bound to %s:%d", up_ep[0], up_ep[1])

        # Start background pending reaper (timeout default 1.0s)
        self._pending.start_reaper(
            interval_secs=self.reaper_interval,
            max_age_secs=self.pending_timeout,
        )

        # Start health prober thread (Fix #4 / C9)
        if self._health_prober and hasattr(self._health_prober, "start"):
            self._health_prober.start()
            logger.info(
                "HealthProber started for primary upstream %s:%d",
                self.primary_host,
                self.primary_port,
            )

        # Start worker pool
        for i in range(self.num_workers):
            t = threading.Thread(
                target=self._worker_loop,
                name=f"dns-worker-{i}",
                daemon=True,
            )
            t.start()
            self._worker_threads.append(t)
        logger.info("Started %d worker thread(s).", self.num_workers)

        # Start upstream listener thread
        self._upstream_listener_thread = threading.Thread(
            target=self._upstream_receive_loop,
            name="upstream-listener",
            daemon=True,
        )
        self._upstream_listener_thread.start()

        # Run client receive loop (blocking)
        self._client_receive_loop()

    def stop(self) -> None:
        """Signal all threads to stop and cleanly release resources."""
        logger.info("Stopping DNS forwarder…")
        self._running = False

        if self._health_prober and hasattr(self._health_prober, "stop"):
            self._health_prober.stop()
            logger.info("HealthProber stopped.")

        self._pending.stop_reaper()

        for _ in self._worker_threads:
            self._query_queue.put((b"", ("", 0)))

        for t in self._worker_threads:
            t.join(timeout=3.0)

        if self._upstream_listener_thread:
            self._upstream_listener_thread.join(timeout=3.0)

        if self._client_sock:
            self._client_sock.close()
        if self._upstream_sock:
            self._upstream_sock.close()

        logger.info("DNS forwarder stopped.")

    # ------------------------------------------------------------------
    # Internal loops
    # ------------------------------------------------------------------

    def _client_receive_loop(self) -> None:
        """Main loop: receive datagrams from clients and enqueue them."""
        logger.info("Client receive loop started.")
        while self._running:
            try:
                raw, addr = self._client_sock.recvfrom(self._RECV_BUFSIZE)
            except socket.timeout:
                continue
            except OSError as exc:
                if self._running:
                    logger.error("Error receiving from client socket: %s", exc)
                break

            if not raw:
                continue

            logger.debug("Received %d bytes from %s", len(raw), addr)
            self._query_queue.put((raw, addr))

        logger.info("Client receive loop exited.")

    def _upstream_receive_loop(self) -> None:
        """Dedicated loop: receive replies from upstream DNS servers (Fix #1 & #5)."""
        logger.info("Upstream receive loop started.")
        while self._running:
            try:
                raw, addr = self._upstream_sock.recvfrom(self._RECV_BUFSIZE)
            except socket.timeout:
                continue
            except OSError as exc:
                if self._running:
                    logger.error("Error receiving from upstream socket: %s", exc)
                break

            _handle_upstream_reply(
                raw=raw,
                upstream_addr=addr,
                pending=self._pending,
                cache=self._cache,
                client_sock=self._client_sock,
                upstream_selector=self._upstream_selector,
                forwarder=self,
            )

        logger.info("Upstream receive loop exited.")

    def _worker_loop(self) -> None:
        """Worker thread: dequeue and process client queries."""
        name = threading.current_thread().name
        logger.debug("%s started.", name)
        while True:
            try:
                raw, client_addr = self._query_queue.get(timeout=1.0)
            except queue.Empty:
                if not self._running:
                    break
                continue

            if not raw and client_addr == ("", 0):
                break

            try:
                _process_query(
                    raw=raw,
                    client_addr=client_addr,
                    pending=self._pending,
                    cache=self._cache,
                    upstream_selector=self._upstream_selector,
                    client_sock=self._client_sock,
                    upstream_sock=self._upstream_sock,
                )
            except Exception as exc:
                logger.exception(
                    "%s: unhandled exception processing query from %s: %s",
                    name,
                    client_addr,
                    exc,
                )
            finally:
                self._query_queue.task_done()

        logger.debug("%s exited.", name)


# ---------------------------------------------------------------------------
# CLI entry-point
# ---------------------------------------------------------------------------

def _build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="DNS Caching Forwarder with Upstream Failover (Group 8)",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    p.add_argument(
        "--port",
        type=int,
        default=5300,
        metavar="PORT",
        help="UDP port to listen on for client queries",
    )
    p.add_argument(
        "--primary",
        default="127.0.0.1:5301",
        metavar="HOST:PORT",
        help="Primary upstream DNS server (default: 127.0.0.1:5301)",
    )
    p.add_argument(
        "--backup",
        default="127.0.0.1:5302",
        metavar="HOST:PORT",
        help="Backup upstream DNS server (default: 127.0.0.1:5302)",
    )
    p.add_argument(
        "--upstream",
        default=None,
        metavar="HOST:PORT",
        help="Override primary upstream server address",
    )
    p.add_argument(
        "--workers",
        type=int,
        default=4,
        metavar="N",
        help="Number of worker threads in the pool",
    )
    p.add_argument(
        "--timeout",
        type=float,
        default=1.0,
        metavar="SECS",
        help="Timeout in seconds before retrying backup upstream (default: 1.0)",
    )
    p.add_argument(
        "--reaper-interval",
        type=float,
        default=0.2,
        metavar="SECS",
        help="How often the reaper sweeps for timed-out queries (default: 0.2)",
    )
    p.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging verbosity level",
    )
    return p


def _configure_logging(level_name: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level_name),
        format="%(asctime)s [%(levelname)-8s] %(threadName)-20s %(name)s – %(message)s",
        stream=sys.stdout,
    )


def _parse_addr(addr_str: str, default_port: int = 53) -> Tuple[str, int]:
    try:
        parts = addr_str.rsplit(":", 1)
        host = parts[0]
        port = int(parts[1]) if len(parts) == 2 else default_port
        return host, port
    except Exception:
        raise ValueError(f"Invalid host:port specification: {addr_str!r}")


def main(argv: list[str] | None = None) -> None:
    parser = _build_arg_parser()
    args = parser.parse_args(argv)

    _configure_logging(args.log_level)

    primary_str = args.upstream if args.upstream else args.primary
    try:
        primary_host, primary_port = _parse_addr(primary_str, 5301)
        backup_host, backup_port = _parse_addr(args.backup, 5302)
    except ValueError as exc:
        parser.error(str(exc))

    forwarder = DNSForwarder(
        listen_port=args.port,
        primary_host=primary_host,
        primary_port=primary_port,
        backup_host=backup_host,
        backup_port=backup_port,
        num_workers=args.workers,
        pending_timeout=args.timeout,
        reaper_interval=args.reaper_interval,
    )

    logger.info(
        "Starting DNS Caching Forwarder on port %d | Primary: %s:%d | Backup: %s:%d",
        args.port,
        primary_host,
        primary_port,
        backup_host,
        backup_port,
    )

    try:
        forwarder.start()
    except KeyboardInterrupt:
        logger.info("Keyboard interrupt received.")
    finally:
        forwarder.stop()


if __name__ == "__main__":
    main()
