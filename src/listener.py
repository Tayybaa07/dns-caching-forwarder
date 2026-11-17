"""
listener.py
===========
UDP Listener & Query-ID Rewriter for the DNS Caching Forwarder.

Group 8 – Tayyba Bashir & Zainab Parveen
Responsibilities:
  - C1: UDP socket listener on configurable port (default: 5300) with worker pool.
  - C3: Random 16-bit upstream Query-ID generation & collision check via PendingTable.
  - C4: Strict response matching (Query-ID, source IP:port, and question section).
  - C6: Concurrency and thread safety across cache, pending table, and selector.
  - C7: 1.0 s timeout with single retry on backup upstream before SERVFAIL.
  - C8/C9: Upstream failover after 3 timeouts and background recovery probing.
  - C10: Structured query logging and CLI statistics / metrics reporting.
"""

from __future__ import annotations

import argparse
import logging
import math
import os
import queue
import socket
import sys
import threading
import time
from datetime import datetime
from typing import Any, Optional, Tuple

# ---------------------------------------------------------------------------
# Sibling module imports & sys.path setup
# ---------------------------------------------------------------------------
_SRC_DIR = os.path.dirname(os.path.abspath(__file__))
if _SRC_DIR not in sys.path:
    sys.path.insert(0, _SRC_DIR)

import dnslib
from dnslib import QTYPE, RCODE

from builder import (
    ParsedQuery,
    hit_reply,
    notimp,
    parse_query,
    parse_response,
    servfail,
    set_id,
)
from cache import DNSCache
from pending_table import PendingEntry, PendingTable
from probe import make_probe_fn
from selector import DOWN, UP, HealthProber, UpstreamSelector

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# C10: Structured Query Logger & Metrics Tracking
# ---------------------------------------------------------------------------

class QueryLogger:
    """
    Structured query logger (compulsory feature C10).

    Outputs one line per completed query in the exact format:
      [TIMESTAMP] client=<ip:port> name=<qname> type=<type> result=<HIT|MISS|DROP|ERR> upstream=<ip:port|NONE> latency_ms=<0.0> rcode=<RCODE>
    """

    def __init__(self, log_file: Optional[str] = None) -> None:
        self.log_file = log_file
        self._lock = threading.Lock()
        if self.log_file:
            # Touch log file to ensure it is writable
            with open(self.log_file, "a") as f:
                pass

    def log(
        self,
        client: Tuple[str, int],
        name: str,
        qtype: str,
        result: str,
        upstream: Optional[Tuple[str, int]],
        latency_ms: float,
        rcode: str,
    ) -> None:
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        client_str = f"{client[0]}:{client[1]}"
        up_str = f"{upstream[0]}:{upstream[1]}" if upstream else "NONE"
        line = (
            f"[{ts}] client={client_str:<21} name={name:<20} type={qtype:<5} "
            f"result={result:<4} upstream={up_str:<21} latency_ms={latency_ms:>6.2f} rcode={rcode}"
        )
        with self._lock:
            print(f"QUERY {line}", flush=True)
            if self.log_file:
                try:
                    with open(self.log_file, "a") as f:
                        f.write(line + "\n")
                except Exception as exc:
                    logger.error("Failed to write to query log file: %s", exc)


class ForwarderStats:
    """
    Thread-safe runtime statistics tracker & CLI view (C10).

    Tracks hits, misses, hit ratio, response-time percentiles, upstream query counts,
    and active upstream server state.
    """

    def __init__(self, selector: UpstreamSelector) -> None:
        self.selector = selector
        self._lock = threading.Lock()
        self.total_queries = 0
        self.hits = 0
        self.misses = 0
        self.errors = 0
        self.mismatch_drops = 0
        self.latencies: list[float] = []
        self.upstream_queries: dict[Tuple[str, int], int] = {}

    def record_hit(self, latency_ms: float) -> None:
        with self._lock:
            self.total_queries += 1
            self.hits += 1
            self.latencies.append(latency_ms)

    def record_miss(self, latency_ms: float) -> None:
        with self._lock:
            self.total_queries += 1
            self.misses += 1
            self.latencies.append(latency_ms)

    def record_unsupported(self, latency_ms: float) -> None:
        with self._lock:
            self.total_queries += 1
            self.errors += 1
            self.latencies.append(latency_ms)

    def record_error(self, latency_ms: float) -> None:
        with self._lock:
            self.total_queries += 1
            self.errors += 1
            self.latencies.append(latency_ms)

    def record_drop(self) -> None:
        with self._lock:
            self.mismatch_drops += 1

    def record_mismatch(self) -> None:
        with self._lock:
            self.mismatch_drops += 1

    def record_upstream_query(self, upstream: Tuple[str, int]) -> None:
        with self._lock:
            self.upstream_queries[upstream] = self.upstream_queries.get(upstream, 0) + 1

    def get_summary(self) -> str:
        with self._lock:
            total = self.total_queries
            hits = self.hits
            misses = self.misses
            errors = self.errors
            drops = self.mismatch_drops
            hit_ratio = (hits / total * 100.0) if total > 0 else 0.0
            lats = sorted(self.latencies)
            up_counts = dict(self.upstream_queries)

        if lats:
            min_lat = lats[0]
            max_lat = lats[-1]
            mean_lat = sum(lats) / len(lats)
            p95_idx = int(math.ceil(0.95 * len(lats))) - 1
            p95_lat = lats[max(0, p95_idx)]
        else:
            min_lat = max_lat = mean_lat = p95_lat = 0.0

        p_state = self.selector.state(self.selector.primary)
        b_state = self.selector.state(self.selector.backup)
        cur = self.selector.current()

        lines = [
            "",
            "=" * 64,
            "                 DNS FORWARDER STATISTICS (C10)",
            "=" * 64,
            f" Total Queries:          {total}",
            f" Cache Hits:             {hits} ({hit_ratio:.1f}%)",
            f" Cache Misses:           {misses} ({100.0 - hit_ratio if total else 0.0:.1f}%)",
            f" Errors / Unsupported:   {errors}",
            f" Mismatched / Dropped:   {drops}",
            "-" * 64,
            " Resolution Latency (ms):",
            f"   Min:                  {min_lat:.2f} ms",
            f"   Mean:                 {mean_lat:.2f} ms",
            f"   95th Percentile:      {p95_lat:.2f} ms",
            f"   Max:                  {max_lat:.2f} ms",
            "-" * 64,
            " Upstream Server Query Counts:",
            f"   Primary {self.selector.primary[0]}:{self.selector.primary[1]} ({p_state:4s}):  {up_counts.get(self.selector.primary, 0)}",
            f"   Backup  {self.selector.backup[0]}:{self.selector.backup[1]} ({b_state:4s}):  {up_counts.get(self.selector.backup, 0)}",
            f" Active Upstream:        {cur[0]}:{cur[1]}",
            "=" * 64,
            "",
        ]
        return "\n".join(lines)


# ---------------------------------------------------------------------------
# Helpers for Matching (Fix #5 / C4)
# ---------------------------------------------------------------------------

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


# ---------------------------------------------------------------------------
# Worker: processes one datagram from the queue
# ---------------------------------------------------------------------------

def _process_query(
    raw: bytes,
    client_addr: Tuple[str, int],
    pending: PendingTable,
    cache: DNSCache,
    upstream_selector: UpstreamSelector,
    client_sock: socket.socket,
    upstream_sock: socket.socket,
    stats: Optional[ForwarderStats] = None,
    query_logger: Optional[QueryLogger] = None,
) -> None:
    """
    Handle a single client DNS query in a worker thread.

    Steps:
      1. Parse query with builder.parse_query -> ParsedQuery dataclass.
         If None (malformed), drop packet immediately without response.
      2. Check parsed.supported (A/AAAA class IN). If unsupported, return notimp.
      3. Cache check: cache.get(qname, qtype, qclass).
         On hit: return hit_reply directly to client.
      4. Cache miss: allocate upstream ID, record in PendingTable, rewrite ID,
         and forward upstream via upstream_sock (Fix #1).
    """
    arrival_time = time.monotonic()

    # 1. Parse query (Zainab's ParsedQuery dataclass)
    parsed = parse_query(raw)
    if parsed is None:
        if stats:
            stats.record_drop()
        if query_logger:
            query_logger.log(
                client=client_addr,
                name="<malformed>",
                qtype="<none>",
                result="DROP",
                upstream=None,
                latency_ms=(time.monotonic() - arrival_time) * 1000.0,
                rcode="MALFORMED",
            )
        logger.warning("Malformed DNS query from %s – dropped without response.", client_addr)
        return

    qname = parsed.name      # lowercased, no trailing dot (e.g. 'www.lab.test')
    qtype = parsed.qtype     # int (e.g. QTYPE.A == 1)
    qclass = parsed.qclass   # int (e.g. CLASS.IN == 1)
    qtype_name = QTYPE.get(qtype, str(qtype))

    # 2. Check query type support (C1/C2: standard A/AAAA, class IN)
    if not parsed.supported:
        reply_bytes = notimp(raw)
        client_sock.sendto(reply_bytes, client_addr)
        latency_ms = (time.monotonic() - arrival_time) * 1000.0
        if stats:
            stats.record_unsupported(latency_ms)
        if query_logger:
            query_logger.log(
                client=client_addr,
                name=qname,
                qtype=qtype_name,
                result="ERR",
                upstream=None,
                latency_ms=latency_ms,
                rcode="NOTIMP",
            )
        logger.info("Unsupported query type %s (%s) from %s – returned NOTIMP", qtype_name, qname, client_addr)
        return

    # 3. Cache lookup (C5)
    rrs = cache.get(qname, qtype, qclass)
    if rrs is not None:
        reply_bytes = hit_reply(raw, rrs)
        client_sock.sendto(reply_bytes, client_addr)
        latency_ms = (time.monotonic() - arrival_time) * 1000.0
        if stats:
            stats.record_hit(latency_ms)
        if query_logger:
            query_logger.log(
                client=client_addr,
                name=qname,
                qtype=qtype_name,
                result="HIT",
                upstream=None,
                latency_ms=latency_ms,
                rcode="NOERROR",
            )
        logger.info("Cache HIT for %s %s → replied to %s (%.2f ms)", qname, qtype_name, client_addr, latency_ms)
        return

    # 4. Cache MISS – forward to selected upstream (Fix #1)
    upstream_addr = upstream_selector.pick()

    upstream_id = pending.allocate_id()
    if upstream_id is None:
        reply_bytes = servfail(raw)
        client_sock.sendto(reply_bytes, client_addr)
        latency_ms = (time.monotonic() - arrival_time) * 1000.0
        if stats:
            stats.record_error(latency_ms)
        if query_logger:
            query_logger.log(
                client=client_addr,
                name=qname,
                qtype=qtype_name,
                result="ERR",
                upstream=upstream_addr,
                latency_ms=latency_ms,
                rcode="SERVFAIL",
            )
        logger.error("Failed to allocate upstream ID for query from %s – sent SERVFAIL", client_addr)
        return

    client_id = parsed.id

    added = pending.add(
        upstream_id=upstream_id,
        client_addr=client_addr,
        client_id=client_id,
        questions=[(qname, qtype, qclass)],
        raw_query=raw,
        upstream_addr=upstream_addr,
        retried=False,
        qname=qname,
        qtype=qtype,
        qclass=qclass,
        start_time=arrival_time,
    )
    if not added:
        logger.warning("PendingTable collision for upstream_id=%d – dropping", upstream_id)
        return

    rewritten = set_id(raw, upstream_id)

    if stats:
        stats.record_upstream_query(upstream_addr)

    try:
        # Use upstream_sock so responses arrive on dedicated upstream listener (Fix #1)
        upstream_sock.sendto(rewritten, upstream_addr)
        logger.info(
            "Cache MISS for %s %s (upstream_id=%d) → forwarded to %s:%d",
            qname,
            qtype_name,
            upstream_id,
            upstream_addr[0],
            upstream_addr[1],
        )
    except Exception as exc:
        logger.error("Failed to forward query to upstream %s: %s", upstream_addr, exc)
        pending.remove(upstream_id)
        reply_bytes = servfail(raw)
        client_sock.sendto(reply_bytes, client_addr)


# ---------------------------------------------------------------------------
# Upstream reply handler (Fix #5 & Fix #2)
# ---------------------------------------------------------------------------

def _handle_upstream_reply(
    raw: bytes,
    upstream_addr: Tuple[str, int],
    pending: PendingTable,
    cache: DNSCache,
    client_sock: socket.socket,
    upstream_selector: UpstreamSelector,
    stats: Optional[ForwarderStats] = None,
    query_logger: Optional[QueryLogger] = None,
) -> None:
    """
    Process reply datagram received from an upstream DNS server.

    Strict verification (C4 / Fix #5):
      1. Check packet length (>= 12 bytes).
      2. Match upstream Query ID to a pending query.
      3. Verify source IP:port matches expected upstream.
      4. Verify question section matches pending query's (name, qtype, qclass).
      If any check fails, drop packet and increment mismatch drop counter.

    Delivery (C5 / Fix #2):
      5. Report success to upstream selector.
      6. Cache ONLY when rcode == NOERROR (with answer records).
      7. Restore original client ID and send reply to client.
      8. Record C10 latency metrics and structured query log.
    """
    if len(raw) < 12:
        if stats:
            stats.record_mismatch()
        logger.warning("Dropped upstream reply: packet too short (%d bytes) from %s", len(raw), upstream_addr)
        return

    upstream_id = int.from_bytes(raw[:2], "big")
    entry = pending.get(upstream_id)

    # 1. Match Query ID
    if entry is None:
        if stats:
            stats.record_mismatch()
        logger.warning("Reply mismatch: unknown or expired upstream_id=%d from %s – dropped", upstream_id, upstream_addr)
        return

    # 2. Verify source IP and port
    if not _match_addr(upstream_addr, entry.upstream_addr):
        if stats:
            stats.record_mismatch()
        logger.warning(
            "Reply mismatch: source address %s does not match expected upstream %s for upstream_id=%d – dropped",
            upstream_addr,
            entry.upstream_addr,
            upstream_id,
        )
        return

    # 3. Parse response and verify question section (Fix #5)
    parsed_resp = parse_response(raw)
    if parsed_resp is None:
        if stats:
            stats.record_mismatch()
        logger.warning("Reply mismatch: failed to parse upstream response for upstream_id=%d – dropped", upstream_id)
        return

    resp_id, rcode, rrs, question_key = parsed_resp
    # In builder.py, question_key is (str(q.qname).lower().rstrip('.'), q.qtype, q.qclass)
    expected_key = (entry.qname, entry.qtype, entry.qclass)
    if question_key != expected_key:
        if stats:
            stats.record_mismatch()
        logger.warning(
            "Reply mismatch: question %s does not match expected %s for upstream_id=%d – dropped",
            question_key,
            expected_key,
            upstream_id,
        )
        return

    # All checks passed! Remove entry from pending table
    pending.remove(upstream_id)

    # Report success to upstream selector (Fix #4)
    upstream_selector.report_success(entry.upstream_addr)

    # Cache only when rcode == NOERROR (Fix #2 / C5)
    if rcode == RCODE.NOERROR and rrs:
        cache.put(entry.qname, entry.qtype, rrs, entry.qclass)
        logger.debug("Cached NOERROR response for %s %s (%d RRs)", entry.qname, entry.qtype, len(rrs))

    # Restore original client ID and send reply to client
    client_response = set_id(raw, entry.client_id)
    try:
        client_sock.sendto(client_response, entry.client_addr)
    except Exception as exc:
        logger.error("Failed to send reply to client %s: %s", entry.client_addr, exc)

    latency_ms = (time.monotonic() - entry.start_time) * 1000.0
    rcode_name = RCODE.get(rcode, str(rcode))
    qtype_name = QTYPE.get(entry.qtype, str(entry.qtype))

    if stats:
        stats.record_miss(latency_ms)

    if query_logger:
        query_logger.log(
            client=entry.client_addr,
            name=entry.qname,
            qtype=qtype_name,
            result="MISS",
            upstream=entry.upstream_addr,
            latency_ms=latency_ms,
            rcode=rcode_name,
        )

    logger.info(
        "Forwarded upstream reply to %s | client_id=%d | upstream_id=%d | rcode=%s | latency=%.2f ms",
        entry.client_addr,
        entry.client_id,
        upstream_id,
        rcode_name,
        latency_ms,
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
        cache_enabled: bool = True,
        log_file: Optional[str] = None,
        stats_interval: float = 0.0,
    ) -> None:
        self.listen_port = listen_port
        self.primary_host = primary_host
        self.primary_port = primary_port
        self.backup_host = backup_host
        self.backup_port = backup_port
        self.num_workers = num_workers
        self.pending_timeout = pending_timeout
        self.reaper_interval = reaper_interval
        self.stats_interval = stats_interval

        # Pending-query table with timeout callback (C3 / C4 / C7)
        self._pending = PendingTable(on_expire=self._handle_pending_timeout)

        # TTL cache (C5) — toggleable for baseline B2
        self._cache = DNSCache(enabled=cache_enabled)

        # Upstream selector (C8)
        self.primary_addr = (self.primary_host, self.primary_port)
        self.backup_addr = (self.backup_host, self.backup_port)
        self._upstream_selector = UpstreamSelector(
            primary=self.primary_addr,
            backup=self.backup_addr,
            fail_threshold=3,
            probe_interval=5.0,
            probe_successes=2,
        )

        # Health prober (C9 / Fix #4)
        send_probe_fn = make_probe_fn("probe.lab.test", timeout=1.0)
        self._health_prober = HealthProber(self._upstream_selector, send_probe_fn, tick=0.25)

        # C10: Structured Logger & Statistics Tracker
        self.stats = ForwarderStats(self._upstream_selector)
        self.query_logger = QueryLogger(log_file=log_file)

        # Work queue: (raw_bytes, client_addr)
        self._query_queue: queue.Queue[Tuple[bytes, Tuple[str, int]]] = queue.Queue()

        self._client_sock: Optional[socket.socket] = None
        self._upstream_sock: Optional[socket.socket] = None

        self._running = False
        self._worker_threads: list[threading.Thread] = []
        self._upstream_listener_thread: Optional[threading.Thread] = None
        self._stats_thread: Optional[threading.Thread] = None

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

        self._upstream_selector.report_timeout(upstream_addr)

        if not entry.retried:
            other_upstream = self._upstream_selector.other(upstream_addr)
            logger.info(
                "Retrying query for client %s (%s) on backup upstream %s (C7)",
                entry.client_addr,
                entry.qname,
                other_upstream,
            )

            new_uid = self._pending.allocate_id()
            if new_uid is None:
                reply_bytes = servfail(entry.raw_query)
                self._client_sock.sendto(reply_bytes, entry.client_addr)
                return

            rewritten = set_id(entry.raw_query, new_uid)
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
                qclass=entry.qclass,
                start_time=entry.start_time,
            )
            if not added:
                reply_bytes = servfail(entry.raw_query)
                self._client_sock.sendto(reply_bytes, entry.client_addr)
                return

            self.stats.record_upstream_query(other_upstream)

            try:
                self._upstream_sock.sendto(rewritten, other_upstream)
                logger.info("Sent retry query upstream_id=%d to backup upstream %s", new_uid, other_upstream)
            except Exception as exc:
                logger.error("Failed to send retry packet to %s: %s", other_upstream, exc)
                self._pending.remove(new_uid)
                reply_bytes = servfail(entry.raw_query)
                self._client_sock.sendto(reply_bytes, entry.client_addr)
        else:
            # Both primary and backup upstreams have timed out (C7)
            latency_ms = (time.monotonic() - entry.start_time) * 1000.0
            qtype_name = QTYPE.get(entry.qtype, str(entry.qtype))
            logger.error(
                "Both upstreams timed out for client %s (%s %s) – sending SERVFAIL",
                entry.client_addr,
                entry.qname,
                qtype_name,
            )
            reply_bytes = servfail(entry.raw_query)
            try:
                self._client_sock.sendto(reply_bytes, entry.client_addr)
            except Exception as exc:
                logger.error("Failed to send SERVFAIL to client %s: %s", entry.client_addr, exc)

            self.stats.record_error(latency_ms)

            if self.query_logger:
                self.query_logger.log(
                    client=entry.client_addr,
                    name=entry.qname,
                    qtype=qtype_name,
                    result="ERR",
                    upstream=entry.upstream_addr,
                    latency_ms=latency_ms,
                    rcode="SERVFAIL",
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

        # Start health prober thread (C9 / Fix #4)
        self._health_prober.start()
        logger.info("HealthProber started for primary upstream %s:%d", self.primary_host, self.primary_port)

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

        # Optional periodic statistics logger (C10)
        if self.stats_interval > 0:
            def _stats_loop() -> None:
                while self._running:
                    time.sleep(self.stats_interval)
                    if self._running:
                        print(self.stats.get_summary(), flush=True)

            self._stats_thread = threading.Thread(
                target=_stats_loop, name="stats-reporter", daemon=True
            )
            self._stats_thread.start()

        # Run client receive loop (blocking)
        self._client_receive_loop()

    def stop(self) -> None:
        """Signal all threads to stop and print final CLI statistics summary."""
        logger.info("Stopping DNS forwarder…")
        self._running = False

        if self._health_prober:
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

        # Always print final CLI statistics summary upon shutdown (C10)
        print(self.stats.get_summary(), flush=True)

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
                stats=self.stats,
                query_logger=self.query_logger,
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
                    stats=self.stats,
                    query_logger=self.query_logger,
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
        help="UDP port to listen on for client queries (default: 5300)",
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
        help="Alias to override primary upstream server address",
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
        "--no-cache",
        action="store_true",
        help="Disable caching for Baseline B2 evaluation",
    )
    p.add_argument(
        "--stats-interval",
        type=float,
        default=0.0,
        metavar="SECS",
        help="Interval in seconds to print live statistics summary (default: 0 = disabled)",
    )
    p.add_argument(
        "--log-file",
        default=None,
        metavar="PATH",
        help="Optional file path to write structured query logs to",
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
        cache_enabled=not args.no_cache,
        log_file=args.log_file,
        stats_interval=args.stats_interval,
    )

    logger.info(
        "Starting DNS Caching Forwarder on port %d | Primary: %s:%d | Backup: %s:%d | Cache: %s",
        args.port,
        primary_host,
        primary_port,
        backup_host,
        backup_port,
        "Disabled (B2)" if args.no_cache else "Enabled",
    )

    try:
        forwarder.start()
    except KeyboardInterrupt:
        logger.info("Keyboard interrupt received.")
    finally:
        forwarder.stop()


if __name__ == "__main__":
    main()
