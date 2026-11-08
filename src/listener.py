"""
listener.py
===========
UDP Listener & Query-ID Rewriter for the DNS Caching Forwarder.

Responsibilities (C1 / C6 – Tayyba Bashir):
  - Open a UDP socket on the configured port (default 5300) and receive
    raw DNS query packets from clients.
  - Spin up a configurable worker-thread pool.  Each received datagram is
    handed off to a worker via a thread-safe queue so the main receive
    loop is never blocked.
  - Each worker:
      1. Parses the raw bytes with dnslib.
      2. Checks the cache (Zainab's cache.py) — returns cached answer
         immediately when a hit is found.
      3. On a cache miss: allocates a collision-free random 16-bit upstream
         ID, records the original client details in PendingTable, rewrites
         the DNS header ID, and forwards the packet to the upstream server
         selected by Zainab's upstream selector.
  - A second UDP socket listens for upstream replies, matches them to the
    PendingTable, rewrites the ID back to the client's original ID, and
    sends the response to the client.

CLI usage
---------
  python src/listener.py [options]

  Options
  -------
  --port PORT          Port to listen on for client queries    (default: 5300)
  --upstream HOST:PORT Upstream DNS server                     (default: 8.8.8.8:53)
  --workers N          Number of worker threads                (default: 4)
  --timeout SECS       Pending-entry timeout in seconds        (default: 10)
  --reaper-interval S  How often the reaper sweeps stale entries (default: 5)
  --log-level LEVEL    Logging verbosity: DEBUG|INFO|WARNING   (default: INFO)

Integration points (Zainab's modules)
--------------------------------------
  cache.py         – import cache; call cache.get(key) / cache.put(key, pkt, ttl)
  upstream.py      – import upstream; call upstream.pick() -> (host, port)

Both interfaces are stubbed here with simple pass-through implementations
so the listener runs stand-alone while Zainab's real modules are developed.
"""

from __future__ import annotations

import argparse
import logging
import os
import queue
import random
import socket
import sys
import threading
import time
from typing import Optional, Tuple

# ---------------------------------------------------------------------------
# Optional dnslib import (graceful fallback for environments without it)
# ---------------------------------------------------------------------------
try:
    import dnslib
    from dnslib import DNSRecord, DNSHeader, DNSError
    _HAS_DNSLIB = True
except ImportError:  # pragma: no cover
    _HAS_DNSLIB = False

# ---------------------------------------------------------------------------
# Sibling module imports
# ---------------------------------------------------------------------------
# Adjust sys.path so this script can be run directly from the repo root
_SRC_DIR = os.path.dirname(os.path.abspath(__file__))
if _SRC_DIR not in sys.path:
    sys.path.insert(0, _SRC_DIR)

from pending_table import PendingTable

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Lightweight integration stubs
# (replaced at runtime by Zainab's real cache.py / upstream.py imports)
# ---------------------------------------------------------------------------

class _StubCache:
    """
    No-op cache stub — always reports a miss.
    Replace with ``from cache import DNSCache`` once Zainab's module is ready.
    """

    def get(self, key: str) -> Optional[bytes]:
        """Return cached raw DNS response bytes, or None on miss."""
        return None

    def put(self, key: str, data: bytes, ttl: int) -> None:
        """Store a raw DNS response with its TTL."""
        pass


class _StubUpstreamSelector:
    """
    Single-upstream stub — always returns the upstream configured via CLI.
    Replace with ``from upstream import UpstreamSelector`` once ready.
    """

    def __init__(self, host: str, port: int) -> None:
        self._host = host
        self._port = port

    def pick(self) -> Tuple[str, int]:
        """Return (host, port) of the best available upstream server."""
        return self._host, self._port


# ---------------------------------------------------------------------------
# Helper: build a cache-key from a parsed DNS query
# ---------------------------------------------------------------------------

def _cache_key(record: "DNSRecord") -> str:
    """
    Deterministic string key for the cache derived from the question section.
    Format: ``<qname>/<qtype>/<qclass>``  e.g. ``example.com./A/IN``
    """
    if not _HAS_DNSLIB or not record.questions:
        return ""
    q = record.questions[0]
    return f"{q.qname}/{dnslib.QTYPE[q.qtype]}/{dnslib.CLASS[q.qclass]}"


# ---------------------------------------------------------------------------
# Worker: processes one datagram from the queue
# ---------------------------------------------------------------------------

def _process_query(
    raw: bytes,
    client_addr: Tuple[str, int],
    pending: PendingTable,
    cache: _StubCache,
    upstream_selector: _StubUpstreamSelector,
    client_sock: socket.socket,
) -> None:
    """
    Handle a single client DNS query in a worker thread.

    Steps
    -----
    1. Parse the raw bytes.
    2. Cache lookup – send reply immediately on hit.
    3. Cache miss – allocate upstream ID, record in PendingTable, rewrite
       header ID, forward to upstream.
    """
    # --- 1. Parse -------------------------------------------------------
    if _HAS_DNSLIB:
        try:
            record = DNSRecord.parse(raw)
        except DNSError as exc:
            logger.warning(
                "Failed to parse DNS query from %s: %s", client_addr, exc
            )
            return
        client_id: int = record.header.id
        questions = list(record.questions)
        key = _cache_key(record)
    else:
        # Raw-byte fallback when dnslib is unavailable
        if len(raw) < 12:
            logger.warning("Received undersized packet from %s, dropping.", client_addr)
            return
        client_id = int.from_bytes(raw[:2], "big")
        questions = []
        key = ""

    logger.debug("Query from %s  client_id=%d  key=%r", client_addr, client_id, key)

    # --- 2. Cache lookup ------------------------------------------------
    cached_response = cache.get(key) if key else None
    if cached_response:
        # Rewrite the ID in the cached response to match client's ID
        if len(cached_response) >= 2:
            response_with_id = client_id.to_bytes(2, "big") + cached_response[2:]
        else:
            response_with_id = cached_response
        client_sock.sendto(response_with_id, client_addr)
        logger.info(
            "Cache HIT for %r  → replied to %s  client_id=%d",
            key,
            client_addr,
            client_id,
        )
        return

    # --- 3. Cache miss – forward to upstream ----------------------------
    # Allocate a collision-free random 16-bit upstream ID
    upstream_id = pending.allocate_id()
    if upstream_id is None:
        logger.error(
            "Cannot allocate upstream ID (table full?). Dropping query from %s.",
            client_addr,
        )
        return

    # Register in pending table before sending (avoids race with fast replies)
    added = pending.add(upstream_id, client_addr, client_id, questions)
    if not added:
        # Extremely rare: allocate_id picked a slot that filled between the
        # check and the add.  Log and drop; client will retry.
        logger.warning(
            "PendingTable collision after allocate_id for upstream_id=%d – dropping.",
            upstream_id,
        )
        return

    # Rewrite DNS message ID to the upstream ID
    rewritten = upstream_id.to_bytes(2, "big") + raw[2:]

    # Pick upstream server (Zainab's selector handles failover)
    up_host, up_port = upstream_selector.pick()

    logger.info(
        "Cache MISS  key=%r  upstream_id=%d  → forwarding to %s:%d",
        key,
        upstream_id,
        up_host,
        up_port,
    )
    client_sock.sendto(rewritten, (up_host, up_port))


# ---------------------------------------------------------------------------
# Upstream reply handler
# ---------------------------------------------------------------------------

def _handle_upstream_reply(
    raw: bytes,
    pending: PendingTable,
    cache: _StubCache,
    client_sock: socket.socket,
) -> None:
    """
    Process a reply datagram received from an upstream DNS server.

    Steps
    -----
    1. Extract the upstream ID from the first two bytes.
    2. Look up and remove the matching PendingTable entry.
    3. Rewrite the message ID back to the original client ID.
    4. Store the response in the cache (Zainab's cache.put).
    5. Send the reply to the original client address.
    """
    if len(raw) < 12:
        logger.warning("Upstream reply too short (%d bytes), ignoring.", len(raw))
        return

    upstream_id = int.from_bytes(raw[:2], "big")
    entry = pending.remove(upstream_id)

    if entry is None:
        logger.warning(
            "Received upstream reply for unknown/expired upstream_id=%d – dropping.",
            upstream_id,
        )
        return

    # Rewrite ID back to client's original ID
    client_response = entry.client_id.to_bytes(2, "big") + raw[2:]

    # Cache the response (parse TTL if dnslib is available)
    if _HAS_DNSLIB:
        try:
            parsed = DNSRecord.parse(raw)
            # Use the minimum TTL across all answer RRs (or 0 if no answers)
            ttl = min((rr.ttl for rr in parsed.rr), default=0)
            key = f"{entry.questions[0].qname}/{dnslib.QTYPE[entry.questions[0].qtype]}/{dnslib.CLASS[entry.questions[0].qclass]}" if entry.questions else ""
        except Exception:
            ttl = 0
            key = ""
    else:
        ttl = 0
        key = ""

    if key and ttl > 0:
        cache.put(key, raw, ttl)
        logger.debug(
            "Cached response  key=%r  ttl=%ds", key, ttl
        )

    # Send reply to client
    client_sock.sendto(client_response, entry.client_addr)
    logger.info(
        "Forwarded upstream reply to %s  client_id=%d  upstream_id=%d  ttl=%d",
        entry.client_addr,
        entry.client_id,
        upstream_id,
        ttl,
    )


# ---------------------------------------------------------------------------
# DNSForwarder – main server class
# ---------------------------------------------------------------------------

class DNSForwarder:
    """
    UDP DNS caching forwarder.

    Two sockets share the same local address:
      • ``_client_sock``   – receives queries FROM clients, also used to
                             FORWARD rewritten queries TO the upstream, and
                             to SEND replies back TO clients.
      • ``_upstream_sock`` – bound to an ephemeral port; receives replies
                             FROM the upstream DNS server.

    Thread layout
    -------------
      main thread        – receive loop on _client_sock
      upstream_listener  – receive loop on _upstream_sock
      workers (N)        – process jobs from _query_queue
    """

    _RECV_BUFSIZE: int = 4096

    def __init__(
        self,
        listen_port: int = 5300,
        upstream_host: str = "8.8.8.8",
        upstream_port: int = 53,
        num_workers: int = 4,
        pending_timeout: float = 10.0,
        reaper_interval: float = 5.0,
    ) -> None:
        self.listen_port = listen_port
        self.upstream_host = upstream_host
        self.upstream_port = upstream_port
        self.num_workers = num_workers
        self.pending_timeout = pending_timeout
        self.reaper_interval = reaper_interval

        # Thread-safe pending-query table (C3/C4)
        self._pending = PendingTable()

        # Integration points — swap these out once Zainab's modules land
        self._cache = _StubCache()
        self._upstream_selector = _StubUpstreamSelector(upstream_host, upstream_port)

        # Job queue: (raw_bytes, client_addr)
        self._query_queue: queue.Queue[Tuple[bytes, Tuple[str, int]]] = queue.Queue()

        # Sockets (created in start())
        self._client_sock: Optional[socket.socket] = None
        self._upstream_sock: Optional[socket.socket] = None

        # Shutdown flag
        self._running = False
        self._worker_threads: list[threading.Thread] = []
        self._upstream_listener_thread: Optional[threading.Thread] = None

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def start(self) -> None:
        """Bind sockets, start worker pool, and begin the receive loop."""
        self._running = True

        # Client-facing socket (also used to forward to upstream)
        self._client_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self._client_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._client_sock.bind(("0.0.0.0", self.listen_port))
        self._client_sock.settimeout(1.0)  # so we can check _running periodically
        logger.info(
            "DNS forwarder listening on UDP 0.0.0.0:%d  upstream=%s:%d",
            self.listen_port,
            self.upstream_host,
            self.upstream_port,
        )

        # Upstream-reply socket (ephemeral port; upstream replies come here)
        self._upstream_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self._upstream_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._upstream_sock.bind(("0.0.0.0", 0))
        self._upstream_sock.settimeout(1.0)
        up_ep = self._upstream_sock.getsockname()
        logger.info("Upstream-reply socket bound to %s:%d", up_ep[0], up_ep[1])

        # Start background reaper
        self._pending.start_reaper(
            interval_secs=self.reaper_interval,
            max_age_secs=self.pending_timeout,
        )

        # Start worker threads
        for i in range(self.num_workers):
            t = threading.Thread(
                target=self._worker_loop,
                name=f"dns-worker-{i}",
                daemon=True,
            )
            t.start()
            self._worker_threads.append(t)
        logger.info("Started %d worker thread(s).", self.num_workers)

        # Start upstream-reply listener thread
        self._upstream_listener_thread = threading.Thread(
            target=self._upstream_receive_loop,
            name="upstream-listener",
            daemon=True,
        )
        self._upstream_listener_thread.start()

        # Main receive loop (blocking)
        self._client_receive_loop()

    def stop(self) -> None:
        """Signal all threads to stop and clean up."""
        logger.info("Stopping DNS forwarder…")
        self._running = False
        self._pending.stop_reaper()

        # Unblock workers with sentinel values
        for _ in self._worker_threads:
            self._query_queue.put((b"", ("", 0)))  # sentinel

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
        """Main loop: receive UDP datagrams from clients and enqueue them."""
        logger.info("Client receive loop started.")
        while self._running:
            try:
                raw, addr = self._client_sock.recvfrom(self._RECV_BUFSIZE)
            except socket.timeout:
                continue  # check _running and loop back
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
        """Dedicated loop: receive UDP replies from upstream DNS servers."""
        logger.info("Upstream receive loop started.")
        while self._running:
            try:
                raw, _ = self._upstream_sock.recvfrom(self._RECV_BUFSIZE)
            except socket.timeout:
                continue
            except OSError as exc:
                if self._running:
                    logger.error("Error receiving from upstream socket: %s", exc)
                break

            _handle_upstream_reply(
                raw,
                self._pending,
                self._cache,
                self._client_sock,
            )

        logger.info("Upstream receive loop exited.")

    def _worker_loop(self) -> None:
        """Worker thread: dequeue jobs and process them."""
        name = threading.current_thread().name
        logger.debug("%s started.", name)
        while True:
            try:
                raw, client_addr = self._query_queue.get(timeout=1.0)
            except queue.Empty:
                if not self._running:
                    break
                continue

            # Sentinel check
            if not raw and client_addr == ("", 0):
                break

            try:
                _process_query(
                    raw,
                    client_addr,
                    self._pending,
                    self._cache,
                    self._upstream_selector,
                    self._client_sock,
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
        description="DNS Caching Forwarder with Upstream Failover — listener module",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    p.add_argument(
        "--port",
        type=int,
        default=5300,
        metavar="PORT",
        help="UDP port to listen on for DNS queries from clients",
    )
    p.add_argument(
        "--upstream",
        default="8.8.8.8:53",
        metavar="HOST:PORT",
        help="Upstream DNS server address (Zainab's selector overrides this at runtime)",
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
        default=10.0,
        metavar="SECS",
        help="Seconds before a pending query entry is considered timed-out",
    )
    p.add_argument(
        "--reaper-interval",
        type=float,
        default=5.0,
        metavar="SECS",
        help="How often the background reaper sweeps for stale pending entries",
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
        format="%(asctime)s [%(levelname)-8s] %(threadName)-22s %(name)s – %(message)s",
        stream=sys.stdout,
    )


def main(argv: list[str] | None = None) -> None:
    parser = _build_arg_parser()
    args = parser.parse_args(argv)

    _configure_logging(args.log_level)

    # Parse --upstream HOST:PORT
    try:
        up_parts = args.upstream.rsplit(":", 1)
        up_host = up_parts[0]
        up_port = int(up_parts[1]) if len(up_parts) == 2 else 53
    except (ValueError, IndexError):
        parser.error(f"Invalid --upstream value: {args.upstream!r}")

    forwarder = DNSForwarder(
        listen_port=args.port,
        upstream_host=up_host,
        upstream_port=up_port,
        num_workers=args.workers,
        pending_timeout=args.timeout,
        reaper_interval=args.reaper_interval,
    )

    logger.info(
        "Starting DNS Caching Forwarder | port=%d | upstream=%s:%d | workers=%d",
        args.port,
        up_host,
        up_port,
        args.workers,
    )

    try:
        forwarder.start()
    except KeyboardInterrupt:
        logger.info("Keyboard interrupt received.")
    finally:
        forwarder.stop()


if __name__ == "__main__":
    main()
