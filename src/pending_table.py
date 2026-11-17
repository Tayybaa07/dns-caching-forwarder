"""
pending_table.py
================
Thread-safe pending-query table for the DNS Caching Forwarder.

Responsibilities (C3 / C4 / C7 – Tayyba Bashir):
  - Maps a randomly generated 16-bit upstream query-ID to all the
    information needed to send a reply back to the original client once
    the upstream responds:
        • client address (ip, port)
        • original client-supplied DNS query ID
        • question section (list of dnslib.DNSQuestion objects or (name, qtype, qclass))
        • timestamp at which the entry was inserted (monotonic clock)
        • raw client query packet (for retries & SERVFAIL generation)
        • upstream address (ip, port) that query was routed to (for response verification)
        • retried flag (indicates whether 1 backup retry has already been attempted)
        • qname, qtype, qclass (canonical query attributes)
        • start_time (initial arrival timestamp for C10 resolution latency calculation)

  - Provides atomic insert / lookup / remove operations so that the
    listener thread and multiple worker threads can share the table safely.

  - Provides a background reaper that expires stale entries whose
    upstream never replied within a configurable timeout (default: 1.0 s).
    On expiration, invokes an on_expire callback to perform backup failover
    retry before returning SERVFAIL (C7).

Public API
----------
  PendingEntry      – named-tuple representing a single table row
  PendingTable      – the table class (thread-safe)
    .add(...) -> bool
    .get(upstream_id)  -> PendingEntry | None
    .remove(upstream_id) -> PendingEntry | None
    .expire_old(max_age_secs, callback) -> list[int]
    .size() -> int
    .start_reaper(interval_secs, max_age_secs, callback)
    .stop_reaper()
"""

from __future__ import annotations

import logging
import random
import threading
import time
from collections import namedtuple
from typing import Any, Callable, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Data model
# ---------------------------------------------------------------------------

# A single row in the pending-query table.
#   upstream_id   : int   – the 16-bit ID we sent to the upstream server
#   client_addr   : tuple – (ip_str, port_int) of the originating client
#   client_id     : int   – the original query ID chosen by the client
#   questions     : list  – questions list from the client query
#   timestamp     : float – monotonic time (time.monotonic()) of insertion
#   raw_query     : bytes – original query packet for backup retries / SERVFAIL
#   upstream_addr : tuple – (host, port) of the upstream server queried
#   retried       : bool  – True if backup upstream retry has been attempted
#   qname         : str   – domain name queried (lowercased, no trailing dot)
#   qtype         : Any   – query record type (int or str)
#   qclass        : int   – query class (e.g. CLASS.IN == 1)
#   start_time    : float – initial request arrival time for C10 latency metrics
PendingEntry = namedtuple(
    "PendingEntry",
    [
        "upstream_id",
        "client_addr",
        "client_id",
        "questions",
        "timestamp",
        "raw_query",
        "upstream_addr",
        "retried",
        "qname",
        "qtype",
        "qclass",
        "start_time",
    ],
    defaults=(b"", ("", 0), False, "", "", 1, 0.0),
)


# ---------------------------------------------------------------------------
# PendingTable
# ---------------------------------------------------------------------------

class PendingTable:
    """
    Thread-safe mapping from random 16-bit upstream IDs to PendingEntry rows.

    All public methods acquire ``_lock`` before touching ``_table``, so this
    class is safe to call from multiple threads simultaneously.
    """

    _MAX_ID: int = 0xFFFF  # 65 535 — largest valid DNS message ID

    def __init__(
        self, on_expire: Optional[Callable[[PendingEntry], None]] = None
    ) -> None:
        # Main storage: upstream_id (int) -> PendingEntry
        self._table: Dict[int, PendingEntry] = {}
        self._lock = threading.Lock()

        # Background reaper thread state
        self._reaper_thread: Optional[threading.Thread] = None
        self._stop_reaper_event = threading.Event()
        self.on_expire = on_expire

        logger.debug("PendingTable initialised.")

    # ------------------------------------------------------------------
    # Core operations
    # ------------------------------------------------------------------

    def add(
        self,
        upstream_id: int,
        client_addr: Tuple[str, int],
        client_id: int,
        questions: list,
        raw_query: bytes = b"",
        upstream_addr: Tuple[str, int] = ("", 0),
        retried: bool = False,
        qname: str = "",
        qtype: Any = "",
        qclass: int = 1,
        start_time: float = 0.0,
    ) -> bool:
        """
        Insert a new pending-query entry.

        Returns True if inserted, False if upstream_id collision occurred.
        """
        now = time.monotonic()
        if start_time <= 0.0:
            start_time = now

        entry = PendingEntry(
            upstream_id=upstream_id,
            client_addr=client_addr,
            client_id=client_id,
            questions=questions,
            timestamp=now,
            raw_query=raw_query,
            upstream_addr=upstream_addr,
            retried=retried,
            qname=qname,
            qtype=qtype,
            qclass=qclass,
            start_time=start_time,
        )
        with self._lock:
            if upstream_id in self._table:
                logger.warning(
                    "PendingTable.add: collision on upstream_id=%d – rejected.",
                    upstream_id,
                )
                return False
            self._table[upstream_id] = entry
            logger.debug(
                "PendingTable.add: upstream_id=%d client=%s client_id=%d upstream=%s qname=%s",
                upstream_id,
                client_addr,
                client_id,
                upstream_addr,
                qname,
            )
            return True

    def get(self, upstream_id: int) -> Optional[PendingEntry]:
        """Look up a pending entry by upstream ID without removing it."""
        with self._lock:
            return self._table.get(upstream_id)

    def remove(self, upstream_id: int) -> Optional[PendingEntry]:
        """Remove and return the entry for upstream_id, or None if not found."""
        with self._lock:
            entry = self._table.pop(upstream_id, None)
            if entry:
                logger.debug(
                    "PendingTable.remove: upstream_id=%d client=%s",
                    upstream_id,
                    entry.client_addr,
                )
            return entry

    def expire_old(
        self,
        max_age_secs: float = 1.0,
        callback: Optional[Callable[[PendingEntry], None]] = None,
    ) -> List[int]:
        """
        Remove and return all upstream IDs whose entries are older than
        max_age_secs seconds (measured with monotonic clock).

        Executes the expiration callback outside the lock to avoid deadlocks.
        """
        cutoff = time.monotonic() - max_age_secs
        expired_entries: List[PendingEntry] = []

        with self._lock:
            to_remove = [
                uid
                for uid, entry in self._table.items()
                if entry.timestamp < cutoff
            ]
            for uid in to_remove:
                expired_entries.append(self._table.pop(uid))

        cb = callback or self.on_expire
        for entry in expired_entries:
            logger.warning(
                "PendingTable.expire_old: upstream_id=%d expired (client=%s, age=%.2fs)",
                entry.upstream_id,
                entry.client_addr,
                time.monotonic() - entry.timestamp,
            )
            if cb is not None:
                try:
                    cb(entry)
                except Exception as exc:
                    logger.exception(
                        "Error in PendingTable on_expire callback: %s", exc
                    )

        return [e.upstream_id for e in expired_entries]

    def size(self) -> int:
        """Return the current number of in-flight pending queries."""
        with self._lock:
            return len(self._table)

    # ------------------------------------------------------------------
    # ID allocation helper
    # ------------------------------------------------------------------

    def allocate_id(self, max_attempts: int = 100) -> Optional[int]:
        """Generate a random 16-bit upstream ID that is not currently in use."""
        for _ in range(max_attempts):
            candidate = random.randint(0, self._MAX_ID)
            with self._lock:
                if candidate not in self._table:
                    return candidate
        logger.error(
            "PendingTable.allocate_id: failed to find free ID after %d attempts (size=%d)",
            max_attempts,
            len(self._table),
        )
        return None

    # ------------------------------------------------------------------
    # Background reaper
    # ------------------------------------------------------------------

    def start_reaper(
        self,
        interval_secs: float = 0.2,
        max_age_secs: float = 1.0,
        callback: Optional[Callable[[PendingEntry], None]] = None,
    ) -> None:
        """Start a daemon thread that periodically sweeps expired entries."""
        if self._reaper_thread and self._reaper_thread.is_alive():
            logger.warning("PendingTable.start_reaper: reaper already running.")
            return

        self._stop_reaper_event.clear()

        def _reaper_loop() -> None:
            logger.info(
                "PendingTable reaper started (interval=%.2fs, max_age=%.2fs).",
                interval_secs,
                max_age_secs,
            )
            while not self._stop_reaper_event.wait(timeout=interval_secs):
                expired = self.expire_old(
                    max_age_secs=max_age_secs, callback=callback
                )
                if expired:
                    logger.debug(
                        "PendingTable reaper swept %d expired entr%s.",
                        len(expired),
                        "y" if len(expired) == 1 else "ies",
                    )
            logger.info("PendingTable reaper stopped.")

        self._reaper_thread = threading.Thread(
            target=_reaper_loop, name="pending-table-reaper", daemon=True
        )
        self._reaper_thread.start()

    def stop_reaper(self) -> None:
        """Signal the background reaper thread to stop and wait for it."""
        self._stop_reaper_event.set()
        if self._reaper_thread:
            self._reaper_thread.join(timeout=5.0)
            self._reaper_thread = None
        logger.info("PendingTable reaper stopped by caller.")

    # ------------------------------------------------------------------
    # Dunder helpers
    # ------------------------------------------------------------------

    def __len__(self) -> int:
        return self.size()

    def __repr__(self) -> str:
        return f"PendingTable(size={self.size()})"
