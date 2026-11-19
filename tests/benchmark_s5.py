"""
benchmark_s5.py
===============
Concurrency Benchmark for Experiment S5 (EE 347: Computer Networks).

Evaluates forwarder behavior under concurrent load (1, 5, 10, 20 parallel workers).
Objective O4: "At least 20 concurrent clients are served with zero mismatched
or dropped responses."
"""

from __future__ import annotations

import argparse
import math
import random
import socket
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Tuple

from dnslib import DNSRecord, QTYPE


def send_dns_query(
    host: str,
    port: int,
    qname: str,
    qtype: str = "A",
    timeout: float = 2.0,
) -> Tuple[bool, float, str]:
    """
    Send a single UDP DNS query and measure latency.

    Returns:
        (success: bool, latency_ms: float, detail_str: str)
    """
    qid = random.randint(1, 0xFFFF)
    query = DNSRecord.question(qname, qtype)
    query.header.id = qid
    packet = query.pack()

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.settimeout(timeout)
    t0 = time.perf_counter()

    try:
        sock.sendto(packet, (host, port))
        resp_data, _ = sock.recvfrom(4096)
        elapsed_ms = (time.perf_counter() - t0) * 1000.0
        resp = DNSRecord.parse(resp_data)
        if resp.header.id != qid:
            return False, elapsed_ms, "mismatched ID"
        return True, elapsed_ms, "OK"
    except socket.timeout:
        elapsed_ms = (time.perf_counter() - t0) * 1000.0
        return False, elapsed_ms, "timeout"
    except Exception as exc:
        elapsed_ms = (time.perf_counter() - t0) * 1000.0
        return False, elapsed_ms, str(exc)
    finally:
        sock.close()


def run_benchmark_level(
    concurrency: int,
    num_queries: int,
    host: str,
    port: int,
) -> dict:
    """Run benchmark for a given concurrency level with a mix of hit/miss queries."""
    # Generate query list: half hits (www.lab.test), half unique misses
    queries = []
    half = num_queries // 2
    for _ in range(half):
        queries.append("www.lab.test")
    for i in range(num_queries - half):
        queries.append(f"client{concurrency}-test{i}.lab.test")
    random.shuffle(queries)

    latencies: List[float] = []
    successes = 0
    failures = 0

    with ThreadPoolExecutor(max_workers=concurrency) as executor:
        futures = [
            executor.submit(send_dns_query, host, port, q)
            for q in queries
        ]
        for f in as_completed(futures):
            ok, lat, _ = f.result()
            if ok:
                successes += 1
                latencies.append(lat)
            else:
                failures += 1

    drop_rate = (failures / num_queries) * 100.0 if num_queries > 0 else 0.0

    if latencies:
        latencies.sort()
        mean_lat = sum(latencies) / len(latencies)
        p95_idx = int(math.ceil(0.95 * len(latencies))) - 1
        p95_lat = latencies[max(0, p95_idx)]
    else:
        mean_lat = 0.0
        p95_lat = 0.0

    return {
        "concurrency": concurrency,
        "sent": num_queries,
        "received": successes,
        "dropped": failures,
        "drop_rate": drop_rate,
        "mean_latency_ms": mean_lat,
        "p95_latency_ms": p95_lat,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="DNS Forwarder Experiment S5 Concurrency Benchmark")
    parser.add_argument("--host", default="127.0.0.1", help="Forwarder host (default: 127.0.0.1)")
    parser.add_argument("--port", type=int, default=5300, help="Forwarder port (default: 5300)")
    parser.add_argument("--queries-per-level", type=int, default=50, help="Total queries per concurrency level (default: 50)")
    parser.add_argument("--levels", default="1,5,10,20", help="Comma-separated concurrency levels (default: 1,5,10,20)")
    args = parser.parse_args()

    levels = [int(x.strip()) for x in args.levels.split(",") if x.strip()]

    print("=" * 72)
    print("      EXPERIMENT S5: CONCURRENCY & ZERO-MISMATCH BENCHMARK")
    print(f" Target: {args.host}:{args.port} | Queries/Level: {args.queries_per_level}")
    print("=" * 72)

    # Warmup query
    send_dns_query(args.host, args.port, "www.lab.test")

    results = []
    for lvl in levels:
        print(f"[*] Running benchmark with Concurrency = {lvl} workers...", flush=True)
        res = run_benchmark_level(lvl, args.queries_per_level, args.host, args.port)
        results.append(res)
        time.sleep(0.5)

    print("\n" + "=" * 72)
    print("                     BENCHMARK RESULTS SUMMARY")
    print("=" * 72)
    print(
        f"{'Concurrency':<12} {'Sent':<7} {'Received':<10} {'Drop Rate':<12} "
        f"{'Mean Lat (ms)':<15} {'p95 Lat (ms)':<13}"
    )
    print("-" * 72)
    for r in results:
        drop_str = f"{r['drop_rate']:.1f}%"
        print(
            f"{r['concurrency']:<12} {r['sent']:<7} {r['received']:<10} {drop_str:<12} "
            f"{r['mean_latency_ms']:<15.2f} {r['p95_latency_ms']:<13.2f}"
        )
    print("=" * 72)

    all_passed = all(r["dropped"] == 0 for r in results)
    if all_passed:
        print(">> PASS: Objective O4 met (0% drop rate across all concurrency levels) <<")
    else:
        print(">> WARNING: Some queries timed out or dropped <<")
    print("=" * 72 + "\n")


if __name__ == "__main__":
    main()
