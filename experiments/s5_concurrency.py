"""S5 - Concurrency and zero-mismatch benchmark (compulsory feature C6, objective O4).

Tests 1, 5, 10, and 20 concurrent clients against the forwarder.
Verifies that:
  * Zero queries are dropped or mismatched (100% correct response matching).
  * Mean and p95 latency scale gracefully under concurrent load.

Writes results to:
  results/s5_concurrency/s5_queries.csv
  results/s5_concurrency/s5_runs.csv
  results/s5_concurrency/s5_summary.csv
"""
import argparse
import math
import os
import random
import statistics
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import common as c

TTL = 300
PRIMARY = {
    "www.lab.test": {"A": ["10.1.0.1"], "ttl": TTL},
    "app.lab.test": {"A": ["10.1.0.2"], "ttl": TTL},
}
# Add 100 mock domains for uncached misses
for i in range(1, 101):
    PRIMARY[f"h{i:03d}.lab.test"] = {"A": [f"10.1.0.{i % 250 + 1}"], "ttl": TTL}

BACKUP = {k: {"A": ["10.1.1.1"], "ttl": TTL} for k in PRIMARY}


def run_concurrency_level(level, run, num_queries, target_port):
    """Run concurrent queries for one level and run."""
    # 50% cache hits (www.lab.test), 50% misses
    half = num_queries // 2
    names = ["www.lab.test"] * half
    names += [f"h{i:03d}.lab.test" for i in range(1, num_queries - half + 1)]
    random.shuffle(names)

    queries_out = []
    latencies = []
    successes = 0
    failures = 0
    t0_level = time.time()

    def do_query(seq, qname):
        r = c.query(target_port, qname, timeout=4.0)
        return seq, qname, r

    with ThreadPoolExecutor(max_workers=level) as executor:
        futures = [
            executor.submit(do_query, i, name)
            for i, name in enumerate(names, 1)
        ]
        for f in as_completed(futures):
            seq, name, r = f.result()
            ok = r["ok"] and r["id_ok"]
            if ok:
                successes += 1
                latencies.append(r["latency_ms"])
            else:
                failures += 1
            queries_out.append(
                dict(
                    concurrency=level,
                    run=run,
                    seq=seq,
                    name=name,
                    latency_ms=round(r["latency_ms"], 3),
                    rcode=r["rcode"],
                    id_ok=r["id_ok"],
                    success=ok,
                )
            )

    drop_rate = round((failures / num_queries) * 100.0, 2)
    d = c.describe(latencies)
    run_summary = dict(
        concurrency=level,
        run=run,
        sent=num_queries,
        received=successes,
        dropped=failures,
        drop_rate=drop_rate,
        mean_ms=round(d["mean"], 3),
        median_ms=round(d["median"], 3),
        p95_ms=round(d["p95"], 3),
    )
    return queries_out, run_summary


def main():
    parser = argparse.ArgumentParser(description="Experiment S5: Concurrency Benchmark")
    parser.add_argument("--runs", type=int, default=5, help="Number of runs per level (default: 5)")
    parser.add_argument("--queries-per-run", type=int, default=50, help="Queries per run (default: 50)")
    parser.add_argument("--levels", default="1,5,10,20", help="Concurrency levels (default: 1,5,10,20)")
    parser.add_argument(
        "--out",
        default=os.path.join(c.ROOT, "results", "s5_concurrency"),
        help="Output directory (default: results/s5_concurrency)",
    )
    args = parser.parse_args()

    levels = [int(x.strip()) for x in args.levels.split(",") if x.strip()]
    wd = c.workdir()
    os.makedirs(args.out, exist_ok=True)

    print("=" * 72)
    print("      EXPERIMENT S5: CONCURRENCY BENCHMARK (O4)")
    print(f" Levels: {levels} | Runs/Level: {args.runs} | Queries/Run: {args.queries_per_run}")
    print(f" Output: {args.out}")
    print("=" * 72)

    primary = c.start_test_server(c.PRIMARY_PORT, PRIMARY, wd)
    backup = c.start_test_server(c.BACKUP_PORT, BACKUP, wd)
    log_file = os.path.join(wd, "s5_forwarder.log")
    fwd = c.start_forwarder(wd, log_file)

    all_queries = []
    all_runs = []

    try:
        # Warmup cache with www.lab.test
        c.query(c.FWD_PORT, "www.lab.test")

        for lvl in levels:
            print(f"\n[*] Testing Concurrency Level = {lvl} worker(s)...", flush=True)
            for r in range(1, args.runs + 1):
                q_rows, run_sum = run_concurrency_level(lvl, r, args.queries_per_run, c.FWD_PORT)
                all_queries.extend(q_rows)
                all_runs.append(run_sum)
                print(
                    f"    Run {r}/{args.runs}: Sent={run_sum['sent']} | Recv={run_sum['received']} | "
                    f"Drop={run_sum['drop_rate']}% | Mean={run_sum['mean_ms']:.2f}ms | p95={run_sum['p95_ms']:.2f}ms",
                    flush=True,
                )
                time.sleep(0.3)
    finally:
        c.stop(fwd)
        c.stop(primary)
        c.stop(backup)

    # Write per-query and per-run CSVs
    c.write_csv(
        os.path.join(args.out, "s5_queries.csv"),
        all_queries,
        ["concurrency", "run", "seq", "name", "latency_ms", "rcode", "id_ok", "success"],
    )
    c.write_csv(
        os.path.join(args.out, "s5_runs.csv"),
        all_runs,
        ["concurrency", "run", "sent", "received", "dropped", "drop_rate", "mean_ms", "median_ms", "p95_ms"],
    )

    # Compute pooled summary across runs for each concurrency level
    pooled_summary = []
    print("\n" + "=" * 72)
    print("                   EXPERIMENT S5 SUMMARY TABLE")
    print("=" * 72)
    print(
        f"{'Concurrency':<12} {'Runs':<6} {'Total Sent':<12} {'Drop Rate':<12} "
        f"{'Mean Lat (ms)':<15} {'Median (ms)':<13} {'p95 Lat (ms)':<12}"
    )
    print("-" * 72)

    for lvl in levels:
        lvl_runs = [r for r in all_runs if r["concurrency"] == lvl]
        lvl_queries = [q for q in all_queries if q["concurrency"] == lvl]
        total_sent = sum(r["sent"] for r in lvl_runs)
        total_recv = sum(r["received"] for r in lvl_runs)
        total_drop = sum(r["dropped"] for r in lvl_runs)
        drop_rate = round((total_drop / total_sent) * 100.0, 2)
        lats = [q["latency_ms"] for q in lvl_queries if q["success"]]
        d = c.describe(lats)

        row = dict(
            concurrency=lvl,
            runs=len(lvl_runs),
            total_sent=total_sent,
            total_received=total_recv,
            total_dropped=total_drop,
            drop_rate=drop_rate,
            mean_ms=round(d["mean"], 3),
            median_ms=round(d["median"], 3),
            p95_ms=round(d["p95"], 3),
        )
        pooled_summary.append(row)
        print(
            f"{row['concurrency']:<12} {row['runs']:<6} {row['total_sent']:<12} {str(row['drop_rate']) + '%':<12} "
            f"{row['mean_ms']:<15.2f} {row['median_ms']:<13.2f} {row['p95_ms']:<12.2f}"
        )

    print("=" * 72)
    c.write_csv(
        os.path.join(args.out, "s5_summary.csv"),
        pooled_summary,
        ["concurrency", "runs", "total_sent", "total_received", "total_dropped", "drop_rate", "mean_ms", "median_ms", "p95_ms"],
    )
    print(f"\nCSV files saved in {args.out}/\n")


if __name__ == "__main__":
    main()
