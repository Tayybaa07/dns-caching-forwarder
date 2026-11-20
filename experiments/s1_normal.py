"""S1 - Normal operation: cache on versus off (baselines B1, B2 and the full forwarder).

100 queries = 20 domains x 5 repetitions, sent one after another. Repeated for
RUNS runs (default 10 -> 1,000 queries per configuration pooled for percentiles).
Every run starts with a cold cache (the forwarder is restarted).

    python experiments/s1_normal.py                 # full experiment
    python experiments/s1_normal.py --runs 2        # quick check
    python experiments/s1_normal.py --upstream-delay 20   # realistic upstream latency
    python experiments/s1_normal.py --wait-for-capture   # pause so you can start Wireshark

Writes results/s1_queries.csv (every query) and results/s1_summary.csv.
"""
import argparse
import os
import shutil
import statistics

import common as c

DOMAINS = [f"d{i:02d}.lab.test" for i in range(1, 21)]
REPS = 5
TTL = 300
FIELDS = ["config", "run", "seq", "name", "kind", "latency_ms", "rcode",
          "id_ok", "addr_ok", "ttl_ok", "ttl"]

# Each domain has a different address on the primary and on the backup, so a
# wrong or stale answer would be visible.
PRIMARY = {d: {"A": [f"10.1.0.{i}"], "ttl": TTL} for i, d in enumerate(DOMAINS, 1)}
BACKUP = {d: {"A": [f"10.1.1.{i}"], "ttl": TTL} for i, d in enumerate(DOMAINS, 1)}


def one_run(config, run, wd, out_dir):
    """Run the 100-query set once. Returns (query rows, summary row)."""
    target = c.PRIMARY_PORT if config == "B1_direct" else c.FWD_PORT
    fwd = None
    log_file = os.path.join(wd, f"{config}_run{run}.log")
    if config != "B1_direct":
        fwd = c.start_forwarder(wd, log_file, no_cache=(config == "B2_nocache"))
    before = c.server_queries(c.PRIMARY_PORT)
    rows, seq = [], 0
    try:
        for _ in range(REPS):
            for i, name in enumerate(DOMAINS, 1):
                seq += 1
                r = c.query(target, name)
                ttl = r["ttl"]
                rows.append(dict(
                    config=config, run=run, seq=seq, name=name, kind="",
                    latency_ms=round(r["latency_ms"], 3), rcode=r["rcode"],
                    id_ok=r["id_ok"], addr_ok=r["answers"] == [f"10.1.0.{i}"],
                    ttl_ok=ttl is not None and 1 <= ttl <= TTL, ttl=ttl))
        if fwd:  # the forwarder logs a query just after replying: let the log catch up
            c.wait_until(lambda: len(c.read_results(log_file)) >= len(rows), 5)
    finally:
        c.stop(fwd)
    upstream = c.server_queries(c.PRIMARY_PORT) - before

    results = c.read_results(log_file) if fwd else []
    if len(results) == len(rows):
        for row, res in zip(rows, results):
            row["kind"] = res
    hits = results.count("HIT")
    lat = [r["latency_ms"] for r in rows]
    d = c.describe(lat)
    return rows, dict(
        config=config, run=run, queries=len(rows),
        hits=hits, misses=results.count("MISS"),
        hit_ratio=round(hits / len(rows), 4) if fwd else "",
        mean_ms=round(d["mean"], 3), median_ms=round(d["median"], 3),
        p95_ms=round(d["p95"], 3), upstream_queries=upstream,
        errors=sum(1 for r in rows if r["rcode"] != "NOERROR"),
        wrong=sum(1 for r in rows if not (r["id_ok"] and r["addr_ok"] and r["ttl_ok"])))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--runs", type=int, default=10)
    ap.add_argument("--out", default=os.path.join(c.ROOT, "results"))
    ap.add_argument("--upstream-delay", type=int, default=0,
                    help="artificial upstream delay in ms (S2 varies this; try 20 here)")
    ap.add_argument("--wait-for-capture", action="store_true")
    a = ap.parse_args()

    wd = c.workdir()
    primary = c.start_test_server(c.PRIMARY_PORT, PRIMARY, wd, a.upstream_delay)
    backup = c.start_test_server(c.BACKUP_PORT, BACKUP, wd, a.upstream_delay)
    all_rows, summaries = [], []
    try:
        c.wait_for_capture(a.wait_for_capture)
        for config in ("B1_direct", "B2_nocache", "FULL_cache"):
            for run in range(1, a.runs + 1):
                rows, s = one_run(config, run, wd, a.out)
                all_rows += rows
                summaries.append(s)
                print(f"{config:11} run {run:2}: hit_ratio={s['hit_ratio']!s:6} "
                      f"mean={s['mean_ms']:7.2f} ms  p95={s['p95_ms']:7.2f} ms  "
                      f"upstream={s['upstream_queries']:3}  errors={s['errors']}  wrong={s['wrong']}")
    finally:
        c.stop(primary)
        c.stop(backup)

    c.write_csv(os.path.join(a.out, "s1_queries.csv"), all_rows, FIELDS)
    c.write_csv(os.path.join(a.out, "s1_runs.csv"), summaries, list(summaries[0]))

    pooled = []
    print("\n=== S1 pooled over all runs ===")
    print(f"{'config':11} {'queries':>7} {'hit%':>6} {'mean':>8} {'median':>8} {'p95':>8} "
          f"{'hit mean':>9} {'miss mean':>10} {'upstream/run':>13} {'wrong':>6}")
    for config in ("B1_direct", "B2_nocache", "FULL_cache"):
        rows = [r for r in all_rows if r["config"] == config]
        runs = [s for s in summaries if s["config"] == config]
        d = c.describe([r["latency_ms"] for r in rows])
        hit = [r["latency_ms"] for r in rows if r["kind"] == "HIT"]
        miss = [r["latency_ms"] for r in rows if r["kind"] == "MISS"]
        ratio = (sum(s["hits"] for s in runs) / len(rows)) if config != "B1_direct" else None
        p = dict(config=config, queries=len(rows),
                 hit_ratio=round(ratio, 4) if ratio is not None else "",
                 mean_ms=round(d["mean"], 3), median_ms=round(d["median"], 3),
                 p95_ms=round(d["p95"], 3),
                 hit_mean_ms=round(statistics.fmean(hit), 3) if hit else "",
                 miss_mean_ms=round(statistics.fmean(miss), 3) if miss else "",
                 upstream_per_run=round(statistics.fmean(s["upstream_queries"] for s in runs), 1),
                 wrong=sum(s["wrong"] for s in runs))
        pooled.append(p)
        print(f"{config:11} {p['queries']:7} {str(round(ratio*100,1)) + '%' if ratio is not None else '-':>6} "
              f"{p['mean_ms']:8.2f} {p['median_ms']:8.2f} {p['p95_ms']:8.2f} "
              f"{p['hit_mean_ms']!s:>9} {p['miss_mean_ms']!s:>10} {p['upstream_per_run']:13} {p['wrong']:6}")
    c.write_csv(os.path.join(a.out, "s1_summary.csv"), pooled, list(pooled[0]))
    print(f"\nCSV files written to {a.out}")


if __name__ == "__main__":
    main()
