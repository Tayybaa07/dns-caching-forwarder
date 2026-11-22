"""S2 - Changed network conditions and TTL.

Three parts (run one with --part, or all of them):
  delay  upstream delay 0, 50, 100, 200 ms -> miss time grows, hit time stays flat
  loss   packet loss 0, 5, 10 % at 50 ms   -> retries, but correct answers
  ttl    TTL 5, 30, 300 s                  -> shorter TTL lowers the hit ratio

Delay and loss are applied by the test servers (the fallback in the proposal's risk table),
not by netem. Every run starts with a cold cache (the forwarder is restarted).

    python experiments/s2_conditions.py --part delay
    python experiments/s2_conditions.py --part loss
    python experiments/s2_conditions.py --part ttl
    python experiments/s2_conditions.py                # all three, about 10 minutes

Results go to results/s2_conditions/.
"""
import argparse
import os
import statistics
import time

import common as c

DELAYS = [0, 50, 100, 200]
LOSSES = [0, 5, 10]
TTLS = [5, 30, 300]
DOMAINS20 = [f"d{i:02d}.lab.test" for i in range(1, 21)]
DOMAINS100 = [f"n{i:03d}.lab.test" for i in range(1, 101)]


def records(prefix, names, ttl, probe=False):
    rec = {n: {"A": [f"{prefix}.{i}"], "ttl": ttl} for i, n in enumerate(names, 1)}
    if probe:
        rec["probe.lab.test"] = {"A": [f"{prefix}.250"], "ttl": 5}   # recovery probe
    return rec


def correct(row, expected):
    return row["rcode"] == "NOERROR" and row["answers"] == [expected]


def fmt(x, nd=2):
    return "-" if x == "" else f"{x:.{nd}f}"


def start_servers(names, ttl, wd, delay=0, loss=0):
    primary = c.start_test_server(c.PRIMARY_PORT, records("10.3.0", names, ttl, True), wd, delay)
    backup = c.start_test_server(c.BACKUP_PORT, records("10.3.1", names, ttl), wd, delay)
    if loss:
        c.ctl(c.PRIMARY_PORT, f"loss {loss}")
        c.ctl(c.BACKUP_PORT, f"loss {loss}")
    return primary, backup


def count_lines(stdout_file, text):
    with open(stdout_file) as f:
        return sum(1 for ln in f if text in ln)


def classify(results, rows):
    """Attach HIT/MISS (from the forwarder log) to the query rows, in order."""
    if len(results) == len(rows):
        for row, res in zip(rows, results):
            row["kind"] = res
    return rows


# ------------------------------------------------------------------ delay
def part_delay(runs, out, wd):
    run_rows, summary = [], []
    for delay in DELAYS:
        primary, backup = start_servers(DOMAINS20, 300, wd, delay)
        hit_all, miss_all, ratios = [], [], []
        try:
            for run in range(1, runs + 1):
                log = os.path.join(wd, f"delay{delay}_run{run}.log")
                fwd = c.start_forwarder(wd, log)
                rows = []
                try:
                    for _ in range(5):                      # 20 domains x 5 = 100 queries
                        for i, name in enumerate(DOMAINS20, 1):
                            r = c.query(c.FWD_PORT, name)
                            r["kind"] = ""
                            r["wrong"] = not correct(r, f"10.3.0.{i}")
                            rows.append(r)
                    c.wait_until(lambda: len(c.read_results(log)) >= len(rows), 5)
                finally:
                    c.stop(fwd)
                classify(c.read_results(log), rows)
                hit = [r["latency_ms"] for r in rows if r["kind"] == "HIT"]
                miss = [r["latency_ms"] for r in rows if r["kind"] == "MISS"]
                hit_all += hit
                miss_all += miss
                ratios.append(len(hit) / len(rows))
                run_rows.append(dict(delay_ms=delay, run=run, queries=len(rows),
                                     hit_ratio=round(len(hit) / len(rows), 3),
                                     hit_mean_ms=round(statistics.fmean(hit), 3) if hit else "",
                                     miss_mean_ms=round(statistics.fmean(miss), 3) if miss else "",
                                     wrong=sum(r["wrong"] for r in rows)))
        finally:
            c.stop(primary)
            c.stop(backup)
        h, m = c.describe(hit_all), c.describe(miss_all)
        summary.append(dict(delay_ms=delay, queries=runs * 100,
                            hit_ratio=round(statistics.fmean(ratios), 3),
                            hit_mean_ms=round(h["mean"], 3), hit_p95_ms=round(h["p95"], 3),
                            miss_mean_ms=round(m["mean"], 3), miss_p95_ms=round(m["p95"], 3),
                            miss_over_hit=round(m["mean"] / h["mean"], 1),
                            wrong=sum(r["wrong"] for r in run_rows if r["delay_ms"] == delay)))
        print(f"delay {delay:3} ms: hit ratio {summary[-1]['hit_ratio']:.2f}  "
              f"hit {h['mean']:.2f} ms  miss {m['mean']:.2f} ms  "
              f"(miss is {summary[-1]['miss_over_hit']}x slower)", flush=True)
    c.write_csv(os.path.join(out, "s2_delay_runs.csv"), run_rows, list(run_rows[0]))
    c.write_csv(os.path.join(out, "s2_delay_summary.csv"), summary, list(summary[0]))
    miss = [s["miss_mean_ms"] for s in summary]
    hit = [s["hit_mean_ms"] for s in summary]
    print("\nCheck: miss time grows with the delay:", "YES" if miss == sorted(miss) else "NO")
    print(f"Check: hit time stays flat (spread {max(hit) - min(hit):.2f} ms):",
          "YES" if max(hit) - min(hit) < 5 else "NO")
    print("Check: hits at least 5x faster than misses at 50 ms or more:",
          "YES" if all(s["miss_over_hit"] >= 5 for s in summary if s["delay_ms"] >= 50) else "NO")


# ------------------------------------------------------------------- loss
def part_loss(runs, out, wd):
    run_rows, summary = [], []
    for loss in LOSSES:
        primary, backup = start_servers(DOMAINS100, 300, wd, delay=50, loss=loss)
        all_rows = []
        try:
            for run in range(1, runs + 1):
                log = os.path.join(wd, f"loss{loss}_run{run}.log")
                fwd = c.start_forwarder(wd, log)
                stdout = os.path.join(wd, "forwarder_stdout.log")
                rows = []
                try:
                    for i, name in enumerate(DOMAINS100, 1):    # 100 different names = 100 misses
                        r = c.query(c.FWD_PORT, name)
                        r["wrong"] = r["rcode"] == "NOERROR" and r["answers"] not in ([f"10.3.0.{i}"], [f"10.3.1.{i}"])
                        rows.append(r)
                finally:
                    c.stop(fwd)
                retries = count_lines(stdout, "Retrying query")
                down = count_lines(stdout, "is now DOWN")
                slow = sum(1 for r in rows if r["latency_ms"] >= 900)
                fails = sum(1 for r in rows if r["rcode"] != "NOERROR")
                all_rows += rows
                run_rows.append(dict(loss_pct=loss, run=run, queries=len(rows), retries=retries,
                                     slow_over_900ms=slow, failed=fails,
                                     wrong_answers=sum(r["wrong"] for r in rows),
                                     primary_marked_down=down))
        finally:
            c.stop(primary)
            c.stop(backup)
        rr = [r for r in run_rows if r["loss_pct"] == loss]
        lat = c.describe([r["latency_ms"] for r in all_rows])
        summary.append(dict(loss_pct=loss, queries=len(all_rows),
                            retries=sum(r["retries"] for r in rr),
                            failed=sum(r["failed"] for r in rr),
                            wrong_answers=sum(r["wrong_answers"] for r in rr),
                            mean_ms=round(lat["mean"], 2), median_ms=round(lat["median"], 2),
                            p95_ms=round(lat["p95"], 2),
                            primary_marked_down=sum(r["primary_marked_down"] for r in rr)))
        s = summary[-1]
        print(f"loss {loss:2}% : {s['queries']} queries, {s['retries']} retried, "
              f"{s['failed']} failed (both tries lost), wrong answers {s['wrong_answers']}, "
              f"median {s['median_ms']} ms, p95 {s['p95_ms']} ms", flush=True)
    c.write_csv(os.path.join(out, "s2_loss_runs.csv"), run_rows, list(run_rows[0]))
    c.write_csv(os.path.join(out, "s2_loss_summary.csv"), summary, list(summary[0]))
    print("\nCheck: more loss causes more retries:",
          "YES" if [s["retries"] for s in summary] == sorted(s["retries"] for s in summary) else "NO")
    print("Check: every answer that arrives is correct:",
          "YES" if all(s["wrong_answers"] == 0 for s in summary) else "NO")


# -------------------------------------------------------------------- ttl
def part_ttl(runs, out, wd, duration):
    run_rows, summary = [], []
    pace = 0.1                                  # 10 queries per second, round robin over 20 names
    for ttl in TTLS:
        primary, backup = start_servers(DOMAINS20, ttl, wd, delay=50)
        ratios = []
        try:
            for run in range(1, runs + 1):
                log = os.path.join(wd, f"ttl{ttl}_run{run}.log")
                fwd = c.start_forwarder(wd, log)
                before = c.server_queries(c.PRIMARY_PORT)
                n, t0 = 0, time.perf_counter()
                try:
                    while time.perf_counter() - t0 < duration:
                        c.query(c.FWD_PORT, DOMAINS20[n % 20])
                        n += 1
                        wait = t0 + n * pace - time.perf_counter()
                        if wait > 0:
                            time.sleep(wait)
                    c.wait_until(lambda: len(c.read_results(log)) >= n, 5)
                finally:
                    c.stop(fwd)
                res = c.read_results(log)
                hits = res.count("HIT")
                ratios.append(hits / n)
                run_rows.append(dict(ttl_s=ttl, run=run, queries=n, hits=hits, misses=res.count("MISS"),
                                     hit_ratio=round(hits / n, 3),
                                     upstream_queries=c.server_queries(c.PRIMARY_PORT) - before))
        finally:
            c.stop(primary)
            c.stop(backup)
        summary.append(dict(ttl_s=ttl, runs=runs, hit_ratio=round(statistics.fmean(ratios), 3),
                            min_ratio=round(min(ratios), 3), max_ratio=round(max(ratios), 3)))
        print(f"TTL {ttl:3} s: hit ratio {summary[-1]['hit_ratio']:.3f} "
              f"(min {summary[-1]['min_ratio']:.3f}, max {summary[-1]['max_ratio']:.3f})", flush=True)
    c.write_csv(os.path.join(out, "s2_ttl_runs.csv"), run_rows, list(run_rows[0]))
    c.write_csv(os.path.join(out, "s2_ttl_summary.csv"), summary, list(summary[0]))
    ratios = [s["hit_ratio"] for s in summary]
    print("\nCheck: a shorter TTL lowers the hit ratio:", "YES" if ratios == sorted(ratios) else "NO")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--part", choices=["delay", "loss", "ttl", "all"], default="all")
    ap.add_argument("--runs", type=int, default=None,
                    help="runs per setting (default: delay 5, loss 5, ttl 3)")
    ap.add_argument("--ttl-duration", type=float, default=40,
                    help="seconds each TTL run lasts (default 40)")
    ap.add_argument("--out", default=os.path.join(c.ROOT, "results", "s2_conditions"))
    ap.add_argument("--wait-for-capture", action="store_true")
    a = ap.parse_args()
    wd = c.workdir()
    c.wait_for_capture(a.wait_for_capture)
    if a.part in ("delay", "all"):
        print("=== S2 delay ===")
        part_delay(a.runs or 5, a.out, wd)
    if a.part in ("loss", "all"):
        print("\n=== S2 packet loss (50 ms upstream delay) ===")
        part_loss(a.runs or 5, a.out, wd)
    if a.part in ("ttl", "all"):
        print("\n=== S2 TTL ===")
        part_ttl(a.runs or 3, a.out, wd, a.ttl_duration)
    print(f"\nCSV files written to {a.out}")


if __name__ == "__main__":
    main()
