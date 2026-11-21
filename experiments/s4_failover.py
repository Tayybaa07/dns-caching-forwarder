"""S4 - Primary failure and recovery (compulsory features C8, C9).

Measures:
  * Detection time (from primary stop to DOWN event in log)
  * Failover time (from primary stop to first reply from backup)
  * Failed queries during the switch
  * Recovery time (from primary restart to traffic returning to primary)

Writes results to:
  results/s4_failover/s4_queries.csv
  results/s4_failover/s4_runs.csv
  results/s4_failover/s4_summary.csv
"""
import argparse
import os
import re
import statistics
import time

import common as c

TTL = 2

# Primary returns 10.1.0.x; Backup returns 10.1.1.x so we know which answered
PRIMARY = {
    f"d{i:02d}.lab.test": {"A": [f"10.1.0.{i}"], "ttl": TTL} for i in range(1, 21)
}
PRIMARY["probe.lab.test"] = {"A": ["10.1.0.99"], "ttl": 1}

BACKUP = {
    f"d{i:02d}.lab.test": {"A": [f"10.1.1.{i}"], "ttl": TTL} for i in range(1, 21)
}
BACKUP["probe.lab.test"] = {"A": ["10.1.1.99"], "ttl": 1}

DOWN_RE = re.compile(r"UPSTREAM .*?5401.*? is now DOWN")
UP_RE = re.compile(r"UPSTREAM .*?5401.*? is now UP")


def run_s4_once(run, wd, out_dir):
    """Execute a single failure and recovery cycle."""
    log_file = os.path.join(wd, f"s4_fwd_run{run}.log")
    fwd_stdout = os.path.join(wd, "forwarder_stdout.log")
    # Timeout 1.0s, reaper 0.1s so timeouts trigger promptly
    fwd = c.start_forwarder(wd, log_file, extra=["--timeout", "1.0", "--reaper-interval", "0.1"])

    queries_log = []
    t_start_run = time.time()
    seq = 0

    def send_query(name, phase):
        nonlocal seq
        seq += 1
        t_send = time.time()
        r = c.query(c.FWD_PORT, name, timeout=4.0)
        rel_time = round(t_send - t_start_run, 3)
        ans = r["answers"][0] if r["answers"] else ""
        if ans.startswith("10.1.0."):
            upstream = "PRIMARY"
        elif ans.startswith("10.1.1."):
            upstream = "BACKUP"
        else:
            upstream = "NONE"
        row = dict(
            run=run,
            seq=seq,
            phase=phase,
            rel_time_s=rel_time,
            name=name,
            upstream=upstream,
            answer=ans,
            latency_ms=round(r["latency_ms"], 2),
            rcode=r["rcode"],
            ok=r["ok"],
        )
        queries_log.append(row)
        return row

    try:
        # Phase 1: Steady state on Primary
        for i in range(1, 6):
            send_query(f"d{i:02d}.lab.test", "1_NORMAL_PRIMARY")
            time.sleep(0.1)

        # Phase 2: Failure of Primary
        t_stop = time.time()
        c.ctl(c.PRIMARY_PORT, "stop")

        t_first_backup = None
        t_down_event = None
        failed_during_switch = 0

        # Send queries to trigger the 3-timeout failover rule
        # Since each query has TTL=2, use distinct names to ensure they go upstream
        for i in range(6, 15):
            res = send_query(f"d{i:02d}.lab.test", "2_SWITCHING")
            if not res["ok"]:
                failed_during_switch += 1
            if res["upstream"] == "BACKUP" and t_first_backup is None:
                t_first_backup = time.time()
            # Check if DOWN was logged
            if t_down_event is None and os.path.exists(fwd_stdout):
                with open(fwd_stdout) as f:
                    if DOWN_RE.search(f.read()):
                        t_down_event = time.time()
            time.sleep(0.1)

        failover_time = round(t_first_backup - t_stop, 3) if t_first_backup else float("nan")
        detection_time = round(t_down_event - t_stop, 3) if t_down_event else failover_time

        # Phase 3: Steady state on Backup
        for i in range(15, 21):
            send_query(f"d{i:02d}.lab.test", "3_BACKUP_SERVING")
            time.sleep(0.1)

        # Phase 4: Recovery of Primary
        t_restore = time.time()
        c.ctl(c.PRIMARY_PORT, "start")

        t_recovered = None
        # Prober checks every 5s; needs 2 successful probes (~10s)
        # Poll until primary answers again or timeout
        poll_seq = 1
        t_poll_end = time.time() + 18.0
        while time.time() < t_poll_end:
            # Alternate queries every 0.8s
            res = send_query(f"d{poll_seq % 20 + 1:02d}.lab.test", "4_RECOVERING")
            if res["upstream"] == "PRIMARY":
                t_recovered = time.time()
                break
            poll_seq += 1
            time.sleep(0.8)

        recovery_time = round(t_recovered - t_restore, 3) if t_recovered else float("nan")

        # Phase 5: Confirm restored state
        for i in range(1, 4):
            send_query(f"d{i:02d}.lab.test", "5_RESTORED_PRIMARY")
            time.sleep(0.1)

    finally:
        c.stop(fwd)
        # Ensure primary is running again for next run
        try:
            c.ctl(c.PRIMARY_PORT, "start")
        except Exception:
            pass

    summary = dict(
        run=run,
        total_queries=len(queries_log),
        detection_time_s=detection_time,
        failover_time_s=failover_time,
        recovery_time_s=recovery_time,
        failed_queries=failed_during_switch,
    )
    return queries_log, summary


def main():
    parser = argparse.ArgumentParser(description="Experiment S4: Primary Failure and Recovery")
    parser.add_argument("--runs", type=int, default=5, help="Number of repetitions (default: 5)")
    parser.add_argument(
        "--out",
        default=os.path.join(c.ROOT, "results", "s4_failover"),
        help="Output directory (default: results/s4_failover)",
    )
    args = parser.parse_args()

    wd = c.workdir()
    os.makedirs(args.out, exist_ok=True)

    print("=" * 72)
    print("      EXPERIMENT S4: PRIMARY FAILURE & RECOVERY")
    print(f" Runs: {args.runs} | Output Directory: {args.out}")
    print("=" * 72)

    primary = c.start_test_server(c.PRIMARY_PORT, PRIMARY, wd)
    backup = c.start_test_server(c.BACKUP_PORT, BACKUP, wd)

    all_queries = []
    run_summaries = []

    try:
        for r in range(1, args.runs + 1):
            print(f"[*] Starting S4 Run {r}/{args.runs}...", flush=True)
            queries, s = run_s4_once(r, wd, args.out)
            all_queries.extend(queries)
            run_summaries.append(s)
            print(
                f"    Run {r}: Failover = {s['failover_time_s']:.2f}s | "
                f"Recovery = {s['recovery_time_s']:.2f}s | "
                f"Failed Queries = {s['failed_queries']}",
                flush=True,
            )
            time.sleep(1.0)
    finally:
        c.stop(primary)
        c.stop(backup)

    # Write CSV files
    query_fields = [
        "run",
        "seq",
        "phase",
        "rel_time_s",
        "name",
        "upstream",
        "answer",
        "latency_ms",
        "rcode",
        "ok",
    ]
    run_fields = [
        "run",
        "total_queries",
        "detection_time_s",
        "failover_time_s",
        "recovery_time_s",
        "failed_queries",
    ]

    c.write_csv(os.path.join(args.out, "s4_queries.csv"), all_queries, query_fields)
    c.write_csv(os.path.join(args.out, "s4_runs.csv"), run_summaries, run_fields)

    # Compute pooled summary
    det_times = [s["detection_time_s"] for s in run_summaries if not c.math.isnan(s["detection_time_s"])]
    fo_times = [s["failover_time_s"] for s in run_summaries if not c.math.isnan(s["failover_time_s"])]
    rec_times = [s["recovery_time_s"] for s in run_summaries if not c.math.isnan(s["recovery_time_s"])]
    fails = [s["failed_queries"] for s in run_summaries]

    summary_rows = []
    for metric_name, vals in [
        ("detection_time_s", det_times),
        ("failover_time_s", fo_times),
        ("recovery_time_s", rec_times),
        ("failed_queries", fails),
    ]:
        if vals:
            d = c.describe(vals)
            summary_rows.append(
                dict(
                    metric=metric_name,
                    mean=round(d["mean"], 3),
                    median=round(d["median"], 3),
                    p95=round(d["p95"], 3),
                    min=round(min(vals), 3),
                    max=round(max(vals), 3),
                )
            )

    c.write_csv(
        os.path.join(args.out, "s4_summary.csv"),
        summary_rows,
        ["metric", "mean", "median", "p95", "min", "max"],
    )

    print("\n" + "=" * 72)
    print("                 EXPERIMENT S4 SUMMARY TABLE")
    print("=" * 72)
    print(f"{'Metric':<20} {'Mean':>10} {'Median':>10} {'p95':>10} {'Min':>8} {'Max':>8}")
    print("-" * 72)
    for r in summary_rows:
        print(
            f"{r['metric']:<20} {r['mean']:>10.2f} {r['median']:>10.2f} {r['p95']:>10.2f} "
            f"{r['min']:>8.2f} {r['max']:>8.2f}"
        )
    print("=" * 72)
    print(f"\nCSV files saved in {args.out}/\n")


if __name__ == "__main__":
    main()
