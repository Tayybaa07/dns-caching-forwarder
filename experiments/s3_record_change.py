"""S3 - Record change and expiry.

A cached answer must live until its TTL ends. If the record changes upstream while
the TTL is still running, the client keeps getting the OLD address until the TTL
ends, and the NEW address within TTL + 1 s.

Per run (the forwarder is restarted, so the cache is cold):
  t = 0       first query -> cache MISS, old address, cache entry created
  t = 2 s     the record is changed on the primary (new address)
  then        the forwarder is asked every 0.1 s until the new address shows up

Pass if: new address appears no earlier than the TTL and within TTL + 1 s,
no old answer is served after the TTL, and the primary was asked exactly twice
(once to fill the cache, once after expiry).

    python experiments/s3_record_change.py              # 5 runs, TTL 10 s
    python experiments/s3_record_change.py --runs 2 --ttl 5
    python experiments/s3_record_change.py --wait-for-capture
"""
import argparse
import os
import statistics
import time

import common as c

NAME = "chg.lab.test"
OLD, NEW = "10.2.0.1", "10.2.0.99"
CHANGE_AT = 2.0       # seconds after the first query
POLL = 0.1            # seconds between polls
RUN_FIELDS = ["run", "ttl", "first_new_s", "delay_after_change_s", "old_answers",
              "new_answers", "stale_after_ttl", "upstream_queries", "pass"]
Q_FIELDS = ["run", "t_s", "answer", "ttl_reported"]


def one_run(run, ttl, wd):
    log_file = os.path.join(wd, f"s3_run{run}.log")
    fwd = c.start_forwarder(wd, log_file)
    queries = []
    try:
        before = c.server_queries(c.PRIMARY_PORT)
        r = c.query(c.FWD_PORT, NAME)
        t0 = time.perf_counter()                    # cache entry exists from here
        if r["answers"] != [OLD]:
            raise RuntimeError(f"first answer was {r['answers']}, expected {[OLD]}")
        queries.append(dict(run=run, t_s=0.0, answer=OLD, ttl_reported=r["ttl"]))

        changed, first_new, t_change = False, None, None
        deadline = t0 + ttl + 5
        while time.perf_counter() < deadline:
            now = time.perf_counter() - t0
            if not changed and now >= CHANGE_AT:
                c.ctl(c.PRIMARY_PORT, f"set {NAME} A {NEW} {ttl}")
                changed, t_change = True, time.perf_counter() - t0
            r = c.query(c.FWD_PORT, NAME)
            t = time.perf_counter() - t0
            ans = r["answers"][0] if r["answers"] else r["rcode"]
            queries.append(dict(run=run, t_s=round(t, 3), answer=ans, ttl_reported=r["ttl"]))
            if ans == NEW:
                first_new = t
                break
            time.sleep(max(0, POLL - (time.perf_counter() - t0 - t)))
        upstream = c.server_queries(c.PRIMARY_PORT) - before
    finally:
        c.stop(fwd)
        c.ctl(c.PRIMARY_PORT, f"set {NAME} A {OLD} {ttl}")      # restore for the next run

    old_n = sum(1 for q in queries if q["answer"] == OLD)
    new_n = sum(1 for q in queries if q["answer"] == NEW)
    stale = sum(1 for q in queries if q["answer"] == OLD and q["t_s"] > ttl)
    ok = (first_new is not None and ttl <= first_new <= ttl + 1
          and stale == 0 and upstream == 2)
    row = dict(run=run, ttl=ttl,
               first_new_s=round(first_new, 3) if first_new is not None else "",
               delay_after_change_s=round(first_new - t_change, 3)
               if first_new is not None and t_change is not None else "",
               old_answers=old_n, new_answers=new_n, stale_after_ttl=stale,
               upstream_queries=upstream, **{"pass": ok})
    return queries, row


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--runs", type=int, default=5)
    ap.add_argument("--ttl", type=int, default=10)
    ap.add_argument("--out", default=os.path.join(c.ROOT, "results", "s3_record_change"))
    ap.add_argument("--wait-for-capture", action="store_true")
    a = ap.parse_args()

    wd = c.workdir()
    rec = {NAME: {"A": [OLD], "ttl": a.ttl}}
    primary = c.start_test_server(c.PRIMARY_PORT, rec, wd)
    backup = c.start_test_server(c.BACKUP_PORT, {NAME: {"A": ["10.2.1.1"], "ttl": a.ttl}}, wd)
    all_q, rows = [], []
    try:
        c.wait_for_capture(a.wait_for_capture)
        for run in range(1, a.runs + 1):
            q, row = one_run(run, a.ttl, wd)
            all_q += q
            rows.append(row)
            print(f"run {run}: new address after {row['first_new_s']} s "
                  f"({row['delay_after_change_s']} s after the change) | "
                  f"old answers={row['old_answers']} stale={row['stale_after_ttl']} "
                  f"upstream queries={row['upstream_queries']} | "
                  f"{'PASS' if row['pass'] else 'FAIL'}")
    finally:
        c.stop(primary)
        c.stop(backup)

    c.write_csv(os.path.join(a.out, "s3_queries.csv"), all_q, Q_FIELDS)
    c.write_csv(os.path.join(a.out, "s3_runs.csv"), rows, RUN_FIELDS)
    firsts = [r["first_new_s"] for r in rows if r["first_new_s"] != ""]
    print(f"\n=== S3 summary (TTL {a.ttl} s, {len(rows)} runs) ===")
    if firsts:
        print(f"time until the client sees the new address: mean {statistics.fmean(firsts):.2f} s, "
              f"min {min(firsts):.2f} s, max {max(firsts):.2f} s  (limit: TTL + 1 = {a.ttl + 1} s)")
    print(f"stale answers served after the TTL: {sum(r['stale_after_ttl'] for r in rows)}")
    print(f"runs passed: {sum(1 for r in rows if r['pass'])} / {len(rows)}")
    print(f"CSV files written to {a.out}")


if __name__ == "__main__":
    main()
