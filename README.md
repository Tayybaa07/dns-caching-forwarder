# DNS Caching Forwarder with Upstream Failover

A multi-threaded UDP DNS caching forwarder written in Python using `dnslib`. It answers repeated A/AAAA queries from an in-memory TTL cache, rewrites 16-bit Query-IDs to prevent spoofing, strictly validates upstream replies, and automatically fails over to a backup upstream when the primary goes down.

**Course:** EE 347: Computer Networks — NUST SEECS (Fall 2026)  
**Group:** 8

---

## Team & Task Division

| Member | Reg ID | Assigned Modules |
|---|---|---|
| **Tayyba Bashir** | `541682` | UDP listener & worker pool (`listener.py`), pending query table & Query-ID rewriter (`pending_table.py`), upstream response validation, timeout/retry logic, structured logger/CLI stats, and concurrency testing (S4, S5). |
| **Zainab Parveen** | `550827` | DNS packet parser & reply builder (`builder.py`), TTL cache (`cache.py`), upstream selector & recovery prober (`selector.py`, `probe.py`), controllable mock DNS servers (`testserver/`), and experiments S1 to S3 with their graphs. |

---

## Features

- **Concurrent UDP Server:** Worker thread pool handles client datagrams on port 5300 without blocking.
- **TTL-Aware Caching:** Caches A and AAAA answers, serving subsequent hits with accurate remaining TTLs.
- **Query-ID Rewriting:** Maps queries to random 16-bit upstream IDs with collision checks to protect against spoofing.
- **Strict Response Matching:** Drops stray or mismatched packets (checks Query-ID, source IP:port, and question section).
- **Automatic Failover:** Switches to backup upstream after 3 consecutive primary timeouts (1.0 s timeout with 1 retry).
- **Health Probing & Recovery:** Periodically probes a downed primary server every 5 seconds and restores it after 2 successful replies.
- **Unsupported Type Handling:** Returns `NOTIMP` for non-A/AAAA queries (such as MX or TXT) and strips EDNS OPT records.
- **Live CLI Metrics:** Tracks total queries, hits, misses, latency percentiles, and per-upstream statistics.
- **Controllable Test Servers:** Mock upstream DNS servers whose records, TTLs, delay and packet loss can be changed, or which can be switched off, while they run.

---

## How It Works

```text
                        ┌────────────────────── DNS caching forwarder (port 5300) ──────────────────────┐
  client ── query ────► │ listener ─► builder.parse_query ─► cache.get ──hit──► builder.hit_reply ──────┼──► client
                        │                                       │ miss                                  │
                        │                         pending table: new random upstream ID                 │
                        │                                       │                                       │
                        │                           selector.pick() ──► primary (5301) / backup (5302)  │
                        │                                       │                                       │
                        │   reply ─► match ID + source + question ─► cache.put (NOERROR only) ──────────┼──► client
                        └───────────────────────────────────────────────────────────────────────────────┘
```

1. **Parse.** `builder.parse_query` rejects malformed packets (dropped without a reply). Types other than A and AAAA get `NOTIMP`.
2. **Cache.** On a hit, the cached records are returned with the *remaining* TTL and the client's own query ID. An entry lives for the smallest TTL among its records, and records with TTL 0 are not cached.
3. **Forward.** On a miss, the query gets a new random upstream ID, is stored in the pending table, and is sent to the upstream chosen by the selector (the primary, unless it is DOWN).
4. **Match.** A reply is accepted only if its ID, source address and question section match the pending query. It is cached (NOERROR answers only), the client's original ID is restored, and it is sent to the client.
5. **Timeout and failover.** If no reply arrives within 1 s, the timeout is reported to the selector and the query is retried once on the other upstream. If that also times out, the client gets `SERVFAIL`. After 3 consecutive timeouts the primary is marked DOWN and all queries go to the backup.
6. **Recovery.** While the primary is DOWN, a background prober sends `probe.lab.test` to it every 5 s. After 2 consecutive successful replies it is marked UP again. Every UP/DOWN change is logged with a timestamp.

---

## Project Structure

```text
dns-caching-forwarder/
├── src/
│   ├── builder.py            # DNS packet parsing and reply building
│   ├── cache.py              # In-memory TTL cache with remaining lifetime
│   ├── listener.py           # UDP server loop, worker pool, and CLI metrics
│   ├── pending_table.py      # Thread-safe pending query table and ID allocator
│   ├── probe.py              # Recovery probe sender
│   └── selector.py           # Upstream health tracker and failover state machine
├── testserver/
│   ├── test_dns_server.py    # Controllable mock DNS server
│   ├── ctl.py                # Runtime control client (stop, start, delay, loss, set)
│   ├── ask.py                # Sends one query, prints status, TTL and time (dig replacement)
│   ├── records_primary.json  # Primary test records (www.lab.test -> 10.0.0.1)
│   └── records_backup.json   # Backup test records (www.lab.test -> 10.0.1.1)
├── tests/
│   ├── conftest.py           # Test configuration and path setup
│   ├── test_builder.py       # Builder unit tests
│   ├── test_cache.py         # Cache unit tests
│   ├── test_listener.py      # Listener and matching unit tests
│   ├── test_probe.py         # Prober unit tests
│   ├── test_selector.py      # Upstream selector unit tests
│   ├── test_testserver.py    # Test server unit tests
│   └── benchmark_s5.py       # Multi-threaded concurrency benchmark
├── experiments/
│   ├── common.py             # Shared helpers (start servers/forwarder, queries, statistics)
│   ├── s1_normal.py          # S1: cache on vs off (B1, B2, full forwarder)
│   ├── s2_conditions.py      # S2: upstream delay, packet loss and TTL
│   ├── s3_record_change.py   # S3: record change while the TTL is running
│   ├── s4_failover.py        # S4: failover and recovery
│   ├── s5_concurrency.py     # S5: concurrent clients
│   └── make_graphs.py        # Graphs for S1 to S3, made from the CSV files
└── results/                  # CSV results and graphs for every experiment
```

---

## Setup & Running

The project is pure Python (3.10 or newer) and runs on Linux and Windows.

### 1. Environment Setup

```bash
# Clone the repository
git clone https://github.com/Tayybaa07/dns-caching-forwarder.git
cd dns-caching-forwarder

# Create and activate virtual environment
python3 -m venv .venv
source .venv/bin/activate          # Windows PowerShell: .venv\Scripts\Activate.ps1

# Install dependencies and run tests
pip install dnslib pytest
pytest tests/
```

On Windows, use `python` instead of `python3` in every command below. Graphs additionally need `pip install matplotlib`.

### 2. Start the Servers

Run each command in a separate terminal:

**Terminal 1 — Primary Upstream (Port 5301, Control 6301):**
```bash
python3 testserver/test_dns_server.py --port 5301 --control-port 6301 --records testserver/records_primary.json
```

**Terminal 2 — Backup Upstream (Port 5302, Control 6302):**
```bash
python3 testserver/test_dns_server.py --port 5302 --control-port 6302 --records testserver/records_backup.json
```

**Terminal 3 — DNS Caching Forwarder (Port 5300):**
```bash
python3 src/listener.py --port 5300 --primary 127.0.0.1:5301 --backup 127.0.0.1:5302
```

> The primary must answer `probe.lab.test` (it does in `records_primary.json`), because the recovery prober uses that name.

---

### 3. Client Verification (Terminal 4)

Use `dig` on Linux, or `testserver/ask.py` on any system (for example `python3 testserver/ask.py www.lab.test`). Windows `nslookup` is unreliable with a custom port, so use `ask.py` there.

**A. Cache Miss vs. Cache Hit (TTL Countdown):**
```bash
# First query: MISS -> forwarded upstream, cached with TTL 30
dig @127.0.0.1 -p 5300 www.lab.test A       # or: python3 testserver/ask.py www.lab.test

# Immediate second query: HIT -> served instantly from cache, decremented TTL
dig @127.0.0.1 -p 5300 www.lab.test A
```

**B. Unsupported Query Type (NOTIMP):**
```bash
dig @127.0.0.1 -p 5300 www.lab.test MX      # or: python3 testserver/ask.py www.lab.test MX
```

**C. Failover & Recovery:**
```bash
# 1. Stop primary server
python3 testserver/ctl.py 6301 stop

# 2. Trigger 3 timeouts -> forwarder marks primary DOWN and switches to backup
dig @127.0.0.1 -p 5300 fail1.lab.test A +tries=1 +time=1
dig @127.0.0.1 -p 5300 fail2.lab.test A +tries=1 +time=1
dig @127.0.0.1 -p 5300 fail3.lab.test A +tries=1 +time=1

# 3. Query resolved by backup (returns 10.0.1.2)
dig @127.0.0.1 -p 5300 app.lab.test A

# 4. Restart primary -> background prober restores primary after 2 successful probes
python3 testserver/ctl.py 6301 start
sleep 10
dig @127.0.0.1 -p 5300 www.lab.test A
```

**D. Concurrency Benchmark:**
```bash
python3 tests/benchmark_s5.py --port 5300 --queries-per-level 50
```

Stopping the forwarder (`Ctrl+C`) in Terminal 3 automatically prints a summary table of hits, misses, hit ratio %, and latency statistics.

---

## Command-Line Reference

### Forwarder (`src/listener.py`)

| Option | Default | Meaning |
|---|---|---|
| `--port PORT` | `5300` | UDP port for client queries |
| `--primary HOST:PORT` | `127.0.0.1:5301` | Primary upstream |
| `--backup HOST:PORT` | `127.0.0.1:5302` | Backup upstream |
| `--upstream HOST:PORT` | none | Alias that overrides the primary |
| `--workers N` | `4` | Worker threads |
| `--timeout SECS` | `1.0` | Time before a query is retried on the other upstream |
| `--reaper-interval SECS` | `0.2` | How often timed-out queries are swept |
| `--no-cache` | off | Disable the cache (baseline B2) |
| `--stats-interval SECS` | `0` (off) | Print live statistics every N seconds |
| `--log-file PATH` | none | Write one structured line per query to a file |
| `--log-level LEVEL` | `INFO` | `DEBUG`, `INFO`, `WARNING` or `ERROR` |

### Test servers (`testserver/ctl.py`)

Each test server has a control port, which is its DNS port plus 1000 unless `--control-port` is given. `test_dns_server.py` also takes `--port`, `--records` and `--delay` (milliseconds) at start-up.

```bash
python3 testserver/ctl.py 6301 delay 100                       # add 100 ms to every answer
python3 testserver/ctl.py 6301 loss 5                          # drop 5% of incoming queries
python3 testserver/ctl.py 6301 stop                            # stop answering (looks like a dead server)
python3 testserver/ctl.py 6301 start                           # answer again
python3 testserver/ctl.py 6301 set www.lab.test A 10.0.0.5 10  # change a record (name, type, address, TTL)
python3 testserver/ctl.py 6301 ttl www.lab.test 5              # change only the TTL
python3 testserver/ctl.py 6301 del www.lab.test                # remove a record
python3 testserver/ctl.py 6301 status                          # show state, delay, loss and records
```

---

## Experiments

The experiment scripts start their own test servers and forwarder on ports **5400** (forwarder), **5401** (primary) and **5402** (backup), so they do not clash with the manual setup above. In S1 to S3 every run starts with a cold cache (the forwarder is restarted for each run). Results are written as CSV files to `results/`. The plan asks for at least 10 runs per test.

| Test | What it measures | Command | Results folder |
|---|---|---|---|
| **S1** | Normal operation: direct upstream (B1), forwarder without cache (B2) and full forwarder, with and without upstream delay | `python3 experiments/s1_normal.py --out results/s1_delay0`<br>`python3 experiments/s1_normal.py --upstream-delay 20 --out results/s1_delay20` | `results/s1_delay0`, `results/s1_delay20` |
| **S2** | Upstream delay (0, 50, 100, 200 ms), packet loss (0, 5, 10 % at 50 ms) and TTL (5, 30, 300 s) | `python3 experiments/s2_conditions.py --part delay --runs 10`<br>`python3 experiments/s2_conditions.py --part loss --runs 10`<br>`python3 experiments/s2_conditions.py --part ttl --runs 10` | `results/s2_conditions` |
| **S3** | A record changes upstream while its TTL is still running | `python3 experiments/s3_record_change.py --runs 10` | `results/s3_record_change` |
| **S4** | Primary failure and recovery: detection, failover and recovery times, failed queries | `python3 experiments/s4_failover.py --runs 10` | `results/s4_failover` |
| **S5** | 1, 5, 10 and 20 concurrent clients, with zero dropped or mismatched replies | `python3 experiments/s5_concurrency.py --runs 10` | `results/s5_concurrency` |

`s2_conditions.py` also takes `--ttl-duration` (seconds per TTL run, default 40), and every script takes `--out` and `--runs`. Add `--wait-for-capture` to S1, S2 or S3 to pause before the run so a Wireshark capture can be started first.

### Graphs

```bash
pip install matplotlib
python3 experiments/make_graphs.py
```

This writes three charts to `results/graphs/` from the CSV files, with error bars showing the spread over the runs: `s1_mean_response_time.png`, `s2_miss_time_vs_delay.png` and `s2_hit_ratio_vs_ttl.png`.

### Delay and packet loss

Upstream delay and packet loss are emulated by the test servers (`ctl.py ... delay` and `ctl.py ... loss`), which is the fallback named in the project plan, and not by Linux `tc netem`. The delay is added before the server answers, and loss drops a query on arrival, so the forwarder sees a late answer or a timeout.

### Wireshark captures

The experiments were captured on the loopback interface. Wireshark only decodes DNS on port 53 by default, so use **Analyze → Decode As...** and set UDP ports 5300 to 5302 (manual setup) and 5400 to 5402 (experiments) to DNS. The capture files (`.pcapng`) and screenshots are submitted with the report and are not stored in this repository.

---

## Results Summary

All runs are on one machine (localhost). Times are in milliseconds unless stated.

**S1: mean response time (10 runs of 100 queries each)**

| Setup | 0 ms upstream delay | 20 ms upstream delay | Queries reaching the primary (per 100) |
|---|---|---|---|
| B1: direct to the primary | 1.64 | 22.56 | 100 |
| B2: forwarder, no cache | 3.90 | 23.26 | 100 |
| Full forwarder with cache | 2.67 | 7.15 | 20 |

The cache hit ratio is 80% (20 domains, 5 repetitions). With a 20 ms delay, cache hits average 2.9 ms against 24.1 ms for misses.

**S2: delay (10 runs per point).** Hit time stays flat while miss time follows the delay.

| Upstream delay | 0 ms | 50 ms | 100 ms | 200 ms |
|---|---|---|---|---|
| Hit time | 1.28 | 1.25 | 1.40 | 1.28 |
| Miss time | 1.78 | 52.32 | 102.37 | 202.09 |

**S2: packet loss at 50 ms (10 runs of 100 queries per level).** Every answer that arrived was correct.

| Packet loss | Queries retried on the backup | Failed (both tries lost) | Wrong answers |
|---|---|---|---|
| 0 % | 0 | 0 | 0 |
| 5 % | 47 of 1000 | 2 | 0 |
| 10 % | 92 of 1000 | 8 | 0 |

**S2: TTL (10 runs per point).** Cache-hit ratio is 65 % for a 5 s TTL, 90 % for 30 s and 95 % for 300 s.

**S3: record change (10 runs, TTL 10 s).** All 10 runs passed. The new address appeared after 10.07 s on average (limit: TTL + 1 s), no stale answer was served after the TTL, and the primary was asked exactly twice per run.

**S4: failover and recovery (5 runs).** Mean detection time 3.37 s, failover time 1.08 s and recovery time 9.05 s, with 0 failed queries.

**S5: concurrency (5 runs of 50 queries per level).** No query was dropped or mismatched at any level. Mean latency was 0.32, 1.02, 3.51 and 6.52 ms for 1, 5, 10 and 20 clients.

---

## Known Limitations

- UDP only: there is no TCP fallback or truncation handling.
- Only A and AAAA queries (class IN) are answered. Other types get `NOTIMP`.
- No negative caching (NXDOMAIN and empty answers are not cached), and no DNSSEC.
- Delay and loss are emulated by the test servers, not by `tc netem`.
- All measurements were taken on a single machine over the loopback interface, so absolute times are small and differ between computers. The trends (hit time flat, miss time following the delay) are what matter.