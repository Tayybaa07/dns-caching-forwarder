# DNS Caching Forwarder with Upstream Failover

A multi-threaded UDP DNS caching forwarder written in Python using `dnslib`. The system intercepts standard client DNS queries, serves repeated lookups from an in-memory TTL-aware cache with decremented remaining lifetimes, protects against DNS spoofing through 16-bit random upstream Query-ID rewriting and strict three-way response matching, and ensures high availability via automatic primary-to-backup upstream failover with background recovery probing.

Built for **EE 347: Computer Networks | Group 8 | NUST SEECS (Fall 2026)**.

---

## Team & Contributions

| Member | Student ID | Modules & Contributions |
|---|---|---|
| **Tayyba Bashir** | `541682` | **Forwarder Core & Concurrency:**<br>• UDP listener loop & worker thread pool (`src/listener.py`, C1, C6)<br>• Thread-safe pending-query table & 16-bit Query-ID rewriter (`src/pending_table.py`, C3)<br>• Strict upstream reply matching (Query-ID, IP:port, question section) & drop counter (C4)<br>• 1.0 s upstream timeout with single backup retry logic (C7)<br>• Structured query logger & live CLI statistics reporting (`ForwarderStats`, C10)<br>• Concurrency benchmark & failover experiments (S4, S5, `tests/benchmark_s5.py`) |
| **Zainab Parveen** | `550827` | **DNS Engine & Upstream Management:**<br>• `dnslib` packet parser & response builder (`src/builder.py`, C1, C2)<br>• In-memory TTL cache with remaining-TTL countdown (`src/cache.py`, C5)<br>• Upstream state tracker with 3-timeout failover rule (`src/selector.py`, C8)<br>• Dedicated health prober & recovery probe factory (`src/probe.py`, C9)<br>• Controlled mock DNS upstream servers & TCP control client (`testserver/`, C11)<br>• Caching benefit, delay, and record change experiments (S1–S3) |

---

## Key Features

- **Concurrent UDP Request Processing (C1, C6):** Worker thread pool handles client datagrams on port `5300`, decoupling receive I/O from upstream resolution. Lock-protected data structures prevent race conditions across concurrent clients.
- **TTL-Aware In-Memory Cache (C5):** Answers standard `A` and `AAAA` queries from cache, returning copies of resource records carrying the accurate *remaining* TTL (rounded up so live entries never report TTL 0). Entries with TTL 0 or error responses are never cached.
- **Randomized 16-Bit Upstream Query-ID Rewriting (C3):** Per RFC 5452, every uncached miss receives a cryptographically randomized 16-bit upstream ID checked for collisions in a thread-safe pending table before forwarding. The client's original ID is restored in the final response.
- **Strict Response Matching & Forgery Protection (C4):** Accepts upstream responses only if the Query-ID, the source `IP:port`, and the question section `(name, qtype, qclass)` match an in-flight query. Stray, forged, or late packets are dropped and tracked on a mismatch counter.
- **Upstream Failover & Recovery Probing (C7, C8, C9):**
  - **1.0 s Timeout & Retry (C7):** If the selected upstream fails to reply within 1.0 s, the forwarder retries the query once on the alternate upstream before returning `SERVFAIL`.
  - **3-Timeout Failover Rule (C8):** 3 consecutive timeouts mark the primary upstream `DOWN`, automatically switching all subsequent queries to the backup server.
  - **Background Health Prober (C9):** While the primary is `DOWN`, a daemon prober tests `probe.lab.test` every 5.0 s. After 2 consecutive successful probes, the primary is marked `UP` and restored as active.
- **Unsupported Type Handling (C2):** Queries for non-A/AAAA types (such as `MX`, `TXT`) immediately return `RCODE NOTIMP` without forwarding. EDNS `OPT` pseudo-records are cleanly stripped.
- **Structured Query Logging & Live CLI Metrics (C10):** Every query logs a single-line transaction record with client IP:port, domain, record type, hit/miss status, latency in milliseconds, and RCODE. A CLI summary banner prints on shutdown (`Ctrl+C`) or periodically.

---

## Project Layout

```text
dns-caching-forwarder/
├── README.md                           # Complete project guide and reproduction steps
├── .gitignore                          # Standard Python gitignore (ignores .venv, bytecode)
│
├── src/                                # Core forwarder source modules
│   ├── builder.py                      # DNS packet parser & reply builder (dnslib)
│   ├── cache.py                        # TTL cache with remaining-TTL calculations
│   ├── listener.py                     # UDP forwarder, worker pool, CLI, and C10 metrics
│   ├── pending_table.py                # Thread-safe pending-query table & ID allocator
│   ├── probe.py                        # Recovery probe generator for primary upstream
│   └── selector.py                     # Upstream UP/DOWN selector & HealthProber thread
│
├── testserver/                         # Controlled upstream DNS servers (C11)
│   ├── test_dns_server.py              # Controllable mock DNS server with TCP control port
│   ├── ctl.py                          # Runtime control client (delay, stop, start, set)
│   ├── records_primary.json            # Primary server DNS records (e.g. www -> 10.0.0.1)
│   └── records_backup.json             # Backup server DNS records (e.g. www -> 10.1.1.1)
│
└── tests/                              # Unit test suite & Experiment benchmarks
    ├── conftest.py                     # Pytest path configuration
    ├── test_builder.py                 # Tests for query parsing, hit_reply, notimp, servfail
    ├── test_cache.py                   # Tests for TTL countdown, expiry, and concurrency
    ├── test_listener.py                # Tests for listener, strict matching, and metrics
    ├── test_probe.py                   # Tests for probe generation and prober recovery
    ├── test_selector.py                # Tests for 3-timeout failover and state transitions
    ├── test_testserver.py              # Tests for mock DNS server answers and control API
    └── benchmark_s5.py                 # Concurrency benchmark script for Experiment S5
```

---

## Setup & Running the 4-Terminal Demo

### 1. Prerequisites & Environment Setup

Clone the repository and install dependencies using Python 3:

```bash
cd dns-caching-forwarder

# Create a virtual environment and activate it
python3 -m venv .venv
source .venv/bin/activate

# Install required dependencies
pip install dnslib pytest
```

Run the automated test suite (all 29 unit tests):

```bash
pytest tests/
```

---

### 2. Multi-Terminal Live Demo

Open 4 separate terminal tabs to demonstrate caching, strict forwarding, and failover:

#### **Terminal 1: Start Primary Upstream Server (Port 5301)**
Runs the primary mock DNS server with control port `6301`:
```bash
source .venv/bin/activate
python3 testserver/test_dns_server.py --port 5301 --control-port 6301 --records testserver/records_primary.json
```

#### **Terminal 2: Start Backup Upstream Server (Port 5302)**
Runs the backup mock DNS server with control port `6302`:
```bash
source .venv/bin/activate
python3 testserver/test_dns_server.py --port 5302 --control-port 6302 --records testserver/records_backup.json
```

#### **Terminal 3: Start the DNS Caching Forwarder (Port 5300)**
Listens on `127.0.0.1:5300` and forwards to primary `5301` / backup `5302`:
```bash
source .venv/bin/activate
python3 src/listener.py --port 5300 --primary 127.0.0.1:5301 --backup 127.0.0.1:5302
```

*(Optional CLI flags: `--no-cache` to evaluate baseline B2; `--log-file queries.log` to export transaction logs; `--stats-interval 5` to print periodic stats).*

---

#### **Terminal 4: Client Verification & Experiments**

Run the following client commands in Terminal 4 to verify each feature:

##### **A. Cache Miss vs. Cache Hit (TTL Countdown)**
```bash
# 1. First query: MISS -> forwarded to Primary (10.0.0.1), cached with TTL 30
dig @127.0.0.1 -p 5300 www.lab.test A

# 2. Immediate second query: HIT -> served instantly from cache, TTL ~29
dig @127.0.0.1 -p 5300 www.lab.test A

# 3. Query after 5 seconds: HIT -> TTL continues to count down (~24)
sleep 5
dig @127.0.0.1 -p 5300 www.lab.test A
```

##### **B. Unsupported Query Type Handling (C2)**
```bash
# Querying MX returns RCODE NOTIMP without hitting upstreams
dig @127.0.0.1 -p 5300 www.lab.test MX
```

##### **C. Failover & Recovery Demo (C8, C9)**
```bash
# 1. Simulate Primary failure: stop the primary server via control port
python3 testserver/ctl.py 6301 stop

# 2. Send 3 uncached queries to trigger the 3-timeout threshold
dig @127.0.0.1 -p 5300 fail1.lab.test A +tries=1 +time=1
dig @127.0.0.1 -p 5300 fail2.lab.test A +tries=1 +time=1
dig @127.0.0.1 -p 5300 fail3.lab.test A +tries=1 +time=1

# 3. Primary is now marked DOWN. Querying returns the Backup address (10.1.1.1):
dig @127.0.0.1 -p 5300 app.lab.test A

# 4. Restore the Primary server:
python3 testserver/ctl.py 6301 start

# 5. After ~10 seconds (2 recovery probes to probe.lab.test), Primary is marked UP:
sleep 10
dig @127.0.0.1 -p 5300 www.lab.test A
```

##### **D. Concurrency Benchmark (Experiment S5)**
Run the automated multi-threaded load test simulating 1, 5, 10, and 20 parallel client workers:
```bash
python3 tests/benchmark_s5.py --port 5300 --queries-per-level 50
```

Sample output confirming **0.0% drop rate** and sub-20ms latency under 20 concurrent threads:
```text
========================================================================
                     BENCHMARK RESULTS SUMMARY
========================================================================
Concurrency  Sent    Received   Drop Rate    Mean Lat (ms)   p95 Lat (ms) 
------------------------------------------------------------------------
1            50      50         0.0%         2.02            3.54         
5            50      50         0.0%         3.83            12.98        
10           50      50         0.0%         8.50            16.34        
20           50      50         0.0%         10.64           18.93        
========================================================================
>> PASS: Objective O4 met (0% drop rate across all concurrency levels) <<
========================================================================
```

##### **E. Shutdown CLI Statistics View (C10)**
Press `Ctrl+C` in Terminal 3 (Forwarder) to stop the server and observe the final statistics summary report:
```text
================================================================
                 DNS FORWARDER STATISTICS (C10)
================================================================
 Total Queries:          254
 Cache Hits:             178 (70.1%)
 Cache Misses:           74 (29.1%)
 Errors / Unsupported:   2
 Mismatched / Dropped:   0
----------------------------------------------------------------
 Resolution Latency (ms):
   Min:                  0.17 ms
   Mean:                 5.12 ms
   95th Percentile:      18.24 ms
   Max:                  32.10 ms
----------------------------------------------------------------
 Upstream Server Query Counts:
   Primary 127.0.0.1:5301 (UP  ):  62
   Backup  127.0.0.1:5302 (UP  ):  12
 Active Upstream:        127.0.0.1:5301
================================================================
```
