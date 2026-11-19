# DNS Caching Forwarder with Upstream Failover

A multi-threaded UDP DNS caching forwarder written in Python using `dnslib`. It answers repeated A/AAAA queries from an in-memory TTL cache, rewrites 16-bit Query-IDs to prevent spoofing, strictly validates upstream replies, and automatically fails over to a backup upstream when the primary goes down.

**Course:** EE 347: Computer Networks — NUST SEECS (Fall 2026)  
**Group:** 8

---

## Team & Task Division

| Member | Reg ID | Assigned Modules |
|---|---|---|
| **Tayyba Bashir** | `541682` | UDP listener & worker pool (`listener.py`), pending query table & Query-ID rewriter (`pending_table.py`), upstream response validation, timeout/retry logic, structured logger/CLI stats, and concurrency testing. |
| **Zainab Parveen** | `550827` | DNS packet parser & reply builder (`builder.py`), TTL cache (`cache.py`), upstream selector & recovery prober (`selector.py`, `probe.py`), and controllable mock DNS servers (`testserver/`). |

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
│   ├── ctl.py                # Runtime control client (stop, start, delay, set)
│   ├── records_primary.json  # Primary test records (www.lab.test -> 10.0.0.1)
│   └── records_backup.json   # Backup test records (www.lab.test -> 10.1.1.1)
└── tests/
    ├── conftest.py           # Test configuration and path setup
    ├── test_builder.py       # Builder unit tests
    ├── test_cache.py         # Cache unit tests
    ├── test_listener.py      # Listener and matching unit tests
    ├── test_probe.py         # Prober unit tests
    ├── test_selector.py      # Upstream selector unit tests
    ├── test_testserver.py    # Test server unit tests
    └── benchmark_s5.py       # Multi-threaded concurrency benchmark
```

---

## Setup & Running

### 1. Environment Setup

```bash
# Clone the repository
git clone https://github.com/Tayybaa07/dns-caching-forwarder.git
cd dns-caching-forwarder

# Create and activate virtual environment
python3 -m venv .venv
source .venv/bin/activate

# Install dependencies and run tests
pip install dnslib pytest
pytest tests/
```

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

---

### 3. Client Verification (Terminal 4)

**A. Cache Miss vs. Cache Hit (TTL Countdown):**
```bash
# First query: MISS -> forwarded upstream, cached with TTL 30
dig @127.0.0.1 -p 5300 www.lab.test A

# Immediate second query: HIT -> served instantly from cache, decremented TTL
dig @127.0.0.1 -p 5300 www.lab.test A
```

**B. Unsupported Query Type (NOTIMP):**
```bash
dig @127.0.0.1 -p 5300 www.lab.test MX
```

**C. Failover & Recovery:**
```bash
# 1. Stop primary server
python3 testserver/ctl.py 6301 stop

# 2. Trigger 3 timeouts -> forwarder marks primary DOWN and switches to backup
dig @127.0.0.1 -p 5300 fail1.lab.test A +tries=1 +time=1
dig @127.0.0.1 -p 5300 fail2.lab.test A +tries=1 +time=1
dig @127.0.0.1 -p 5300 fail3.lab.test A +tries=1 +time=1

# 3. Query resolved by backup (returns 10.1.1.2)
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
