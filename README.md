# DNS Caching Forwarder with Upstream Failover

A Python-based DNS caching forwarder built using `dnslib`.

## Team
- **Tayyba Bashir** – UDP listener loop & worker pool, Query-ID rewriting & pending-query table, Logger/CLI scaffold (C1, C3, C4, C6, C10)
- **Zainab Parveen** – dnslib parser/builder, TTL Cache, Upstream selector & failover/health prober, Local mock upstream DNS servers (C2, C5, C7, C8, C9, C11)

## Project Structure
```
src/
├── pending_table.py   # Thread-safe pending-query table (Tayyba)
├── listener.py        # UDP listener & query-ID rewriter (Tayyba)
├── cache.py           # TTL cache (Zainab)
├── upstream.py        # Upstream selector & failover (Zainab)
├── parser.py          # dnslib parser/builder (Zainab)
└── mock_upstream.py   # Local mock DNS servers (Zainab)
```

## Requirements
```
dnslib
```

## Running
```bash
python src/listener.py --port 5300 --upstream 8.8.8.8:53
```
