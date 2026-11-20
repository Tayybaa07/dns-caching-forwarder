"""Shared helpers for the experiment scripts (S1 to S3).

Each experiment starts its own primary/backup test servers and forwarder on
ports 5401 / 5402 / 5400, so it never clashes with the servers you run by hand
(5301 / 5302 / 5300). Close nothing; just run the script.
"""
import csv
import json
import math
import os
import random
import re
import socket
import statistics
import subprocess
import sys
import tempfile
import time

from dnslib import DNSRecord, RCODE

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PY = sys.executable
FWD_PORT, PRIMARY_PORT, BACKUP_PORT = 5400, 5401, 5402


# ---------------------------------------------------------------- processes
def _wait_for(predicate, timeout, what):
    end = time.time() + timeout
    while time.time() < end:
        if predicate():
            return
        time.sleep(0.1)
    raise RuntimeError(f"timed out waiting for {what}")


def wait_until(predicate, timeout):
    """Poll until predicate() is true or the timeout passes. Returns the final value."""
    end = time.time() + timeout
    while time.time() < end:
        if predicate():
            return True
        time.sleep(0.05)
    return predicate()


def ctl(dns_port, command):
    """Send one control command to a test server (control port = DNS port + 1000)."""
    with socket.create_connection(("127.0.0.1", dns_port + 1000), timeout=3) as s:
        s.sendall((command + "\n").encode())
        return s.makefile().readline().strip()


def server_queries(dns_port):
    """Number of queries the test server has answered so far."""
    return json.loads(ctl(dns_port, "status"))["queries"]


def start_test_server(port, records, workdir, delay_ms=0):
    path = os.path.join(workdir, f"records_{port}.json")
    with open(path, "w") as f:
        json.dump(records, f)
    out = open(os.path.join(workdir, f"server_{port}.log"), "w")
    proc = subprocess.Popen(
        [PY, os.path.join(ROOT, "testserver", "test_dns_server.py"),
         "--port", str(port), "--records", path, "--delay", str(delay_ms)],
        stdout=out, stderr=subprocess.STDOUT, cwd=ROOT)
    _wait_for(lambda: _ctl_ok(port), 8, f"test server on {port}")
    return proc


def _ctl_ok(port):
    try:
        ctl(port, "status")
        return True
    except OSError:
        return False


def start_forwarder(workdir, log_file, no_cache=False, extra=()):
    stdout_path = os.path.join(workdir, "forwarder_stdout.log")
    out = open(stdout_path, "w")
    cmd = [PY, os.path.join(ROOT, "src", "listener.py"),
           "--port", str(FWD_PORT),
           "--primary", f"127.0.0.1:{PRIMARY_PORT}",
           "--backup", f"127.0.0.1:{BACKUP_PORT}",
           "--log-file", log_file]
    if no_cache:
        cmd.append("--no-cache")
    cmd += list(extra)
    proc = subprocess.Popen(cmd, stdout=out, stderr=subprocess.STDOUT, cwd=ROOT)

    def ready():
        with open(stdout_path) as f:
            return "Client receive loop started" in f.read()
    _wait_for(ready, 10, "forwarder start-up")
    return proc


def stop(proc):
    if proc and proc.poll() is None:
        proc.terminate()
        try:
            proc.wait(timeout=4)
        except subprocess.TimeoutExpired:
            proc.kill()


# ------------------------------------------------------------------ queries
def query(port, name, qtype="A", timeout=3.0, host="127.0.0.1"):
    """One DNS query. Returns a dict with latency, rcode, ID check and answers."""
    q = DNSRecord.question(name, qtype)
    qid = random.randint(1, 0xFFFF)
    q.header.id = qid
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.settimeout(timeout)
    t0 = time.perf_counter()
    try:
        sock.sendto(q.pack(), (host, port))
        data, _ = sock.recvfrom(4096)
    except socket.timeout:
        return dict(ok=False, latency_ms=(time.perf_counter() - t0) * 1000,
                    rcode="TIMEOUT", id_ok=False, answers=[], ttl=None)
    finally:
        sock.close()
    ms = (time.perf_counter() - t0) * 1000
    r = DNSRecord.parse(data)
    return dict(ok=r.header.rcode == RCODE.NOERROR, latency_ms=ms,
                rcode=RCODE[r.header.rcode], id_ok=r.header.id == qid,
                answers=[str(rr.rdata) for rr in r.rr],
                ttl=min((rr.ttl for rr in r.rr), default=None))


# --------------------------------------------------------------- statistics
def percentile(values, p):
    """Nearest-rank percentile (p in 0..100)."""
    if not values:
        return float("nan")
    v = sorted(values)
    return v[max(0, math.ceil(p / 100 * len(v)) - 1)]


def describe(values):
    if not values:
        return dict(mean=float("nan"), median=float("nan"), p95=float("nan"))
    return dict(mean=statistics.fmean(values), median=statistics.median(values),
                p95=percentile(values, 95))


_LOG_RE = re.compile(r"result=(HIT|MISS|ERR)")


def read_results(log_file):
    """Ordered list of HIT/MISS/ERR from the forwarder's per-query log."""
    if not os.path.exists(log_file):
        return []
    with open(log_file) as f:
        return [m.group(1) for line in f if (m := _LOG_RE.search(line))]


def write_csv(path, rows, fields):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        w.writerows(rows)


def wait_for_capture(enabled):
    if enabled:
        input("\nStart the Wireshark capture now "
              f"(filter: udp.port == {FWD_PORT} or udp.port == {PRIMARY_PORT}), "
              "then press Enter... ")


def workdir():
    return tempfile.mkdtemp(prefix="dnsexp_")
