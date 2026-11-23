"""Graphs for S1 to S3, made from the CSV files in results/.

    pip install matplotlib
    python experiments/make_graphs.py

Writes three PNG files to results/graphs/:
  1. s1_mean_response_time.png   bar chart: direct upstream, forwarder without cache, with cache
  2. s2_miss_time_vs_delay.png   line chart: miss (and hit) response time against upstream delay
  3. s2_hit_ratio_vs_ttl.png     line chart: cache-hit ratio against TTL
Error bars show the spread (standard deviation) over the repeated runs.
"""
import csv
import os
import statistics

import matplotlib
matplotlib.use("Agg")                       # draw to files, no window needed
import matplotlib.pyplot as plt

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RES = os.path.join(ROOT, "results")
OUT = os.path.join(RES, "graphs")

NAVY, ORANGE, TEAL, GREY = "#1f3a5f", "#e07a2f", "#2a9d8f", "#6b7280"
plt.rcParams.update({"font.size": 11, "axes.spines.top": False, "axes.spines.right": False,
                     "axes.grid": True, "grid.alpha": 0.25, "figure.dpi": 100})


def read(path):
    with open(os.path.join(RES, path), newline="") as f:
        return list(csv.DictReader(f))


def spread(values):
    return statistics.stdev(values) if len(values) > 1 else 0.0


def save(fig, name):
    os.makedirs(OUT, exist_ok=True)
    path = os.path.join(OUT, name)
    fig.tight_layout()
    fig.savefig(path, dpi=200)
    plt.close(fig)
    print("wrote", os.path.relpath(path, ROOT))


# ---- 1. S1 bar chart ------------------------------------------------------
def s1_bars():
    configs = [("B1_direct", "Direct to upstream", GREY),
               ("B2_nocache", "Forwarder, no cache", ORANGE),
               ("FULL_cache", "Forwarder with cache", TEAL)]
    settings = [("s1_delay0", "Upstream delay 0 ms"), ("s1_delay20", "Upstream delay 20 ms")]
    fig, ax = plt.subplots(figsize=(8, 5))
    width = 0.25
    summary = []
    for k, (folder, label) in enumerate(settings):
        rows = read(f"{folder}/s1_runs.csv")
        for j, (cfg, cfg_label, color) in enumerate(configs):
            means = [float(r["mean_ms"]) for r in rows if r["config"] == cfg]
            m, s = statistics.fmean(means), spread(means)
            x = k + (j - 1) * width
            ax.bar(x, m, width, yerr=s, capsize=4, color=color,
                   label=cfg_label if k == 0 else None)
            ax.text(x, m + s + 0.4, f"{m:.1f}", ha="center", va="bottom", fontsize=10)
            summary.append((label, cfg_label, m, s, len(means)))
    ax.set_xticks(range(len(settings)))
    ax.set_xticklabels([s[1] for s in settings])
    ax.set_ylabel("Mean response time (ms)")
    ax.set_title("S1: mean response time with and without the cache")
    ax.legend(frameon=False)
    save(fig, "s1_mean_response_time.png")
    print("\nS1 table (mean of the per-run means, spread = std dev over runs)")
    for label, cfg, m, s, n in summary:
        print(f"  {label:22} {cfg:22} {m:7.2f} ms  +/- {s:5.2f}  ({n} runs)")


# ---- 2. S2 delay line chart ----------------------------------------------
def s2_delay():
    rows = read("s2_conditions/s2_delay_runs.csv")
    delays = sorted({int(r["delay_ms"]) for r in rows})
    miss_m, miss_s, hit_m, hit_s = [], [], [], []
    for d in delays:
        miss = [float(r["miss_mean_ms"]) for r in rows if int(r["delay_ms"]) == d]
        hit = [float(r["hit_mean_ms"]) for r in rows if int(r["delay_ms"]) == d]
        miss_m.append(statistics.fmean(miss)); miss_s.append(spread(miss))
        hit_m.append(statistics.fmean(hit)); hit_s.append(spread(hit))
    n = len([r for r in rows if int(r["delay_ms"]) == delays[0]])
    fig, ax = plt.subplots(figsize=(8, 5))
    ax.errorbar(delays, miss_m, yerr=miss_s, marker="o", capsize=4, color=ORANGE,
                linewidth=2, label="Cache miss")
    ax.errorbar(delays, hit_m, yerr=hit_s, marker="s", capsize=4, color=TEAL,
                linewidth=2, label="Cache hit")
    for d, m in zip(delays, miss_m):
        ax.annotate(f"{m:.0f} ms", (d, m), textcoords="offset points", xytext=(0, 9),
                    ha="center", fontsize=10)
    ax.set_xticks(delays)
    ax.set_xlabel("Artificial upstream delay (ms)")
    ax.set_ylabel("Mean response time (ms)")
    ax.set_title(f"S2: response time against upstream delay ({n} runs per point)")
    ax.legend(frameon=False)
    save(fig, "s2_miss_time_vs_delay.png")
    print("\nS2 delay table")
    for d, mm, ms, hm, hs in zip(delays, miss_m, miss_s, hit_m, hit_s):
        print(f"  delay {d:3} ms: miss {mm:7.2f} +/- {ms:5.2f} ms   hit {hm:5.2f} +/- {hs:4.2f} ms")


# ---- 3. S2 TTL line chart -------------------------------------------------
def s2_ttl():
    rows = read("s2_conditions/s2_ttl_runs.csv")
    ttls = sorted({int(r["ttl_s"]) for r in rows})
    means, spreads = [], []
    for t in ttls:
        v = [float(r["hit_ratio"]) * 100 for r in rows if int(r["ttl_s"]) == t]
        means.append(statistics.fmean(v)); spreads.append(spread(v))
    n = len([r for r in rows if int(r["ttl_s"]) == ttls[0]])
    fig, ax = plt.subplots(figsize=(8, 5))
    ax.errorbar(ttls, means, yerr=spreads, marker="o", capsize=4, color=NAVY, linewidth=2)
    for t, m in zip(ttls, means):
        ax.annotate(f"{m:.0f}%", (t, m), textcoords="offset points", xytext=(0, 9),
                    ha="center", fontsize=10)
    ax.set_xscale("log")
    ax.set_xticks(ttls)
    ax.set_xticklabels([f"{t} s" for t in ttls])
    ax.set_ylim(0, 105)
    ax.set_xlabel("Record TTL (log scale)")
    ax.set_ylabel("Cache-hit ratio (%)")
    ax.set_title(f"S2: cache-hit ratio against TTL ({n} runs per point)")
    save(fig, "s2_hit_ratio_vs_ttl.png")
    print("\nS2 TTL table")
    for t, m, s in zip(ttls, means, spreads):
        print(f"  TTL {t:3} s: hit ratio {m:5.1f}% +/- {s:4.2f}")


if __name__ == "__main__":
    s1_bars()
    s2_delay()
    s2_ttl()
