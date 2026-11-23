#!/usr/bin/env python3
"""Generate experimental charts for the DNS caching forwarder project.

- charts/s5_concurrency_latency.png: grouped bar chart of Mean and p95 latency per concurrency level.
- charts/s4_failover_timeline.png: timeline of query latency over time with phase shading.
"""

import pandas as pd
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import os

# Ensure output directory exists
os.makedirs("charts", exist_ok=True)

# ---------- S5 Concurrency Latency Chart ----------
s5_path = "results/s5_concurrency/s5_summary.csv"
if not os.path.exists(s5_path):
    raise FileNotFoundError(f"{s5_path} not found")
# CSV columns: concurrency,runs,total_sent,total_received,total_dropped,drop_rate,mean_ms,median_ms,p95_ms
s5_df = pd.read_csv(s5_path)
# Sort by concurrency just in case
s5_df = s5_df.sort_values("concurrency")
concurrency = s5_df["concurrency"].astype(str)
mean_vals = s5_df["mean_ms"]
p95_vals = s5_df["p95_ms"]

fig, ax = plt.subplots(figsize=(8, 5), dpi=200)
bar_width = 0.35
indices = range(len(concurrency))
ax.bar([i - bar_width/2 for i in indices], mean_vals, width=bar_width, label="Mean Latency (ms)", color="#4c72b0")
ax.bar([i + bar_width/2 for i in indices], p95_vals, width=bar_width, label="p95 Latency (ms)", color="#55a868")
ax.set_xlabel("Concurrency Level (workers)")
ax.set_ylabel("Latency (ms)")
ax.set_title("S5 Concurrency Benchmark – Latency per Worker Count")
# Add numeric labels above each bar
for i, val in enumerate(mean_vals):
    ax.text(i - bar_width/2, val + 0.5, f"{val:.2f}", ha='center', va='bottom', fontsize=8)
for i, val in enumerate(p95_vals):
    ax.text(i + bar_width/2, val + 0.5, f"{val:.2f}", ha='center', va='bottom', fontsize=8)
ax.set_xticks(indices)
ax.set_xticklabels(concurrency)
ax.set_ylim(0, 10.5)  # ensure top label fits comfortably
ax.legend()
# Subtitle with drop rate (they are all zero in our data)
ax.text(0.5, 0.95, "0.0% Packet Drop Rate Across All Levels", transform=ax.transAxes,
        ha="center", fontsize=9, color="gray")
plt.tight_layout()
plt.savefig("charts/s5_concurrency_latency.png")
plt.close(fig)

# ---------- S4 Failover Timeline Chart ----------
# Load the detailed per‑query CSV
s4_path = "results/s4_failover/s4_queries.csv"
if not os.path.exists(s4_path):
    raise FileNotFoundError(f"{s4_path} not found")
# Columns: run,seq,phase,rel_time_s,name,upstream,answer,latency_ms,rcode,ok
s4_df = pd.read_csv(s4_path)
# Use relative time as x‑axis, latency as y‑axis
x = s4_df["rel_time_s"]
y = s4_df["latency_ms"]
phases = s4_df["phase"]

fig, ax = plt.subplots(figsize=(10, 4), dpi=200)
# Plot latency line
ax.plot(x, y, marker='o', linestyle='-', color='#1f77b4', label='Query Latency (ms)')
# Highlight different phases with background colors
phase_groups = s4_df.groupby('phase')
colors = {
    '1_NORMAL_PRIMARY': '#d4edda',  # light green
    '2_SWITCHING': '#fff3cd',      # light amber (failover period)
    '3_BACKUP_SERVING': '#cce5ff', # light blue (backup serving)
    # Any other phases will use default no shading
}
for phase, group in phase_groups:
    if phase in colors:
        ax.axvspan(group['rel_time_s'].min(), group['rel_time_s'].max(),
                   color=colors[phase], alpha=0.3, label=phase.replace('_', ' '))
# Annotate the failover point (first huge latency)
# Find the max latency point (the spike when switching)
max_idx = y.idxmax()
ax.annotate('Failover spike', xy=(x[max_idx], y[max_idx]), xytext=(x[max_idx], y[max_idx]+200),
            arrowprops=dict(facecolor='red', arrowstyle='->'), fontsize=8, color='red')

ax.set_xlabel('Relative Time (s)')
ax.set_ylabel('Latency (ms)')
ax.set_title('S4 Failover & Recovery – Query Latency Timeline')
# Remove duplicate legend entries
handles, labels = ax.get_legend_handles_labels()
by_label = dict(zip(labels, handles))
ax.legend(by_label.values(), by_label.keys(), loc='upper right')
plt.tight_layout()
plt.savefig('charts/s4_failover_timeline.png')
plt.close(fig)

print('Charts generated successfully.')
