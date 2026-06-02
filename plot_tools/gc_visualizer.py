#!/usr/bin/env python3
"""
TerarkDB Blob GC Log Visualizer — Publication-Ready for SIGMOD-style Papers

Usage:  python gc_visualizer.py --log /path/to/LOG [--cf default] [--out ./output]

Output charts:
  1. GC bandwidth composition          (stacked bars + pie)
  2. Block invalidity distribution     (histogram + CDF)
  3. GC per-phase latency breakdown    (pie + per-job line series)
  4. GC bandwidth over time            (timeseries)
  5. GC job duration distribution      (histogram + percentiles)
"""

import re
import argparse
import os
from collections import defaultdict
from typing import Optional

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import numpy as np

# ═══════════════════════════════════════════════════════════════════════════════
#  Publication-ready rcParams — SIGMOD / ACM double-column style
# ═══════════════════════════════════════════════════════════════════════════════

plt.rcParams.update({
    # Font
    "font.family":        "serif",
    "font.serif":         ["Times New Roman", "DejaVu Serif", "Liberation Serif"],
    "font.size":          9,
    "axes.titlesize":     10,
    "axes.labelsize":     9,
    "legend.fontsize":    7.5,
    "xtick.labelsize":    8,
    "ytick.labelsize":    8,

    # Figure
    "figure.dpi":         150,
    "savefig.dpi":        300,
    "savefig.bbox":       "tight",
    "savefig.pad_inches": 0.05,

    # Axes
    "axes.linewidth":     0.8,
    "axes.spines.top":    False,
    "axes.spines.right":  False,
    "axes.grid":          True,
    "grid.alpha":         0.25,
    "grid.linewidth":     0.5,

    # Lines / markers
    "lines.linewidth":    1.2,
    "lines.markersize":   3.5,
    "lines.markeredgewidth": 0,

    # Legend
    "legend.frameon":     True,
    "legend.framealpha":  0.9,
    "legend.edgecolor":   "#cccccc",
    "legend.fancybox":    False,
    "legend.borderpad":   0.3,
    "legend.handletextpad": 0.5,
    "legend.handlelength":  1.2,

    # LaTeX (fallback if unavailable will use mathtext)
    "text.usetex":        False,
})

# ═══════════════════════════════════════════════════════════════════════════════
#  Color palette — colorblind-friendly (Tableau 10 inspired)
# ═══════════════════════════════════════════════════════════════════════════════

PALETTE = {
    "vsst_read":        "#4C72B0",   # blue
    "ksst_read":        "#DD8452",   # orange
    "invalid_read":     "#C44E52",   # red
    "relocation_write": "#55A868",   # green
    "trigger":          "#8172B2",   # purple
    "select":           "#937860",   # brown
    "scan":             "#DA8BC3",   # pink
    "lookup":           "#8C8C8C",   # grey
    "write":            "#CCB974",   # gold
    "meta":             "#64B5CD",   # cyan
}

BAND_PALETTE = ["#55A868", "#4C72B0", "#CCB974", "#DD8452", "#C44E52"]

# ═══════════════════════════════════════════════════════════════════════════════
#  Regex parsers
# ═══════════════════════════════════════════════════════════════════════════════

RE_BEGIN = re.compile(
    r"\[(?P<cf>[^\]]+)\] \[JOB (?P<job>\d+)\] "
    r"GarbageCollection begin: ts=(?P<ts>\d+)"
)
RE_END = re.compile(
    r"\[(?P<cf>[^\]]+)\] \[JOB (?P<job>\d+)\] "
    r"GarbageCollection end: status=(?P<status>\S+), run_micros=(?P<run_micros>\d+)"
)
RE_BREAKDOWN = re.compile(
    r"\[(?P<cf>[^\]]+)\] \[JOB (?P<job>\d+)\] \[BLOB_GC_BREAKDOWN\] "
    r"blob_files:\[(?P<files>[^\]]*)\], "
    r"trigger:(?P<trigger>\d+), select:(?P<select>\d+), scan:(?P<scan>\d+), "
    r"lookup:(?P<lookup>\d+), write:(?P<write>\d+), meta:(?P<meta>\d+), "
    r"vsst_read:(?P<vsst_read>\d+), ksst_read:(?P<ksst_read>\d+), "
    r"invalid_read:(?P<invalid_read>\d+), relocation_write:(?P<relocation_write>\d+)"
)
RE_BLOCKDIST = re.compile(
    r"\[(?P<cf>[^\]]+)\] \[JOB (?P<job>\d+)\] \[BLOB_GC_BLOCK_DIST\] "
    r"blob_files:\[(?P<files>[^\]]*)\], "
    r"block_total:(?P<block_total>\d+), "
    r"invalid_0_25:(?P<i0_25>\d+), invalid_25_50:(?P<i25_50>\d+), "
    r"invalid_50_75:(?P<i50_75>\d+), invalid_75_100:(?P<i75_100>\d+), "
    r"invalid_100:(?P<i100>\d+), skippable_ratio:(?P<skippable>[0-9.]+)%"
)


def parse_log(log_path: str, cf_filter: Optional[str] = None):
    """Parse TerarkDB INFO_LOG and extract GC instrumentation records.

    Returns
    -------
    breakdowns : list[dict]
        Per-job BLOB_GC_BREAKDOWN records augmented with ``run_micros``
        and ``status`` from the matching GarbageCollection end line.
    blockdists : list[dict]
        Per-job BLOB_GC_BLOCK_DIST records.
    """
    begins = {}       # job_id -> ts_us
    ends   = {}       # job_id -> {status, run_micros}
    breakdowns = []
    blockdists = []

    with open(log_path, "r", errors="replace") as f:
        for line in f:
            m = RE_BEGIN.search(line)
            if m and (cf_filter is None or m.group("cf") == cf_filter):
                begins[int(m.group("job"))] = int(m.group("ts"))
                continue

            m = RE_END.search(line)
            if m and (cf_filter is None or m.group("cf") == cf_filter):
                ends[int(m.group("job"))] = {
                    "status":     m.group("status"),
                    "run_micros": int(m.group("run_micros")),
                }
                continue

            m = RE_BREAKDOWN.search(line)
            if m and (cf_filter is None or m.group("cf") == cf_filter):
                job = int(m.group("job"))
                rec = {
                    k: int(m.group(k))
                    for k in ("trigger", "select", "scan", "lookup",
                              "write", "meta", "vsst_read", "ksst_read",
                              "invalid_read", "relocation_write")
                }
                rec["job"]   = job
                rec["cf"]    = m.group("cf")
                rec["files"] = m.group("files")
                rec["ts_us"] = begins.get(job, 0)
                breakdowns.append(rec)
                continue

            m = RE_BLOCKDIST.search(line)
            if m and (cf_filter is None or m.group("cf") == cf_filter):
                job = int(m.group("job"))
                rec = {
                    "job":         job,
                    "cf":          m.group("cf"),
                    "block_total": int(m.group("block_total")),
                    "i0_25":       int(m.group("i0_25")),
                    "i25_50":      int(m.group("i25_50")),
                    "i50_75":      int(m.group("i50_75")),
                    "i75_100":     int(m.group("i75_100")),
                    "i100":        int(m.group("i100")),
                    "skippable":   float(m.group("skippable")),
                    "ts_us":       begins.get(job, 0),
                }
                blockdists.append(rec)

    # Attach end-of-job metadata to each breakdown record
    for rec in breakdowns:
        e = ends.get(rec["job"], {})
        rec["run_micros"] = e.get("run_micros", 0)
        rec["status"]     = e.get("status", "unknown")

    return breakdowns, blockdists


# ═══════════════════════════════════════════════════════════════════════════════
#  Helpers
# ═══════════════════════════════════════════════════════════════════════════════

def _bytes_to_mb(b: int) -> float:
    return b / (1024 * 1024)


def _ns_to_ms(ns: int) -> float:
    return ns / 1e6


def _us_to_sec(us: int) -> float:
    return us / 1e6


def _save(fig, out_dir: str, name: str, fmt: str = "png"):
    """Save figure in both raster (PNG) and vector (PDF) formats."""
    base, _ = os.path.splitext(name)
    for ext in ("png", "pdf"):
        path = os.path.join(out_dir, f"{base}.{ext}")
        fig.savefig(path, dpi=300, bbox_inches="tight", pad_inches=0.05)
    plt.close(fig)
    print(f"  [saved] {out_dir}/{base}.{{png,pdf}}")


# ═══════════════════════════════════════════════════════════════════════════════
#  Figure 1a — GC Bandwidth Composition (Pie Chart)
# ═══════════════════════════════════════════════════════════════════════════════

def plot_bandwidth_pie(breakdowns, out_dir: str):
    """Pie chart: cumulative I/O distribution across all GC jobs."""
    if not breakdowns:
        print("  [skip] no BREAKDOWN records")
        return

    labels = {
        "vsst_read":        "Live reads (vSST)",
        "ksst_read":        "GC lookups",
        "invalid_read":     "Wasted reads (dead KV)",
        "relocation_write": "Relocation writes",
    }

    # Aggregate data
    totals = defaultdict(float)
    for r in breakdowns:
        for k in labels:
            totals[k] += _bytes_to_mb(r[k])
    keys = list(labels.keys())

    fig, ax = plt.subplots(figsize=(4.5, 3.5))

    values = [totals[k] for k in keys]
    colors = [PALETTE[k] for k in keys]
    
    wedges, texts, autotexts = ax.pie(
        values,
        labels=[labels[k] for k in keys],
        colors=colors,
        autopct="%1.1f%%",
        startangle=140,
        pctdistance=0.6,
        wedgeprops={"edgecolor": "white", "linewidth": 0.8},
        textprops={"fontsize": 8},
    )
    
    for t in autotexts:
        t.set_fontsize(7)
        t.set_fontweight("bold")
    
    ax.set_title("Cumulative I/O Distribution", fontsize=11, fontweight="bold", pad=15)

    # Add annotation
    total_all = sum(values)
    waste_read = totals["invalid_read"] + totals["ksst_read"]
    waste_ratio = waste_read / (total_all + 1e-9) * 100
    fig.text(0.5, -0.08,
             f"Wasted read ratio = {waste_ratio:.1f}%",
             ha="center", fontsize=9, color=PALETTE["invalid_read"], fontweight="bold")

    fig.tight_layout()
    _save(fig, out_dir, "fig1a_bandwidth_pie.png")


# ═══════════════════════════════════════════════════════════════════════════════
#  Figure 1b — GC Bandwidth Composition (Stacked Bars)
# ═══════════════════════════════════════════════════════════════════════════════

def plot_bandwidth_stacked_bars(breakdowns, out_dir: str):
    """Stacked-bar chart: per-job I/O composition."""
    if not breakdowns:
        print("  [skip] no BREAKDOWN records")
        return

    labels = {
        "vsst_read":        "Live reads (vSST)",
        "ksst_read":        "GC lookups",
        "invalid_read":     "Wasted reads (dead KV)",
        "relocation_write": "Relocation writes",
    }

    # Sample data if too many jobs for clear visualization
    n = len(breakdowns)
    MAX_BARS = 50
    if n > MAX_BARS:
        sample_indices = np.linspace(0, n - 1, MAX_BARS, dtype=int)
        sampled_breakdowns = [breakdowns[i] for i in sample_indices]
        display_n = MAX_BARS
        xlabel = f"GC job id (sampled {MAX_BARS}/{n})"
    else:
        sampled_breakdowns = breakdowns
        display_n = n
        xlabel = "GC job id"
    
    fig, ax = plt.subplots(figsize=(6.0, 2.8))
    
    x = np.arange(display_n)
    width = max(0.5, min(0.85, 25.0 / max(display_n, 1)))

    bottom = np.zeros(display_n)
    order = ("vsst_read", "invalid_read", "ksst_read", "relocation_write")
    for k in order:
        vals = np.array([_bytes_to_mb(r[k]) for r in sampled_breakdowns])
        ax.bar(x, vals, width, bottom=bottom,
               color=PALETTE[k], label=labels[k], alpha=0.95,
               edgecolor="white", linewidth=0.4)
        bottom += vals

    ax.set_xlabel(xlabel, fontsize=9)
    ax.set_ylabel("I/O volume (MB)", fontsize=9)
    ax.set_title("Per-job I/O Composition", fontsize=11, fontweight="bold", pad=10)
    
    # Optimize legend for publication style - place outside the plot
    ax.legend(fontsize=7, ncol=1, loc="center left",
              bbox_to_anchor=(1.02, 0.5),
              columnspacing=0.8, handlelength=1.2, framealpha=0.95,
              edgecolor="#cccccc", fancybox=False)
    
    # Optimize y-axis
    ax.yaxis.set_major_locator(mticker.MaxNLocator(5))
    ax.yaxis.set_tick_params(labelsize=8)
    
    # Optimize x-axis labels for clarity
    if display_n > 30:
        # Show fewer but clearer labels
        num_labels = min(10, display_n // 5)
        label_step = max(1, display_n // num_labels)
        label_indices = list(range(0, display_n, label_step))
        
        ax.set_xticks([x[i] for i in label_indices])
        
        if n > MAX_BARS:
            # Format large numbers with K suffix
            def format_job_idx(idx):
                original_idx = sample_indices[idx]
                if original_idx >= 1000:
                    return f"{original_idx//1000}K"
                return str(original_idx)
            ax.set_xticklabels([format_job_idx(i) for i in label_indices], fontsize=8)
        else:
            ax.set_xticklabels([str(i) for i in label_indices], fontsize=8)
    else:
        ax.set_xticklabels([str(sample_indices[i]) if n > MAX_BARS else str(i) for i in x], fontsize=8)
    
    # Add subtle grid for better readability
    ax.grid(True, axis='y', alpha=0.3, linewidth=0.5)
    ax.set_axisbelow(True)

    fig.tight_layout()
    _save(fig, out_dir, "fig1b_bandwidth_stacked_bars.png")


# ═══════════════════════════════════════════════════════════════════════════════
#  Figure 2 — Block Invalidity Distribution
# ═══════════════════════════════════════════════════════════════════════════════

def plot_block_dist(blockdists, out_dir: str):
    """Histogram + CDF showing how dead-KV fraction is distributed across blocks."""
    if not blockdists:
        print("  [skip] no BLOCK_DIST records")
        return

    agg = {"i0_25": 0, "i25_50": 0, "i50_75": 0, "i75_100": 0, "i100": 0}
    total_blocks = 0
    for r in blockdists:
        for k in agg:
            agg[k] += r[k]
        total_blocks += r["block_total"]

    if total_blocks == 0:
        print("  [skip] block_total = 0")
        return

    bins  = ["[0, 25%)", "[25, 50%)", "[50, 75%)", "[75, 100%)", "100%"]
    counts = [agg["i0_25"], agg["i25_50"], agg["i50_75"],
              agg["i75_100"], agg["i100"]]
    pcts   = [c / total_blocks * 100 for c in counts]
    cumul  = np.cumsum(pcts)

    fig, ax1 = plt.subplots(figsize=(5.2, 2.8))

    x = np.arange(len(bins))
    bars = ax1.bar(x, pcts, color=BAND_PALETTE, edgecolor="white",
                   linewidth=0.8, alpha=0.92, width=0.65)
    ax1.set_xticks(x)
    ax1.set_xticklabels(bins)
    ax1.set_ylabel("Fraction of blocks (%)")
    ax1.set_xlabel("Dead-KV ratio within a logical block")
    ax1.set_ylim(0, max(pcts) * 1.20)
    ax1.yaxis.set_major_locator(mticker.MaxNLocator(6))

    # Annotate bar values
    for bar, pct, cnt in zip(bars, pcts, counts):
        ax1.text(bar.get_x() + bar.get_width() / 2,
                 bar.get_height() + max(pcts) * 0.015,
                 f"{pct:.1f}%\n({cnt:,})",
                 ha="center", va="bottom", fontsize=6.5, color="#333333")

    # CDF curve
    ax2 = ax1.twinx()
    ax2.plot(x, cumul, "D--", color="#333333", linewidth=1.2,
             markersize=4, label="Cumulative %")
    ax2.set_ylabel("Cumulative fraction (%)")
    ax2.set_ylim(0, 108)
    ax2.legend(loc="lower right", fontsize=6.5)

    # Skippable annotation
    avg_skip = np.mean([r["skippable"] for r in blockdists])
    fig.text(0.5, -0.04,
             f"100%-dead blocks: {pcts[-1]:.1f}%  |  Avg. skippable ratio: {avg_skip:.1f}%",
             ha="center", fontsize=8, color=PALETTE["invalid_read"],
             fontweight="bold")

    fig.tight_layout()
    _save(fig, out_dir, "fig2_block_distribution.png")


# ═══════════════════════════════════════════════════════════════════════════════
#  Figure 3 — Per-Phase Latency Breakdown
# ═══════════════════════════════════════════════════════════════════════════════

def plot_phase_time(breakdowns, out_dir: str):
    """Pie + per-job line plot: where GC spends its time across six phases."""
    if not breakdowns:
        print("  [skip] no BREAKDOWN records")
        return

    phases = ["trigger", "select", "scan", "lookup", "write", "meta"]
    labels = {
        "trigger": "Trigger wait",
        "select":  "File selection",
        "scan":    "vSST scan",
        "lookup":  "kSST lookup",
        "write":   "Reloc. writes",
        "meta":    "Metadata commit",
    }

    totals_ns = {p: sum(r[p] for r in breakdowns) for p in phases}

    fig, (ax_pie, ax_line) = plt.subplots(1, 2, figsize=(7.0, 2.8))

    # -- pie --
    values_ms = [_ns_to_ms(totals_ns[p]) for p in phases]
    colors    = [PALETTE[p] for p in phases]
    wedges, texts, autotexts = ax_pie.pie(
        values_ms,
        labels=[labels[p] for p in phases],
        colors=colors,
        autopct="%1.1f%%",
        startangle=140,
        pctdistance=0.6,
        wedgeprops={"edgecolor": "white", "linewidth": 0.8},
        textprops={"fontsize": 7},
    )
    for t in autotexts:
        t.set_fontsize(6)
    ax_pie.set_title("(a) Aggregate phase share", fontsize=9, fontweight="bold", pad=8)

    # -- per-job lines --
    n = len(breakdowns)
    
    # Sample data if too many jobs for clear visualization
    MAX_POINTS = 100
    if n > MAX_POINTS:
        sample_indices = np.linspace(0, n - 1, MAX_POINTS, dtype=int)
        sampled_breakdowns = [breakdowns[i] for i in sample_indices]
        display_n = MAX_POINTS
        xlabel = f"GC job id (sampled {MAX_POINTS}/{n})"
    else:
        sampled_breakdowns = breakdowns
        display_n = n
        xlabel = "GC job id"
    
    x = np.arange(display_n)
    for p in phases:
        vals = [_ns_to_ms(r[p]) for r in sampled_breakdowns]
        ax_line.plot(x, vals, label=labels[p], color=PALETTE[p],
                     linewidth=0.9, alpha=0.82)

    ax_line.set_xlabel(xlabel)
    ax_line.set_ylabel("Latency (ms)")
    ax_line.set_title("(b) Per-job phase latency", fontsize=9, fontweight="bold", pad=8)
    ax_line.legend(fontsize=6, ncol=2, columnspacing=0.5, handlelength=1.0)
    ax_line.yaxis.set_major_locator(mticker.MaxNLocator(6))

    fig.tight_layout()
    _save(fig, out_dir, "fig3_phase_latency.png")


# ═══════════════════════════════════════════════════════════════════════════════
#  Figure 4 — I/O Volume Over Time
# ═══════════════════════════════════════════════════════════════════════════════

def plot_bandwidth_over_time(breakdowns, out_dir: str):
    """Timeseries of per-job read / write volumes, revealing temporal patterns."""
    if not breakdowns:
        print("  [skip] no BREAKDOWN records")
        return

    ts = [_us_to_sec(r["ts_us"]) for r in breakdowns]
    use_index = all(t == 0 for t in ts)

    # Sample data if too many jobs for clear visualization
    MAX_POINTS = 150
    n = len(breakdowns)
    if n > MAX_POINTS:
        sample_indices = np.linspace(0, n - 1, MAX_POINTS, dtype=int)
        sampled_breakdowns = [breakdowns[i] for i in sample_indices]
        sampled_ts = [ts[i] for i in sample_indices]
        xlabel_suffix = f" (sampled {MAX_POINTS}/{n})"
    else:
        sampled_breakdowns = breakdowns
        sampled_ts = ts
        xlabel_suffix = ""

    if use_index:
        xs = list(range(len(sampled_breakdowns)))
        xlabel = "GC job id" + xlabel_suffix
    else:
        t0 = sampled_ts[0]
        xs = [t - t0 for t in sampled_ts]
        xlabel = "Elapsed time (s)" + xlabel_suffix

    fig, ax = plt.subplots(figsize=(5.8, 2.6))

    labels = {
        "vsst_read":        "Live reads (vSST)",
        "invalid_read":     "Wasted reads (dead KV)",
        "ksst_read":        "Index lookups (kSST)",
        "relocation_write": "Relocation writes",
    }
    for k, label in labels.items():
        vals = [_bytes_to_mb(r[k]) for r in sampled_breakdowns]
        ax.plot(xs, vals, label=label, color=PALETTE[k],
                linewidth=1.0, marker=".", markersize=3, alpha=0.85)

    ax.set_xlabel(xlabel)
    ax.set_ylabel("I/O volume (MB)")
    ax.legend(fontsize=7, ncol=2, columnspacing=0.5, handlelength=1.0)
    ax.yaxis.set_major_locator(mticker.MaxNLocator(6))

    fig.tight_layout()
    _save(fig, out_dir, "fig4_bandwidth_timeline.png")


# ═══════════════════════════════════════════════════════════════════════════════
#  Figure 5 — GC Job Duration Distribution
# ═══════════════════════════════════════════════════════════════════════════════

def plot_duration_dist(breakdowns, out_dir: str):
    """Histogram of wall-clock GC job durations with P50 / P99 / P99.9 markers."""
    durations = [_us_to_sec(r["run_micros"]) * 1000
                 for r in breakdowns if r.get("run_micros", 0) > 0]
    if not durations:
        print("  [skip] no run_micros data")
        return

    fig, ax = plt.subplots(figsize=(4.8, 2.5))

    bins = min(35, max(8, len(durations) // 3))
    ax.hist(durations, bins=bins, color=PALETTE["scan"],
            edgecolor="white", linewidth=0.5, alpha=0.88)

    p50  = np.percentile(durations, 50)
    p99  = np.percentile(durations, 99)
    p999 = np.percentile(durations, 99.9)

    markers = [
        (p50,  "P50",   PALETTE["relocation_write"]),
        (p99,  "P99",   PALETTE["ksst_read"]),
        (p999, "P99.9", PALETTE["invalid_read"]),
    ]
    for val, label, color in markers:
        ax.axvline(val, color=color, linestyle="--", linewidth=1.0,
                   label=f"{label} = {val:.0f} ms")

    ax.set_xlabel("Job duration (ms)")
    ax.set_ylabel("Count")
    ax.legend(fontsize=7, handlelength=1.2, borderpad=0.3)
    ax.yaxis.set_major_locator(mticker.MaxNLocator(5))

    fig.tight_layout()
    _save(fig, out_dir, "fig5_duration_distribution.png")


# ═══════════════════════════════════════════════════════════════════════════════
#  Figure 6 — Block Skippability by Job  (bonus diagnostic)
# ═══════════════════════════════════════════════════════════════════════════════

def plot_skippability_by_job(blockdists, out_dir: str):
    """Per-job skippable ratio to show variance across GC invocations."""
    if not blockdists:
        print("  [skip] no BLOCK_DIST records")
        return

    jobs  = [r["job"] for r in blockdists]
    srats = [r["skippable"] for r in blockdists]

    fig, ax = plt.subplots(figsize=(5.8, 2.5))

    colors = [PALETTE["relocation_write"] if s >= 50 else
              PALETTE["invalid_read"] for s in srats]
    ax.bar(range(len(srats)), srats, color=colors, edgecolor="white",
           linewidth=0.3, alpha=0.88)

    mean_s = np.mean(srats)
    ax.axhline(mean_s, color="#333333", linestyle="--", linewidth=0.9,
               label=f"Mean = {mean_s:.1f}%")

    ax.set_xlabel("GC job id")
    ax.set_ylabel("Skippable block ratio (%)")
    ax.set_ylim(0, 105)
    ax.legend(fontsize=7)
    ax.yaxis.set_major_locator(mticker.MaxNLocator(6))

    fig.tight_layout()
    _save(fig, out_dir, "fig6_skippability_by_job.png")


# ═══════════════════════════════════════════════════════════════════════════════
#  Summary statistics (terminal)
# ═══════════════════════════════════════════════════════════════════════════════

def print_summary(breakdowns, blockdists):
    """Print a compact statistics block to the terminal."""
    sep = "─" * 60
    print(f"\n{sep}")
    print("  Blob GC Instrumentation Summary")
    print(sep)

    if breakdowns:
        n = len(breakdowns)
        total_vsst    = sum(r["vsst_read"]        for r in breakdowns)
        total_ksst    = sum(r["ksst_read"]         for r in breakdowns)
        total_invalid = sum(r["invalid_read"]      for r in breakdowns)
        total_write   = sum(r["relocation_write"]  for r in breakdowns)
        total_read    = total_vsst + total_ksst + total_invalid

        print(f"  GC jobs analyzed               : {n}")
        print(f"  vSST reads (total)             : {_bytes_to_mb(total_vsst):,.1f} MB")
        print(f"  kSST lookups (total)           : {_bytes_to_mb(total_ksst):,.1f} MB")
        print(f"  Wasted dead-KV reads (total)   : {_bytes_to_mb(total_invalid):,.1f} MB")
        print(f"  Relocation writes (total)      : {_bytes_to_mb(total_write):,.1f} MB")
        if total_read > 0:
            waste_pct = (total_invalid + total_ksst) / total_read * 100
            print(f"  ── I/O waste ratio             : {waste_pct:.1f}%  "
                  f"(dead-KV + kSST lookups) / total reads")
        ok_jobs = [r for r in breakdowns if r.get("run_micros", 0) > 0]
        if ok_jobs:
            dur_ms = [_us_to_sec(r["run_micros"]) * 1000 for r in ok_jobs]
            print(f"  P50 job duration               : {np.percentile(dur_ms, 50):,.0f} ms")
            print(f"  P99 job duration               : {np.percentile(dur_ms, 99):,.0f} ms")
            print(f"  P99.9 job duration             : {np.percentile(dur_ms, 99.9):,.0f} ms")

    if blockdists:
        total_blocks  = sum(r["block_total"] for r in blockdists)
        total_skip    = sum(r["i100"]         for r in blockdists)
        avg_skippable = np.mean([r["skippable"] for r in blockdists])
        print(f"\n  Block-distribution jobs        : {len(blockdists)}")
        print(f"  Logical blocks tracked         : {total_blocks:,}")
        print(f"  100%-dead blocks               : {total_skip:,} "
              f"({total_skip / max(total_blocks, 1) * 100:.1f}%)")
        print(f"  Avg. skippable ratio           : {avg_skippable:.1f}%  "
              f"(upper bound of chunk-aware GC benefit)")

    print(f"{sep}\n")


# ═══════════════════════════════════════════════════════════════════════════════
#  Entry point
# ═══════════════════════════════════════════════════════════════════════════════

def main():
    import sys
    
    parser = argparse.ArgumentParser(
        description="TerarkDB Blob GC Log Visualizer — Publication-Ready for SIGMOD-style Papers",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )
    parser.add_argument("--log", required=True, help="Path to INFO_LOG file")
    parser.add_argument("--cf", default=None, help="Column family filter (default: all)")
    parser.add_argument("--out", default="./output", help="Output directory for figures")
    
    args = parser.parse_args()
    
    # Create output directory
    os.makedirs(args.out, exist_ok=True)
    
    # Parse log file
    print(f"Parsing log file: {args.log}")
    if args.cf:
        print(f"Filtering by column family: {args.cf}")
    
    try:
        breakdowns, blockdists = parse_log(args.log, args.cf)
    except FileNotFoundError:
        print(f"Error: Log file not found: {args.log}")
        sys.exit(1)
    except Exception as e:
        print(f"Error parsing log file: {e}")
        sys.exit(1)
    
    print(f"Found {len(breakdowns)} BREAKDOWN records, {len(blockdists)} BLOCK_DIST records")
    
    if not breakdowns and not blockdists:
        print("Error: No GC instrumentation records found in log file")
        print("Possible reasons:")
        print("  - GC instrumentation is disabled in TerarkDB")
        print("  - Column family filter doesn't match any records")
        print("  - Log file is from a different version of TerarkDB")
        sys.exit(1)
    
    # Print summary statistics
    print_summary(breakdowns, blockdists)
    
    # Generate figures
    print("\nGenerating figures...")
    plot_bandwidth_pie(breakdowns, args.out)
    plot_bandwidth_stacked_bars(breakdowns, args.out)
    plot_block_dist(blockdists, args.out)
    plot_phase_time(breakdowns, args.out)
    plot_bandwidth_over_time(breakdowns, args.out)
    plot_duration_dist(breakdowns, args.out)
    
    print(f"\n✓ All figures saved to: {args.out}/")
    print("  - fig1a_bandwidth_pie.{png,pdf}")
    print("  - fig1b_bandwidth_stacked_bars.{png,pdf}")
    print("  - fig2_block_distribution.{png,pdf}")
    print("  - fig3_phase_latency.{png,pdf}")
    print("  - fig4_bandwidth_timeline.{png,pdf}")
    print("  - fig5_duration_distribution.{png,pdf}")

if __name__ == "__main__":
    main()


if __name__ == "__main__":
    main()