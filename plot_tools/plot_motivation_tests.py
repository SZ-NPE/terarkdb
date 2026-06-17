#!/usr/bin/env python3
"""
Publication-style visualizer for two motivation experiments.

The script is intentionally CSV-driven. It does not assume a fixed benchmark
runner, so it can be used with db_bench, rocksdb_kvbench, terarkdb_kvbench, or
manually aggregated counters.
"""

import argparse
import csv
import math
import os
from collections import defaultdict

plt = None
np = None


def setup_matplotlib():
    global plt, np
    try:
        import matplotlib

        matplotlib.use("Agg")
        import matplotlib.pyplot as _plt
        import numpy as _np
    except ImportError as e:
        raise SystemExit(
            "Missing plotting dependency. Install with: "
            "python3 -m pip install --user matplotlib numpy"
        ) from e

    plt = _plt
    np = _np
    plt.rcParams.update({
        "font.family": "serif",
        "font.serif": ["Times New Roman", "DejaVu Serif", "Liberation Serif"],
        "font.size": 9,
        "axes.titlesize": 10,
        "axes.labelsize": 9,
        "legend.fontsize": 7.5,
        "xtick.labelsize": 8,
        "ytick.labelsize": 8,
        "figure.dpi": 150,
        "savefig.dpi": 300,
        "savefig.bbox": "tight",
        "savefig.pad_inches": 0.04,
        "axes.linewidth": 0.8,
        "axes.spines.top": False,
        "axes.spines.right": False,
        "axes.grid": True,
        "grid.alpha": 0.22,
        "grid.linewidth": 0.5,
        "lines.linewidth": 1.3,
        "lines.markersize": 4,
        "legend.frameon": True,
        "legend.framealpha": 0.92,
        "legend.edgecolor": "#cccccc",
        "legend.fancybox": False,
    })


PALETTE = {
    "baseline": "#4C72B0",
    "lru": "#4C72B0",
    "rocksdb": "#4C72B0",
    "gc_aware": "#C44E52",
    "terarkdb": "#C44E52",
    "ours": "#C44E52",
    "dead_read": "#C44E52",
    "live_read": "#4C72B0",
    "relocation_write": "#55A868",
    "other": "#8172B2",
}
MARKERS = ["o", "s", "^", "D", "v", "P", "X"]


ALIASES = {
    "throughput_ops": ["throughput_ops", "ops_sec", "ops_per_sec", "qps"],
    "p99_us": ["p99_us", "p99", "p99_latency_us"],
    "cache_hit_rate": ["cache_hit_rate", "hit_rate", "block_cache_hit_rate"],
    "live_read_mb": ["live_read_mb", "valid_read_mb", "vsst_read_mb"],
    "dead_read_mb": ["dead_read_mb", "invalid_read_mb", "wasted_read_mb"],
    "relocation_write_mb": ["relocation_write_mb", "write_mb", "gc_write_mb"],
    "skippable_ratio_pct": ["skippable_ratio_pct", "skippable_pct"],
    "gc_time_sec": ["gc_time_sec", "gc_sec", "gc_duration_sec"],
    "gc_demote": ["gc_demote", "demote"],
    "gc_low_score_evict": ["gc_low_score_evict", "low_score_evict"],
}


def canonicalize(row):
    out = {k.strip(): v.strip() for k, v in row.items() if k is not None}
    lower = {k.lower(): k for k in out}
    for canonical, aliases in ALIASES.items():
        if canonical in out:
            continue
        for alias in aliases:
            if alias.lower() in lower:
                out[canonical] = out[lower[alias.lower()]]
                break
    return out


def read_csv(path):
    with open(path, "r", newline="") as f:
        return [canonicalize(r) for r in csv.DictReader(f)]


def fnum(row, key, default=math.nan):
    try:
        value = row.get(key, "")
        if value == "":
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def mean(xs):
    vals = [x for x in xs if not math.isnan(x)]
    return sum(vals) / len(vals) if vals else math.nan


def stderr(xs):
    vals = [x for x in xs if not math.isnan(x)]
    if len(vals) <= 1:
        return 0.0
    mu = mean(vals)
    var = sum((x - mu) ** 2 for x in vals) / (len(vals) - 1)
    return math.sqrt(var) / math.sqrt(len(vals))


def group_values(rows, system_col, x_col, metric):
    groups = defaultdict(list)
    for r in rows:
        if system_col not in r or x_col not in r:
            continue
        value = fnum(r, metric)
        if not math.isnan(value):
            groups[(r[system_col], r[x_col])].append(value)
    return groups


def sorted_xs(rows, x_col):
    xs = sorted({r[x_col] for r in rows if x_col in r}, key=_sort_key)
    return xs


def _sort_key(x):
    try:
        return (0, float(x))
    except ValueError:
        return (1, x)


def save(fig, out_dir, name):
    os.makedirs(out_dir, exist_ok=True)
    for ext in ("pdf", "png"):
        path = os.path.join(out_dir, f"{name}.{ext}")
        fig.savefig(path, dpi=300, bbox_inches="tight", pad_inches=0.04)
    plt.close(fig)
    print(f"[saved] {os.path.join(out_dir, name)}.{{pdf,png}}")


def systems(rows, system_col):
    return sorted({r.get(system_col, "") for r in rows if r.get(system_col, "")})


def color_for(system):
    key = system.lower()
    return PALETTE.get(key, None)


def plot_gc_waste_stack(rows, out_dir, system_col, x_col):
    required = ("live_read_mb", "dead_read_mb", "relocation_write_mb")
    if not all(any(k in r for r in rows) for k in required):
        print("[skip] motivation1 stack: missing live/dead/write MB columns")
        return

    groups = defaultdict(lambda: defaultdict(list))
    for r in rows:
        if system_col not in r or x_col not in r:
            continue
        for metric in required:
            groups[(r[system_col], r[x_col])][metric].append(fnum(r, metric, 0.0))

    keys = sorted(groups.keys(), key=lambda k: (_sort_key(k[1]), k[0]))
    x = np.arange(len(keys))
    labels = [f"{k[0]}\n{k[1]}" for k in keys]

    fig, ax = plt.subplots(figsize=(max(4.8, 0.42 * len(keys)), 2.7))
    bottom = np.zeros(len(keys))
    series = [
        ("live_read_mb", "Live reads", PALETTE["live_read"]),
        ("dead_read_mb", "Dead reads", PALETTE["dead_read"]),
        ("relocation_write_mb", "Relocation writes", PALETTE["relocation_write"]),
    ]
    for metric, label, color in series:
        vals = np.array([mean(groups[k][metric]) for k in keys])
        ax.bar(x, vals, bottom=bottom, width=0.72, color=color,
               edgecolor="white", linewidth=0.5, label=label)
        bottom += vals

    ax.set_ylabel("GC I/O volume (MiB)")
    ax.set_xlabel(x_col)
    ax.set_xticks(x)
    ax.set_xticklabels(labels, rotation=0)
    ax.legend(ncol=3, loc="upper center", bbox_to_anchor=(0.5, 1.22))
    ax.set_title("Motivation 1: GC reads substantial dead data")
    save(fig, out_dir, "motivation1_gc_io_breakdown")


def plot_metric_by_x(rows, out_dir, system_col, x_col, metric, ylabel, name,
                     title, higher_is_better=True):
    if not any(metric in r for r in rows):
        print(f"[skip] {name}: missing column {metric}")
        return
    xs = sorted_xs(rows, x_col)
    sys = systems(rows, system_col)
    fig, ax = plt.subplots(figsize=(4.8, 2.6))
    for idx, s in enumerate(sys):
        groups = group_values([r for r in rows if r.get(system_col) == s],
                              system_col, x_col, metric)
        ys = [mean(groups.get((s, x), [])) for x in xs]
        es = [stderr(groups.get((s, x), [])) for x in xs]
        ax.errorbar(range(len(xs)), ys, yerr=es, marker=MARKERS[idx % len(MARKERS)],
                    color=color_for(s), capsize=2.2, label=s)
    ax.set_xticks(range(len(xs)))
    ax.set_xticklabels(xs)
    ax.set_xlabel(x_col)
    ax.set_ylabel(ylabel)
    ax.set_title(title)
    ax.legend(loc="best")
    if not higher_is_better:
        ax.annotate("lower is better", xy=(0.99, 0.03), xycoords="axes fraction",
                    ha="right", va="bottom", fontsize=7, color="#555555")
    save(fig, out_dir, name)


def plot_cache_events(rows, out_dir, system_col, x_col):
    metrics = ["gc_demote", "gc_low_score_evict"]
    if not all(any(m in r for r in rows) for m in metrics):
        print("[skip] motivation2 events: missing gc_demote/gc_low_score_evict")
        return
    xs = sorted_xs(rows, x_col)
    sys = systems(rows, system_col)
    width = 0.8 / max(len(sys), 1)
    fig, ax = plt.subplots(figsize=(5.0, 2.7))
    for i, s in enumerate(sys):
        positions = np.arange(len(xs)) - 0.4 + width / 2 + i * width
        demote = []
        evict = []
        subset = [r for r in rows if r.get(system_col) == s]
        for x in xs:
            demote.append(mean([fnum(r, "gc_demote") for r in subset
                                if r.get(x_col) == x]))
            evict.append(mean([fnum(r, "gc_low_score_evict") for r in subset
                               if r.get(x_col) == x]))
        ax.bar(positions, demote, width=width, color="#8172B2",
               edgecolor="white", linewidth=0.4,
               label="demote" if i == 0 else None)
        ax.bar(positions, evict, width=width, bottom=demote, color="#C44E52",
               edgecolor="white", linewidth=0.4,
               label="low-score evict" if i == 0 else None)
    ax.set_xticks(range(len(xs)))
    ax.set_xticklabels(xs)
    ax.set_xlabel(x_col)
    ax.set_ylabel("GC-aware cache events")
    ax.set_title("Motivation 2: garbage-aware policy takes effect under pressure")
    ax.legend(loc="best")
    save(fig, out_dir, "motivation2_gc_aware_events")


def emit_templates(out_dir):
    os.makedirs(out_dir, exist_ok=True)
    m1 = os.path.join(out_dir, "motivation1_template.csv")
    m2 = os.path.join(out_dir, "motivation2_template.csv")
    with open(m1, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["run", "system", "garbage_ratio_pct", "live_read_mb",
                    "dead_read_mb", "relocation_write_mb",
                    "skippable_ratio_pct", "gc_time_sec"])
        w.writerow([1, "baseline", 20, 1024, 420, 180, 18.2, 11.4])
        w.writerow([1, "gc_aware", 20, 840, 210, 160, 18.2, 8.7])
    with open(m2, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["run", "system", "cache_size_gb", "cache_hit_rate",
                    "throughput_ops", "p99_us", "gc_demote",
                    "gc_low_score_evict"])
        w.writerow([1, "baseline", 1, 0.61, 43000, 2400, 0, 0])
        w.writerow([1, "gc_aware", 1, 0.72, 51000, 1700, 12034, 4201])
    print(f"[template] {m1}")
    print(f"[template] {m2}")


def main():
    p = argparse.ArgumentParser(
        description="Draw SIGMOD/FAST-style figures for motivation tests.")
    p.add_argument("--motivation1", help="CSV for motivation test 1")
    p.add_argument("--motivation2", help="CSV for motivation test 2")
    p.add_argument("--out", default="./motivation_figures",
                   help="Output directory")
    p.add_argument("--system-col", default="system")
    p.add_argument("--x1", default="garbage_ratio_pct",
                   help="X-axis column for motivation1")
    p.add_argument("--x2", default="cache_size_gb",
                   help="X-axis column for motivation2")
    p.add_argument("--emit-template", action="store_true",
                   help="Write example CSV templates and exit")
    args = p.parse_args()

    if args.emit_template:
        emit_templates(args.out)
        return

    if not args.motivation1 and not args.motivation2:
        p.error("provide --motivation1 and/or --motivation2, or use --emit-template")

    setup_matplotlib()

    if args.motivation1:
        rows = read_csv(args.motivation1)
        plot_gc_waste_stack(rows, args.out, args.system_col, args.x1)
        plot_metric_by_x(rows, args.out, args.system_col, args.x1,
                         "skippable_ratio_pct", "Skippable blocks (%)",
                         "motivation1_skippable_ratio",
                         "Motivation 1: dead-block opportunity", True)
        plot_metric_by_x(rows, args.out, args.system_col, args.x1,
                         "gc_time_sec", "GC time (s)",
                         "motivation1_gc_time",
                         "Motivation 1: GC cost grows with dead data", False)

    if args.motivation2:
        rows = read_csv(args.motivation2)
        plot_metric_by_x(rows, args.out, args.system_col, args.x2,
                         "cache_hit_rate", "Block cache hit rate",
                         "motivation2_cache_hit_rate",
                         "Motivation 2: cache pollution under pressure", True)
        plot_metric_by_x(rows, args.out, args.system_col, args.x2,
                         "throughput_ops", "Throughput (ops/s)",
                         "motivation2_throughput",
                         "Motivation 2: performance sensitivity", True)
        plot_metric_by_x(rows, args.out, args.system_col, args.x2,
                         "p99_us", "P99 latency (us)",
                         "motivation2_p99_latency",
                         "Motivation 2: tail latency sensitivity", False)
        plot_cache_events(rows, args.out, args.system_col, args.x2)


if __name__ == "__main__":
    main()
