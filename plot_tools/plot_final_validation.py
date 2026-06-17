#!/usr/bin/env python3
"""
Publication-style visualizer for final benefit validation experiments.

Input is a CSV where each row is one run of one system on one workload. The
script aggregates repeated runs, normalizes metrics against a baseline, and
writes both figures and a summary CSV.
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
        "legend.frameon": True,
        "legend.framealpha": 0.92,
        "legend.edgecolor": "#cccccc",
        "legend.fancybox": False,
    })


COLORS = {
    "baseline": "#4C72B0",
    "lru": "#4C72B0",
    "rocksdb": "#4C72B0",
    "gc_aware": "#C44E52",
    "terarkdb": "#C44E52",
    "ours": "#C44E52",
}
FALLBACK = ["#4C72B0", "#C44E52", "#55A868", "#8172B2", "#DD8452", "#64B5CD"]
ALIASES = {
    "throughput_ops": ["throughput_ops", "ops_sec", "ops_per_sec", "qps"],
    "p50_us": ["p50_us", "p50", "p50_latency_us"],
    "p99_us": ["p99_us", "p99", "p99_latency_us"],
    "p999_us": ["p999_us", "p99_9_us", "p99.9_us", "p999"],
    "read_amp": ["read_amp", "read_amplification"],
    "write_amp": ["write_amp", "write_amplification"],
    "space_amp": ["space_amp", "space_amplification"],
    "cache_hit_rate": ["cache_hit_rate", "hit_rate", "block_cache_hit_rate"],
    "gc_time_sec": ["gc_time_sec", "gc_sec", "gc_duration_sec"],
    "gc_bytes_mb": ["gc_bytes_mb", "gc_io_mb"],
    "cpu_pct": ["cpu_pct", "cpu_percent"],
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


def systems(rows, system_col):
    return sorted({r[system_col] for r in rows if r.get(system_col)})


def workloads(rows, workload_col):
    return sorted({r[workload_col] for r in rows if r.get(workload_col)})


def color(system, idx):
    return COLORS.get(system.lower(), FALLBACK[idx % len(FALLBACK)])


def save(fig, out_dir, name):
    os.makedirs(out_dir, exist_ok=True)
    for ext in ("pdf", "png"):
        fig.savefig(os.path.join(out_dir, f"{name}.{ext}"), dpi=300,
                    bbox_inches="tight", pad_inches=0.04)
    plt.close(fig)
    print(f"[saved] {os.path.join(out_dir, name)}.{{pdf,png}}")


def metric_groups(rows, system_col, workload_col, metric):
    groups = defaultdict(list)
    for r in rows:
        if system_col not in r or workload_col not in r:
            continue
        v = fnum(r, metric)
        if not math.isnan(v):
            groups[(r[system_col], r[workload_col])].append(v)
    return groups


def baseline_means(groups, baseline, workloads_list):
    return {w: mean(groups.get((baseline, w), [])) for w in workloads_list}


def plot_normalized(rows, out_dir, system_col, workload_col, baseline,
                    metric, ylabel, name, title, higher_is_better=True):
    if not any(metric in r for r in rows):
        print(f"[skip] {name}: missing column {metric}")
        return

    ws = workloads(rows, workload_col)
    sys = [s for s in systems(rows, system_col) if s != baseline]
    if baseline not in systems(rows, system_col):
        raise SystemExit(f"baseline '{baseline}' not found in column {system_col}")

    groups = metric_groups(rows, system_col, workload_col, metric)
    base = baseline_means(groups, baseline, ws)
    width = 0.8 / max(len(sys), 1)
    fig, ax = plt.subplots(figsize=(max(4.8, 0.6 * len(ws)), 2.7))

    ax.axhline(1.0, color="#333333", linewidth=0.8, linestyle="--",
               label=f"{baseline}=1.0")
    for i, s in enumerate(sys):
        x = np.arange(len(ws)) - 0.4 + width / 2 + i * width
        vals = []
        errs = []
        for w in ws:
            b = base[w]
            raw = groups.get((s, w), [])
            if not raw or math.isnan(b) or b == 0:
                vals.append(math.nan)
                errs.append(0.0)
            else:
                normalized = [(v / b) if higher_is_better else (b / v)
                              for v in raw if v != 0]
                vals.append(mean(normalized))
                errs.append(stderr(normalized))
        ax.bar(x, vals, width=width, yerr=errs, capsize=2.0,
               color=color(s, i), edgecolor="white", linewidth=0.5, label=s)

    ax.set_xticks(range(len(ws)))
    ax.set_xticklabels(ws, rotation=20, ha="right")
    ax.set_ylabel(ylabel)
    ax.set_title(title)
    ax.legend(loc="best")
    if not higher_is_better:
        ax.annotate("higher means lower raw value", xy=(0.99, 0.03),
                    xycoords="axes fraction", ha="right", va="bottom",
                    fontsize=7, color="#555555")
    save(fig, out_dir, name)


def plot_latency_absolute(rows, out_dir, system_col, workload_col):
    metrics = [m for m in ("p50_us", "p99_us", "p999_us")
               if any(m in r for r in rows)]
    if not metrics:
        print("[skip] latency absolute: no p50/p99/p999 columns")
        return
    ws = workloads(rows, workload_col)
    sys = systems(rows, system_col)
    fig, axes = plt.subplots(1, len(metrics), figsize=(3.2 * len(metrics), 2.7),
                             sharex=True)
    if len(metrics) == 1:
        axes = [axes]
    for ax, metric in zip(axes, metrics):
        groups = metric_groups(rows, system_col, workload_col, metric)
        width = 0.8 / max(len(sys), 1)
        for i, s in enumerate(sys):
            x = np.arange(len(ws)) - 0.4 + width / 2 + i * width
            vals = [mean(groups.get((s, w), [])) for w in ws]
            ax.bar(x, vals, width=width, color=color(s, i),
                   edgecolor="white", linewidth=0.5, label=s)
        ax.set_title(metric.replace("_", " ").upper())
        ax.set_yscale("log")
        ax.set_ylabel("Latency (us, log)")
        ax.set_xticks(range(len(ws)))
        ax.set_xticklabels(ws, rotation=25, ha="right")
    axes[0].legend(loc="best")
    save(fig, out_dir, "final_absolute_tail_latency")


def plot_resource_panel(rows, out_dir, system_col, workload_col, baseline):
    metrics = [m for m in ("read_amp", "write_amp", "space_amp", "gc_bytes_mb")
               if any(m in r for r in rows)]
    if not metrics:
        print("[skip] resource panel: no amplification/resource columns")
        return
    ws = workloads(rows, workload_col)
    sys = [s for s in systems(rows, system_col) if s != baseline]
    fig, axes = plt.subplots(1, len(metrics), figsize=(3.0 * len(metrics), 2.6),
                             sharey=True)
    if len(metrics) == 1:
        axes = [axes]
    for ax, metric in zip(axes, metrics):
        groups = metric_groups(rows, system_col, workload_col, metric)
        base = baseline_means(groups, baseline, ws)
        width = 0.8 / max(len(sys), 1)
        ax.axhline(1.0, color="#333333", linewidth=0.8, linestyle="--")
        for i, s in enumerate(sys):
            x = np.arange(len(ws)) - 0.4 + width / 2 + i * width
            vals = []
            for w in ws:
                b = base[w]
                v = mean(groups.get((s, w), []))
                vals.append(b / v if b and v and not math.isnan(v) else math.nan)
            ax.bar(x, vals, width=width, color=color(s, i),
                   edgecolor="white", linewidth=0.5, label=s)
        ax.set_title(metric.replace("_", " "))
        ax.set_xticks(range(len(ws)))
        ax.set_xticklabels(ws, rotation=25, ha="right")
    axes[0].set_ylabel("Improvement vs. baseline")
    axes[0].legend(loc="best")
    save(fig, out_dir, "final_resource_efficiency")


def plot_cache_hit(rows, out_dir, system_col, workload_col):
    if not any("cache_hit_rate" in r for r in rows):
        print("[skip] cache hit: missing cache_hit_rate")
        return
    ws = workloads(rows, workload_col)
    sys = systems(rows, system_col)
    groups = metric_groups(rows, system_col, workload_col, "cache_hit_rate")
    width = 0.8 / max(len(sys), 1)
    fig, ax = plt.subplots(figsize=(max(4.8, 0.6 * len(ws)), 2.7))
    for i, s in enumerate(sys):
        x = np.arange(len(ws)) - 0.4 + width / 2 + i * width
        vals = [mean(groups.get((s, w), [])) for w in ws]
        ax.bar(x, vals, width=width, color=color(s, i),
               edgecolor="white", linewidth=0.5, label=s)
    ax.set_xticks(range(len(ws)))
    ax.set_xticklabels(ws, rotation=20, ha="right")
    ax.set_ylabel("Block cache hit rate")
    ax.set_title("Final validation: cache efficiency")
    ax.legend(loc="best")
    save(fig, out_dir, "final_cache_hit_rate")


def write_summary(rows, out_dir, system_col, workload_col, baseline):
    metrics = ["throughput_ops", "p50_us", "p99_us", "p999_us", "read_amp",
               "write_amp", "space_amp", "cache_hit_rate", "gc_time_sec",
               "gc_bytes_mb", "cpu_pct"]
    metrics = [m for m in metrics if any(m in r for r in rows)]
    ws = workloads(rows, workload_col)
    sys = systems(rows, system_col)
    out = os.path.join(out_dir, "final_validation_summary.csv")
    os.makedirs(out_dir, exist_ok=True)
    with open(out, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["workload", "system", "metric", "mean", "stderr",
                    "baseline_mean", "ratio_to_baseline"])
        for metric in metrics:
            groups = metric_groups(rows, system_col, workload_col, metric)
            base = baseline_means(groups, baseline, ws)
            for workload in ws:
                for system in sys:
                    vals = groups.get((system, workload), [])
                    m = mean(vals)
                    b = base[workload]
                    ratio = m / b if b and not math.isnan(m) else math.nan
                    w.writerow([workload, system, metric, m, stderr(vals), b, ratio])
    print(f"[saved] {out}")


def emit_template(out_dir):
    os.makedirs(out_dir, exist_ok=True)
    path = os.path.join(out_dir, "final_validation_template.csv")
    with open(path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["run", "workload", "system", "throughput_ops", "p50_us",
                    "p99_us", "p999_us", "read_amp", "write_amp",
                    "space_amp", "cache_hit_rate", "gc_time_sec",
                    "gc_bytes_mb", "cpu_pct"])
        w.writerow([1, "overwrite-zipf", "baseline", 42000, 80, 2400, 9100,
                    1.00, 3.20, 1.28, 0.61, 120, 40960, 780])
        w.writerow([1, "overwrite-zipf", "gc_aware", 51000, 70, 1700, 6800,
                    0.78, 2.65, 1.18, 0.72, 93, 31500, 760])
    print(f"[template] {path}")


def main():
    p = argparse.ArgumentParser(
        description="Draw SIGMOD/FAST-style final validation figures.")
    p.add_argument("--input", help="Final validation CSV")
    p.add_argument("--out", default="./final_validation_figures")
    p.add_argument("--baseline", default="baseline")
    p.add_argument("--system-col", default="system")
    p.add_argument("--workload-col", default="workload")
    p.add_argument("--emit-template", action="store_true")
    args = p.parse_args()

    if args.emit_template:
        emit_template(args.out)
        return
    if not args.input:
        p.error("provide --input or use --emit-template")

    setup_matplotlib()

    rows = read_csv(args.input)
    write_summary(rows, args.out, args.system_col, args.workload_col,
                  args.baseline)
    plot_normalized(rows, args.out, args.system_col, args.workload_col,
                    args.baseline, "throughput_ops",
                    "Speedup over baseline",
                    "final_normalized_throughput",
                    "Final validation: throughput improvement", True)
    plot_normalized(rows, args.out, args.system_col, args.workload_col,
                    args.baseline, "p99_us",
                    "P99 improvement over baseline",
                    "final_normalized_p99",
                    "Final validation: tail-latency reduction", False)
    plot_latency_absolute(rows, args.out, args.system_col, args.workload_col)
    plot_resource_panel(rows, args.out, args.system_col, args.workload_col,
                        args.baseline)
    plot_cache_hit(rows, args.out, args.system_col, args.workload_col)


if __name__ == "__main__":
    main()
