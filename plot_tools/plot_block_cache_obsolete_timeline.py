#!/usr/bin/env python3
"""Plot block-cache obsolete residency from TerarkDB INFO LOG files."""

import argparse
import json
import os
import re


SAMPLE_TAG = "[BLOCK_CACHE_OBSOLETE_SAMPLE]"
EVENT_TAG = "[BLOCK_CACHE_OBSOLETE_EVENT]"
EVENT_LOG_TAG = "EVENT_LOG_v1"


def parse_kv(line):
    return dict(re.findall(r"([A-Za-z0-9_]+)=([^ \n]+)", line))


def read_logs(paths):
    samples = []
    events = []
    job_events = []
    for path in paths:
        with open(path, "r", encoding="utf-8", errors="ignore") as f:
            for line in f:
                if SAMPLE_TAG in line:
                    kv = parse_kv(line)
                    if "ts_us" in kv and "obsolete_byte_ratio" in kv:
                        samples.append({
                            "ts_us": int(kv["ts_us"]),
                            "obsolete_byte_ratio": float(kv["obsolete_byte_ratio"]),
                            "mean_obsolete_byte_ratio": float(
                                kv.get("mean_obsolete_byte_ratio", "0")),
                            "peak_obsolete_byte_ratio": float(
                                kv.get("peak_obsolete_byte_ratio", "0")),
                            "tracked_blocks": int(kv.get("tracked_blocks", "0")),
                            "obsolete_blocks": int(kv.get("obsolete_blocks", "0")),
                            "tracked_bytes": int(kv.get("tracked_bytes", "0")),
                            "obsolete_bytes": int(kv.get("obsolete_bytes", "0")),
                        })
                elif EVENT_TAG in line:
                    kv = parse_kv(line)
                    if "ts_us" in kv:
                        events.append({
                            "ts_us": int(kv["ts_us"]),
                            "job": int(kv.get("job", "0")),
                            "reason": kv.get("reason", "unknown"),
                        })
                elif EVENT_LOG_TAG in line:
                    event = parse_event_log(line)
                    if event is not None:
                        job_events.append(event)
    samples.sort(key=lambda row: row["ts_us"])
    events.sort(key=lambda row: row["ts_us"])
    job_events.sort(key=lambda row: row["ts_us"])
    return samples, events, job_events


def parse_event_log(line):
    pos = line.find(EVENT_LOG_TAG)
    if pos < 0:
        return None
    payload = line[pos + len(EVENT_LOG_TAG):].strip()
    try:
        obj = json.loads(payload)
    except ValueError:
        return None
    event = obj.get("event")
    if event not in ("compaction_started", "compaction_finished"):
        return None
    if "time_micros" not in obj or "job" not in obj:
        return None
    return {
        "ts_us": int(obj["time_micros"]),
        "job": int(obj["job"]),
        "event": event,
        "reason": obj.get("compaction_reason", "unknown"),
    }


def require_plotting():
    import matplotlib.pyplot as plt
    return plt


def ensure_dir(path):
    os.makedirs(path, exist_ok=True)


def rel_minutes(rows, start_us):
    return [(row["ts_us"] - start_us) / 60_000_000.0 for row in rows]


def build_job_intervals(job_events, obsolete_events, start_us, end_us):
    gc_jobs = {event["job"] for event in obsolete_events
               if event.get("reason") == "gc" and event.get("job", 0) != 0}
    active = {}
    intervals = []
    for event in job_events:
        job = event["job"]
        if event["event"] == "compaction_started":
            active[job] = event
        elif event["event"] == "compaction_finished" and job in active:
            begin = active.pop(job)
            kind = "gc" if job in gc_jobs else "compaction"
            intervals.append({
                "start_us": begin["ts_us"],
                "end_us": max(begin["ts_us"], event["ts_us"]),
                "kind": kind,
            })
    for begin in active.values():
        kind = "gc" if begin["job"] in gc_jobs else "compaction"
        intervals.append({
            "start_us": begin["ts_us"],
            "end_us": end_us,
            "kind": kind,
        })
    return intervals


def concurrency_at_samples(samples, intervals):
    compaction = []
    gc = []
    for sample in samples:
        ts_us = sample["ts_us"]
        compaction_count = 0
        gc_count = 0
        for interval in intervals:
            if interval["start_us"] <= ts_us <= interval["end_us"]:
                if interval["kind"] == "gc":
                    gc_count += 1
                else:
                    compaction_count += 1
        compaction.append(compaction_count)
        gc.append(gc_count)
    return compaction, gc


def apply_style(plt):
    plt.rcParams.update({
        "font.family": "DejaVu Sans",
        "font.size": 11,
        "axes.labelsize": 12,
        "axes.titlesize": 12,
        "legend.fontsize": 10,
        "figure.figsize": (7.2, 3.2),
        "axes.spines.top": False,
        "axes.spines.right": False,
        "pdf.fonttype": 42,
        "ps.fonttype": 42,
    })


def save(fig, out_dir, name):
    ensure_dir(out_dir)
    fig.tight_layout()
    fig.savefig(os.path.join(out_dir, name + ".pdf"), bbox_inches="tight")
    fig.savefig(os.path.join(out_dir, name + ".png"), dpi=300,
                bbox_inches="tight")


def plot_timeline(samples, events, job_events, out_dir):
    plt = require_plotting()
    apply_style(plt)
    all_ts = [samples[0]["ts_us"], samples[-1]["ts_us"]]
    all_ts.extend(e["ts_us"] for e in events)
    all_ts.extend(e["ts_us"] for e in job_events)
    start_us = min(all_ts)
    end_us = max(all_ts)

    xs = rel_minutes(samples, start_us)
    obsolete_blocks = [row["obsolete_blocks"] for row in samples]
    intervals = build_job_intervals(job_events, events, start_us, end_us)
    compaction_concurrency, gc_concurrency = concurrency_at_samples(samples,
                                                                    intervals)
    total_concurrency = [
        c + g for c, g in zip(compaction_concurrency, gc_concurrency)]

    fig, ax1 = plt.subplots()
    bar_width = infer_bar_width(xs)
    ax1.bar(xs, obsolete_blocks, width=bar_width, color="#4c78a8", alpha=0.72,
            label="obsolete blocks")
    ax1.set_xlabel("Time since start (min)")
    ax1.set_ylabel("Obsolete blocks in block cache")
    ax1.set_ylim(0, max(1.0, max(obsolete_blocks) * 1.12))
    ax1.grid(True, axis="y", linestyle=":", linewidth=0.8, alpha=0.7)

    ax2 = ax1.twinx()
    ax2.plot(xs, total_concurrency, color="#e45756", linewidth=2.0,
             label="compaction+GC concurrency")
    if any(gc_concurrency):
        ax2.plot(xs, gc_concurrency, color="#54a24b", linewidth=1.4,
                 linestyle="--", label="GC concurrency")
    ax2.set_ylabel("Concurrent compaction/GC jobs")
    ax2.set_ylim(0, max(1.0, max(total_concurrency + [0]) * 1.18))
    ax2.spines["right"].set_visible(True)

    lines1, labels1 = ax1.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    ax1.legend(lines1 + lines2, labels1 + labels2, loc="upper left",
               frameon=False, ncol=2)
    save(fig, out_dir, "block_cache_obsolete_blocks_with_job_concurrency")
    plt.close(fig)


def infer_bar_width(xs):
    if len(xs) < 2:
        return 0.12
    gaps = [xs[i] - xs[i - 1] for i in range(1, len(xs)) if xs[i] > xs[i - 1]]
    if not gaps:
        return 0.12
    return max(min(gaps) * 0.82, 0.02)


def main():
    parser = argparse.ArgumentParser(
        description="Draw SIGMOD/FAST-style obsolete block-cache residency "
                    "figures from TerarkDB INFO LOG files.")
    parser.add_argument("logs", nargs="+", help="INFO LOG files")
    parser.add_argument("--out", default="./block_cache_obsolete_figures",
                        help="Output directory")
    args = parser.parse_args()

    samples, events, job_events = read_logs(args.logs)
    if not samples:
        raise SystemExit("no BLOCK_CACHE_OBSOLETE_SAMPLE lines found")
    plot_timeline(samples, events, job_events, args.out)
    print("wrote figures to {}".format(args.out))


if __name__ == "__main__":
    main()
