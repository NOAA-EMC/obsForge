#!/usr/bin/env python3
import os
import re
import csv
import matplotlib.pyplot as plt
from datetime import datetime, timedelta
import statistics
from collections import defaultdict

# ---------- CONFIG ----------
BASE_DIR = "/work2/noaa/da/mchoi3/temp/come_back/COMROOT/realtime/logs"  # e.g., .../logs
TARGET_LOG = "gdas_marine_dump_prep.log"  # log file name inside each cycle dir
# START_CYCLE = "2025090700"  # YYYYMMDDHH (inclusive)
# END_CYCLE   = "2025092918"  # YYYYMMDDHH (inclusive)
START_CYCLE = "2025102700"  # YYYYMMDDHH (inclusive)
END_CYCLE   = "2025102900"  # YYYYMMDDHH (inclusive)
CYCLE_INTERVAL_HR = 6
OUT_CSV_RAW = "obs_space_counts.csv"
OUT_CSV_SUM_SPACE = "obs_space_counts_summary.csv"
OUT_CSV_SUM_GROUP = "obs_space_counts_group_summary.csv"

# Group definitions (same as before)
GROUPED_OBS_SPACES = {
    "sst": [
        "sst_viirs_n21_l3u", "sst_viirs_n20_l3u", "sst_viirs_npp_l3u",
        "sst_avhrrf_mb_l3u", "sst_avhrrf_mc_l3u", "sst_ahi_h09_l3c",
        "sst_abi_g18_l3c",
    ],
    "rads_adt": [
        "rads_adt_3a", "rads_adt_3b", "rads_adt_6a", "rads_adt_c2",
        "rads_adt_j2", "rads_adt_j3", "rads_adt_sa", "rads_adt_sw",
    ],
    "icec_amsr2": ["icec_amsr2_north", "icec_amsr2_south"],
    "sss": ["sss_smap_l2", "sss_smos_l2"],
}

# ---------- PARSING HELPERS ----------
APPENDING_RE = re.compile(r'^\s*appending:\s+([^/\s]+)/', re.IGNORECASE)
COUNT_RE     = re.compile(r'^\s*obs count:\s*([0-9]+)\s*$', re.IGNORECASE)
IODA_RE      = re.compile(r'^\s*Writing ioda file', re.IGNORECASE)

def parse_counts_from_log(filepath, dt):
    """
    Parse a log for blocks like:
      appending: <obs_space>/...
      obs count: <N>
      Writing ioda file
    Returns list of tuples: (dt, obs_space, count)
    """
    out = []
    try:
        with open(filepath, 'r') as f:
            lines = f.readlines()
    except FileNotFoundError:
        return out

    # Scan sequentially; when we see "Writing ioda file", grab the two previous lines.
    for i, line in enumerate(lines):
        if IODA_RE.search(line):
            # Try to read the two lines immediately before
            obs_space = None
            count_val = None

            # Count line (usually i-1)
            if i - 1 >= 0:
                m_cnt = COUNT_RE.search(lines[i - 1])
                if m_cnt:
                    count_val = int(m_cnt.group(1))

            # Appending line (usually i-2)
            if i - 2 >= 0:
                m_app = APPENDING_RE.search(lines[i - 2])
                if m_app:
                    obs_space = m_app.group(1)

            # Fallbacks: search upward a short distance if strict -1/-2 misses
            if obs_space is None or count_val is None:
                for j in range(max(0, i - 6), i):
                    if obs_space is None:
                        m_app = APPENDING_RE.search(lines[j])
                        if m_app:
                            obs_space = m_app.group(1)
                    if count_val is None:
                        m_cnt = COUNT_RE.search(lines[j])
                        if m_cnt:
                            count_val = int(m_cnt.group(1))
                    if obs_space is not None and count_val is not None:
                        break

            if obs_space is not None and count_val is not None:
                out.append((dt, obs_space, count_val))
    print(f'parse_counts_from_log: {filepath}:')
    print(f'parse_counts_from_log: {out}:')
    return out



def scan_log_range(base_dir, start_dt, end_dt, interval_hr, target_log):
    results = []
    dt = start_dt
    while dt <= end_dt:
        cyc_str = dt.strftime("%Y%m%d%H")
        log_path = os.path.join(base_dir, cyc_str, target_log)
        parsed = parse_counts_from_log(log_path, dt)
        results.extend(parsed)
        dt += timedelta(hours=interval_hr)
    return results

# ---------- CSV WRITERS ----------
def save_counts_csv(data, csvfile):
    # data: list of (dt, obs_space, count)
    with open(csvfile, 'w', newline='') as f:
        w = csv.writer(f)
        w.writerow(["datetime", "obs_space", "obs_count"])
        for dt, space, cnt in data:
            w.writerow([dt.strftime("%Y-%m-%d %H:%M"), space, cnt])

def compute_stats_counts(data, grouped_obs_spaces):
    # Per obs_space
    by_space = defaultdict(list)
    for dt, space, cnt in data:
        by_space[space].append(cnt)

    space_summary = []
    for space, vals in by_space.items():
        n = len(vals)
        mean_v = statistics.mean(vals) if n else float("nan")
        stdev_v = statistics.stdev(vals) if n >= 2 else 0.0 if n == 1 else float("nan")
        min_v = min(vals) if n else float("nan")
        max_v = max(vals) if n else float("nan")
        space_summary.append([space, n, mean_v, stdev_v, min_v, max_v])

    # Per group (aggregate all member obs_spaces)
    group_summary = []
    for group, members in grouped_obs_spaces.items():
        agg = []
        for dt, space, cnt in data:
            if space in members:
                agg.append(cnt)
        n = len(agg)
        mean_v = statistics.mean(agg) if n else float("nan")
        stdev_v = statistics.stdev(agg) if n >= 2 else 0.0 if n == 1 else float("nan")
        min_v = min(agg) if n else float("nan")
        max_v = max(agg) if n else float("nan")
        group_summary.append([group, n, mean_v, stdev_v, min_v, max_v])

    return space_summary, group_summary

def save_space_summary_csv(space_summary, filename):
    with open(filename, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["obs_space", "count(n)", "mean_count", "stdev_count", "min_count", "max_count"])
        for row in space_summary:
            w.writerow(row)

def save_group_summary_csv(group_summary, filename):
    with open(filename, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["group", "count(n)", "mean_count", "stdev_count", "min_count", "max_count"])
        for row in group_summary:
            w.writerow(row)

# ---------- PLOTTING ----------
def plot_grouped_counts(data, grouped_obs_spaces):
    # Organize by obs_space → time-ordered series
    by_space = defaultdict(list)
    for dt, space, cnt in data:
        by_space[space].append((dt, cnt))
    for space in by_space:
        by_space[space].sort(key=lambda x: x[0])

    for group, members in grouped_obs_spaces.items():
        series = {m: by_space[m] for m in members if m in by_space and by_space[m]}
        if not series:
            continue

        # ~1400×600 px
        plt.figure(figsize=(14, 6), dpi=100)

        for space, entries in series.items():
            xs, ys = zip(*entries)
            plt.plot(xs, ys, marker='o', linestyle='-', label=space)

            n = len(ys)
            if n > 0:
                mean_v = statistics.mean(ys)
                stdev_v = statistics.stdev(ys) if n >= 2 else 0.0

                # mean line over this series window
                plt.hlines(mean_v, xmin=xs[0], xmax=xs[-1],
                           linestyles='dashed', linewidth=1.1, label=f"{space} mean")
                if stdev_v > 0:
                    plt.fill_between(
                        xs,
                        [mean_v - stdev_v] * len(xs),
                        [mean_v + stdev_v] * len(xs),
                        alpha=0.12, label=f"{space} ±1σ"
                    )

        plt.title(f"Obs Count Time Series by obs_space: {group}")
        plt.xlabel("Date/Time (UTC)")
        plt.ylabel("Observations (count)")
        plt.grid(True)
        plt.xticks(rotation=45)
        plt.legend()
        plt.tight_layout()
        out_png = f"{group}_obs_count_timeseries.png"
        plt.savefig(out_png)
        plt.close()
        print(f"Plot saved: {out_png}")



if __name__ == "__main__":
    start_dt = datetime.strptime(START_CYCLE, "%Y%m%d%H")
    end_dt   = datetime.strptime(END_CYCLE,   "%Y%m%d%H")

    # Scan logs and extract counts around "Writing ioda file"
    results = scan_log_range(BASE_DIR, start_dt, end_dt, CYCLE_INTERVAL_HR, TARGET_LOG)

    # Save raw time series
    save_to = OUT_CSV_RAW
    save_counts_csv(results, save_to)
    print(f"Saved {len(results)} records to {save_to}")

    # Compute summaries
    space_summary, group_summary = compute_stats_counts(results, GROUPED_OBS_SPACES)
    save_space_summary_csv(space_summary, OUT_CSV_SUM_SPACE)
    save_group_summary_csv(group_summary, OUT_CSV_SUM_GROUP)
    print(f"Summary CSV saved: {OUT_CSV_SUM_SPACE}")
    print(f"Group summary CSV saved: {OUT_CSV_SUM_GROUP}")

    # Console print
    print("\nPer-obs_space summary (n, mean, stdev, min, max):")
    for s, n, mu, sd, mn, mx in sorted(space_summary):
        print(f"  {s:20s} n={n:3d}  mean={mu:.2f}  stdev={sd:.2f}  min={mn:.0f}  max={mx:.0f}")

    print("\nPer-group summary (n, mean, stdev, min, max):")
    for g, n, mu, sd, mn, mx in sorted(group_summary):
        print(f"  {g:12s} n={n:3d}  mean={mu:.2f}  stdev={sd:.2f}  min={mn:.0f}  max={mx:.0f}")

    # Plots
    plot_grouped_counts(results, GROUPED_OBS_SPACES)

