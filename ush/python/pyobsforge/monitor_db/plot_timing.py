#!/usr/bin/env python3

import os
import re
import csv
import matplotlib.pyplot as plt
# import plotext as plt
from datetime import datetime, timedelta
import math
import statistics


def parse_time_elapsed(timestr: str) -> int:
    """HH:MM:SS -> seconds (int). Returns 0 on parse failure."""
    try:
        h, m, s = map(int, timestr.strip().split(":"))
        return h * 3600 + m * 60 + s
    except Exception:
        return 0


def extract_timing_from_log(log_path):
    pattern = re.compile(
        r"End marinebufrdump\.sh at .*? with error code (\d+) \(time elapsed: (\d{2}:\d{2}:\d{2})\)"
    )

    with open(log_path, "r") as f:
        lines = f.readlines()

    # Search backwards
    for line in reversed(lines):
        match = pattern.search(line)
        if match:
            error_code = int(match.group(1))
            if error_code == 0:
                elapsed = parse_time_elapsed(match.group(2))
            else:
                elapsed = 0  # present but failed run
            print(f"Elapsed for {log_path} = {elapsed}")
            return elapsed

    # If nothing matched
    print(f"No matching log entry found in {log_path}")
    return None


# ---- Compute stats (mean & stddev) per label, ignoring NaNs ----
def ComputeStats(results):
    summary_rows = []
    for label, series in results.items():
        valid = [v for v in series if not math.isnan(v)]
        count = len(valid)
        if count > 0:
            mean_val = statistics.mean(valid)
            # Use sample stdev if at least 2 points; otherwise 0.0
            std_val  = statistics.stdev(valid) if count >= 2 else 0.0
            min_val  = min(valid)
            max_val  = max(valid)
        else:
            mean_val = float('nan')
            std_val  = float('nan')
            min_val  = float('nan')
            max_val  = float('nan')
        summary_rows.append([label, count, mean_val, std_val, min_val, max_val])
    return summary_rows


def generate_plot(results, timestamps):
    plt.figure(figsize=(14, 6))
    for label, data in results.items():
        # Plot the series
        plt.plot(timestamps, data, marker='o', linestyle='-', label=label)

        # Mean & +/- 1 std band (only if we have >=1 valid)
        valid = [v for v in data if not math.isnan(v)]
        if valid:
            mean_val = statistics.mean(valid)
            std_val  = statistics.stdev(valid) if len(valid) >= 2 else 0.0

            # Draw mean line across full x-axis span
            plt.hlines(mean_val, xmin=timestamps[0], xmax=timestamps[-1],
                       linestyles='dashed', linewidth=1.2, label=f"{label} mean")

            # Shaded +/- 1 std band if std > 0
            if std_val > 0:
                plt.fill_between(
                    timestamps,
                    [mean_val - std_val] * len(timestamps),
                    [mean_val + std_val] * len(timestamps),
                    alpha=0.12, label=f"{label} ±1σ"
                )

    plt.xlabel("Cycle Time (UTC)")
    plt.ylabel("Elapsed Time (seconds)")
    plt.title("Elapsed Time for gdas Marine Bufr Dump Jobs")
    plt.xticks(rotation=45)
    plt.grid(True)
    plt.legend()
    plt.tight_layout()
    plt.savefig("marine_bufr_dump_elapsed_combined_new.png")
    # plt.show()




def generate_plot_plotext(results, timestamps):
    plt.clf()                                      # clear previous figure

    # Auto-size to terminal
    terminal_width = os.get_terminal_size().columns
    plt.plot_size(width=min(terminal_width - 10, 140), height=30)

    # Build short HH:MM labels
    x_labels = []
    for ts in timestamps:
        if isinstance(ts, datetime):
            x_labels.append(ts.strftime("%H:%M"))
        else:
            x_labels.append(str(ts)[:5])           # fallback, e.g. "00:00"

    # THIS IS THE KEY LINE:
    plt.date_form("time")          # tell plotext we are using only time (HH:MM)

    # Reduce label crowding
    plt.xfrequency(2)             # show every 2nd label; change to 3 if still crowded

    colors = ['red', 'green', 'blue', 'yellow', 'magenta', 'cyan']

    for i, (label, data) in enumerate(results.items()):
        if len(data) != len(timestamps):
            print(f"Skipping {label}: length mismatch")
            continue

        valid = [v for v in data if not math.isnan(v)]
        if not valid:
            continue

        mean_val = statistics.mean(valid)
        std_val  = statistics.stdev(valid) if len(valid) >= 2 else 0.0

        # Main series
        plt.plot(x_labels, data,
                 marker='o',
                 label=label,
                 color=colors[i % len(colors)])

        # Mean dashed line
        plt.hline(mean_val, style='dashed', label=f"{label} mean")

        # ±1σ shaded band
        if std_val > 0:
            lower = [mean_val - std_val] * len(timestamps)
            upper = [mean_val + std_val] * len(timestamps)
            plt.fill_between(x_labels, lower, upper, alpha=0.2,
                             label=f"{label} ±1σ")

    plt.title("Elapsed Time for gdas Marine Bufr Dump Jobs")
    plt.xlabel("Cycle Time (UTC)")
    plt.ylabel("Elapsed Time (seconds)")
    plt.grid(True)

    plt.clt()      # clear terminal for clean output
    plt.show()


def save_results_csv(results, timestamps):
    # ---- Save per-cycle CSV ----
    with open("marine_burf_dump_elapsed_combined_new.csv", "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["Datetime"] + list(results.keys()))
        for i in range(len(timestamps)):
            row = [timestamps[i].strftime("%Y-%m-%d %H:%M:%S")]
            for label in results:
                val = results[label][i]
                row.append("" if math.isnan(val) else val)
            writer.writerow(row)


def save_summary_csv(summary_rows):
    # ---- Save summary CSV (mean/std/min/max, counts) ----
    with open("marine_bufr_dump_elapsed_summary_new.csv", "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["Label", "Count(valid)", "Mean(s)", "StdDev(s)", "Min(s)", "Max(s)"])
        for row in summary_rows:
            writer.writerow(row)


if __name__ == "__main__":

    # start_date_str = "20250907"   # YYYYMMDD
    # end_date_str   = "20250929"   # YYYYMMDD
    start_date_str = "20251027"   # YYYYMMDD
    end_date_str   = "20251029"   # YYYYMMDD
    base_dirs = {
        # "gfs": "/work2/noaa/da/mchoi3/temp/come_back/gfs/COMROOT/obsforge/logs",
        "gdas": "/work2/noaa/da/mchoi3/temp/come_back/COMROOT/realtime/logs",
    }
    cycles = ["00", "06", "12", "18"]
    log_filename_template = "{}_marine_bufr_dump_prep.log"
    # ============================

    start_date = datetime.strptime(start_date_str, "%Y%m%d")
    end_date   = datetime.strptime(end_date_str,   "%Y%m%d")

    # Results dict
    results = {label: [] for label in base_dirs}
    timestamps = []


    # collect resdults and timestamps
    current_date = start_date
    while current_date <= end_date:
        for hour in cycles:
            cycle_str = current_date.strftime("%Y%m%d") + hour
            cycle_dt  = datetime.strptime(cycle_str, "%Y%m%d%H")
            timestamps.append(cycle_dt)

            for label, base_dir in base_dirs.items():
                log_path = os.path.join(base_dir, cycle_str, log_filename_template.format(label))
                elapsed = float('nan')  # Default if missing

                if os.path.exists(log_path):
                    elapsed = extract_timing_from_log(log_path)
                else:
                    print(f"[WARN] Log missing: {log_path}")

                results[label].append(elapsed)
                # print(f'label = {label}')
        current_date += timedelta(days=1)


    generate_plot(results, timestamps)
    # generate_plot_plotext(results, timestamps)


    save_results_csv(results, timestamps)

    summary_rows = ComputeStats(results)

    save_summary_csv(summary_rows)

    # ---- Print summary to console ----
    print("Plot saved to marine_bufr_dump_elapsed_combined_new.png")
    print("CSV (per-cycle) saved to marine_burf_dump_elapsed_combined_new.csv")
    print("CSV (summary)   saved to marine_bufr_dump_elapsed_summary_new.csv\n")

    print("Summary (ignoring NaNs):")
    for row in summary_rows:
        label, count, mean_val, std_val, min_val, max_val = row
        print(f"  {label:10s} n={count:3d}  mean={mean_val:.2f}s  stdev={std_val:.2f}s  "
              f"min={min_val:.0f}s  max={max_val:.0f}s")

