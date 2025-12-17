import numpy as np
import matplotlib.pyplot as plt
from typing import Optional, List

# Local imports for data fetching
from monitor_db_util import (
    fetch_task_timings_for_plot,
    fetch_obs_count_for_space_for_plot,
    fetch_obs_count_by_category_for_plot
)


class MonitorPlotter:
    def __init__(self, db_instance):
        self.db = db_instance

    def plot_timings(self, task: Optional[str], days: Optional[int], output: Optional[str]):
        rows = fetch_task_timings_for_plot(self.db, days, task_name=task)
        if not rows:
            print("No data found for plotting.")
            return

        if days is None and len(rows) > 100:
            print("Warning: Plotting >100 points. Consider using --days N to filter.")

        plt.figure(figsize=(12, 6))

        if task:
            # Single Task Mode
            x = [f"{r['date']} {r['cycle']:02d}Z" for r in rows]
            y = [r["runtime_sec"] for r in rows]
            color = plt.rcParams['axes.prop_cycle'].by_key()['color'][0]

            self._plot_with_mean_std(x, y, task, color)
            plt.title(f"Task Runtime: {task}")
        else:
            # Compare All Tasks Mode
            task_groups = {}
            for r in rows:
                tname = r["name"]
                if tname not in task_groups:
                    task_groups[tname] = {"x": [], "y": []}

                task_groups[tname]["x"].append(f"{r['date']} {r['cycle']:02d}Z")
                task_groups[tname]["y"].append(r["runtime_sec"])

            colors = plt.rcParams['axes.prop_cycle'].by_key()['color']
            for idx, (tname, data) in enumerate(task_groups.items()):
                color = colors[idx % len(colors)]
                self._plot_with_mean_std(data["x"], data["y"], tname, color)

            plt.title("All Task Runtimes")

        self._finalize_plot(output, "Cycle", "Runtime (sec)")

    def plot_obs(self, obs_space: Optional[str], obs_category: Optional[str], days: Optional[int], output: Optional[str]):
        plt.figure(figsize=(12, 6))

        if obs_space:
            rows = fetch_obs_count_for_space_for_plot(self.db, obs_space, days)
            title = f"Obs Count: {obs_space}"
            label = obs_space
            key = "obs_count"
        elif obs_category:
            rows = fetch_obs_count_by_category_for_plot(self.db, obs_category, days)
            title = f"Total Obs Count: {obs_category}"
            label = f"Total {obs_category}"
            key = "total_obs"
        else:
            return

        if not rows:
            print(f"No data found for {label}.")
            return

        x = [f"{r['date']} {r['cycle']:02d}Z" for r in rows]
        y = [r[key] for r in rows]

        color = plt.rcParams['axes.prop_cycle'].by_key()['color'][0]
        self._plot_with_mean_std(x, y, label, color)
        plt.title(title)

        self._finalize_plot(output, "Cycle", "Count")

    # --- Internal Helpers ---

    def _plot_with_mean_std(self, x: List, y: List, label: str, color):
        """Helper to plot a line with mean and std dev shading."""
        y = np.array(y, dtype=float)
        mean = np.mean(y)
        std = np.std(y)

        plt.plot(x, y, label=label, color=color, marker='o', markersize=3)
        plt.axhline(mean, linestyle="--", color=color, alpha=0.6, label=f"{label} mean")
        plt.fill_between(x, mean - std, mean + std, color=color, alpha=0.2)

    def _finalize_plot(self, output: Optional[str], xlabel: str, ylabel: str):
        """Handles common plot formatting and saving."""
        plt.xlabel(xlabel)
        plt.ylabel(ylabel)
        plt.xticks(rotation=60)
        plt.grid(True, linestyle='--', alpha=0.7)
        plt.legend(loc="upper left", bbox_to_anchor=(1.02, 1.0))
        plt.tight_layout()

        if output:
            plt.savefig(output, dpi=150, bbox_inches="tight")
            print(f"Plot saved to: {output}")
            plt.close()
        else:
            plt.show()
