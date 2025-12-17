import numpy as np
import matplotlib.pyplot as plt
from typing import Optional, List, Dict

# NEW: Import DBReader for data fetching
# (We assume the caller passes a DBReader instance or we wrap the DB connection)
from pyobsforge.monitor.database.db_reader import DBReader

class MonitorPlotter:
    def __init__(self, db_connection_or_reader):
        """
        Accepts either a MonitorDB instance (legacy compat) or DBReader.
        """
        if hasattr(db_connection_or_reader, 'get_task_timings'):
            self.reader = db_connection_or_reader
        else:
            # Fallback: Create a reader on the fly if passed a raw DB object
            # This assumes db_connection_or_reader has a .db_path attribute or similar
            # For safety, let's just assume the caller passes the right object or path
            self.reader = DBReader(db_connection_or_reader.db_path)

    def plot_timings(self, task: Optional[str], days: Optional[int], output: Optional[str]):
        # Use DBReader to fetch standardized dicts
        rows = self.reader.get_task_timings(days, task_name=task)
        
        if not rows:
            print("No data found for plotting.")
            return

        if days is None and len(rows) > 100:
            print("Warning: Plotting >100 points. Consider using --days N to filter.")

        plt.figure(figsize=(12, 6))

        if task:
            # Single Task Mode
            # DBReader returns dicts with keys: date, cycle, run_type, task, duration
            x = [f"{r['date']} {r['cycle']:02d}Z" for r in rows]
            y = [r["duration"] for r in rows]
            color = plt.rcParams['axes.prop_cycle'].by_key()['color'][0]

            self._plot_with_mean_std(x, y, task, color)
            plt.title(f"Task Runtime: {task}")
        else:
            # Compare All Tasks Mode
            task_groups = {}
            for r in rows:
                tname = r["task"]
                if tname not in task_groups:
                    task_groups[tname] = {"x": [], "y": []}

                task_groups[tname]["x"].append(f"{r['date']} {r['cycle']:02d}Z")
                task_groups[tname]["y"].append(r["duration"])

            colors = plt.rcParams['axes.prop_cycle'].by_key()['color']
            for idx, (tname, data) in enumerate(task_groups.items()):
                color = colors[idx % len(colors)]
                self._plot_with_mean_std(data["x"], data["y"], tname, color)

            plt.title("All Task Runtimes")

        self._finalize_plot(output, "Cycle", "Runtime (sec)")

    def plot_obs(self, obs_space: Optional[str], obs_category: Optional[str], days: Optional[int], output: Optional[str]):
        plt.figure(figsize=(12, 6))

        rows = []
        title = ""
        label = ""
        key = "count" # DBReader standardizes count key to 'count'

        if obs_space:
            rows = self.reader.get_obs_counts_by_space(obs_space, days)
            title = f"Obs Count: {obs_space}"
            label = obs_space
        elif obs_category:
            rows = self.reader.get_obs_counts_by_category(obs_category, days)
            title = f"Total Obs Count: {obs_category}"
            label = f"Total {obs_category}"
        else:
            print("Error: Must specify --obs-space or --obs-category")
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

    # --- Internal Helpers (Preserved logic) ---

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
