import os
from datetime import datetime

from .base import BaseScanner
from ncdb.ds.file import File
from ncdb.ds.dataset import Dataset


class ObsForgeScanner(BaseScanner):

    def parse_obs_space(self, file_path):
        filename = os.path.basename(file_path)
        parts = filename.split(".")

        if len(parts) < 4 or parts[-1] != "nc":
            return None

        return parts[2]

    def discover_datasets(self, root_dir):
        datasets = []

        for entry in os.listdir(root_dir):
            full = os.path.join(root_dir, entry)

            if not os.path.isdir(full):
                continue

            if "." not in entry:
                continue

            name, date_str = entry.split(".", 1)

            if not date_str.isdigit():
                continue

            datasets.append(name)

        self.datasets = [
            Dataset(name=d, root_dir=root_dir)
            for d in sorted(set(datasets))
        ]

        return self.datasets

    def discover_cycles(self, dataset, root_dir):
        cycles = set()

        for entry in os.listdir(root_dir):
            if not entry.startswith(dataset.name + "."):
                continue

            name, date_str = entry.split(".", 1)

            try:
                cycle_date = datetime.strptime(date_str, "%Y%m%d").date()
            except ValueError:
                continue

            ds_dir = os.path.join(root_dir, entry)

            for hour in os.listdir(ds_dir):
                if hour in {"00", "06", "12", "18"}:
                    cycles.add((cycle_date, hour))

        return sorted(cycles)

    def scan_cycle(self, dataset, root_dir, cycle_date, cycle_hour):
        ds_dir = os.path.join(
            root_dir,
            f"{dataset.name}.{cycle_date.strftime('%Y%m%d')}",
            cycle_hour
        )

        results = []

        if not os.path.isdir(ds_dir):
            return results

        # print(f"scanning {ds_dir}")

        for dirpath, _, filenames in os.walk(ds_dir):
            for f in filenames:
                # print(f"        scanning file {f}")
                if not f.endswith(".nc"):
                    continue

                full = os.path.join(dirpath, f)
                file_obj = File.from_path(full)

                obs_space = self.parse_obs_space(full)
                # print(f"[SCAN] file={f} → obs_space={obs_space}")
                if obs_space:
                    results.append((file_obj, obs_space))

        return results
