import shutil
from pathlib import Path
from datetime import date
from datetime import datetime
import re


class DataProductsServer:
    """
    Manages the directory layout of dataset products and provides methods
    for website integration, including fetching an entire cycle.
    
    Layout:
    root/
      dataset_name/
        YYYYMMDD_HH/
          product_name/
            files...
    """

    def __init__(self, root: str):
        self.root = Path(root)

    # ----------------------------
    # Path helpers
    # ----------------------------
    def dataset_dir(self, dataset_name: str) -> Path:
        return self.root / dataset_name

    def cycle_dir(self, dataset_name: str, cycle_date: date, hour: str) -> Path:
        cycle = f"{cycle_date:%Y%m%d}_{hour}"
        return self.dataset_dir(dataset_name) / cycle

    def product_dir(
        self,
        dataset_name: str,
        cycle_date: date,
        hour: str,
        product: str,
        create: bool = True,
    ) -> Path:
        path = self.cycle_dir(dataset_name, cycle_date, hour) / product
        if create:
            path.mkdir(parents=True, exist_ok=True)
        return path

    def product_file(
        self,
        dataset_name: str,
        cycle_date: date,
        hour: str,
        product: str,
        filename: str,
    ) -> Path:
        return self.product_dir(dataset_name, cycle_date, hour, product) / filename

    def relative_path(self, path: Path) -> str:
        """Return path relative to root (for website links)."""
        return str(path.relative_to(self.root))

    # ----------------------------
    # Website / Fetching Methods
    # ----------------------------
    def fetch_cycle(
        self,
        dataset_name: str,
        cycle_date: date,
        hour: str,
        dest_dir: str,
    ) -> Path:
        """
        Copy the entire cycle directory to a destination directory.

        Args:
            dataset_name: name of the dataset
            cycle_date: date of the cycle
            hour: cycle hour string
            dest_dir: directory where the cycle should be copied

        Returns:
            Path object of the copied cycle directory
        """
        src = self.cycle_dir(dataset_name, cycle_date, hour)
        dest = Path(dest_dir) / f"{dataset_name}_{cycle_date:%Y%m%d}_{hour}"

        if not src.exists():
            raise FileNotFoundError(f"Source cycle directory does not exist: {src}")

        dest.mkdir(parents=True, exist_ok=True)
        shutil.copytree(src, dest, dirs_exist_ok=True)

        return dest

    def list_products(
        self,
        dataset_name: str,
        cycle_date: date,
        hour: str,
    ) -> list[str]:
        """
        List all products available for a given cycle (dataset, cycle_date, hour).
        
        Args:
            dataset_name: The name of the dataset
            cycle_date: The date of the cycle
            hour: The cycle hour
        
        Returns:
            A list of product names (directories) available for the specified cycle.
        """
        cycle_dir = self.cycle_dir(dataset_name, cycle_date, hour)

        if not cycle_dir.exists():
            raise FileNotFoundError(f"Cycle directory does not exist: {cycle_dir}")
        
        # List the subdirectories inside the cycle directory, which represent different products
        products = [product.name for product in cycle_dir.iterdir() if product.is_dir()]
        return products

    def list_available_cycles(self, dataset_name: str) -> list[tuple[date, str]]:
        """List all (date, hour) tuples available in the server's root for a dataset."""
        ds_path = self.dataset_dir(dataset_name)
        if not ds_path.exists():
            return []
        
        cycles = []
        # Pattern matches YYYYMMDD_HH
        pattern = re.compile(r"^(\d{8})_(\d{2})$")
        
        for folder in ds_path.iterdir():
            match = pattern.match(folder.name)
            if match:
                try:
                    d_obj = datetime.strptime(match.group(1), "%Y%m%d").date()
                    cycles.append((d_obj, match.group(2)))
                except ValueError:
                    continue
        
        # Sort chronologically
        return sorted(cycles, key=lambda x: (x[0], x[1]))

    def list_datasets(self) -> list[str]:
        """
        Scans the root directory for available datasets.
        A directory is considered a dataset if it contains at most 
        one level of subdirectories matching the YYYYMMDD_HH pattern.
        """
        if not self.root.exists():
            return []

        datasets = []
        # Cycle pattern: 8 digits, underscore, 2 digits (YYYYMMDD_HH)
        cycle_pattern = re.compile(r"^\d{8}_\d{2}$")

        for item in self.root.iterdir():
            if item.is_dir():
                # Check if this directory contains at least one valid cycle folder
                # to filter out random non-dataset directories
                has_cycles = any(cycle_pattern.match(sub.name) for sub in item.iterdir() if sub.is_dir())
                if has_cycles:
                    datasets.append(item.name)
        
        return sorted(datasets)
