import os
import logging
import re
from pathlib import Path
from datetime import datetime, timedelta
from typing import List, Dict, Tuple, Optional

from scanner.models import CycleData, TaskRunData, FileInventoryData
from scanner.persistence import Registrar


logger = logging.getLogger("FileSystemScanner")


def cycle_to_datetime(date: str, cycle: int) -> datetime:
    return datetime.strptime(date, "%Y%m%d") + timedelta(hours=cycle)

OBS_SPACE_PATTERN = re.compile(r"^[^.]+\.t\d{2}z\.(.+)\.nc$")
def extract_obs_space_name(filename: str) -> str:
    """
    Extract obs space name from filename like:
        gfs.t06z.rads_adt_6a.nc
    Returns:
        rads_adt_6a
    Falls back to stem if pattern does not match.
    """
    match = OBS_SPACE_PATTERN.match(filename)
    if match:
        return match.group(1)
    return Path(filename).stem

class FileSystemScanner:
    """
    Scans FileInventoryData objects, handling both filesystem
    and content inspection (content will later move to Inspector).
    """

    def __init__(self, data_root, known_cycles: set = None):
        self.data_root = data_root
        self.known_cycles = known_cycles or {}

    # temporary
    # def scan_cycles(self, cycles: List[CycleData], limit: int = None) -> list:
    # to be deprecated
    # def scan_cycles(self, known_cycles: set = None, limit: int = None) -> list:
    def inspect_cycles(self, cycles: List[CycleData], limit: int = None) -> list:
        if limit and limit > 0:
            logger.info(f"scanning limited to {limit} cycles")
            cycles = cycles[-limit:]

        for cycle in cycles:
            logger.info(f"Inspecting cycle {cycle.date} {cycle.cycle:02d}")
            for task in cycle.tasks:
                self._inspect_file_system_info(task.files)

        return cycles


    def _inspect_file_system_info(self, file_inventory: list):
        """
        Populates basic file info for a list of FileInventoryData:
          - existence
          - size
          - mtime
          - integrity: MISSING, EMPTY, OK_SKIPPED
        Does not inspect file content.
        """
        for f in file_inventory:
            f.error_msg = None  # reset errors
            f.obs_count = 0
            f.stats = []
            f.domain = None
            f.properties = {}

            full_path = os.path.join(self.data_root, f.rel_path)

            if not os.path.exists(full_path):
                f.integrity = "MISSING"
                continue

            try:
                stat_info = os.stat(full_path)
                f.size_bytes = stat_info.st_size
                f.mtime = int(stat_info.st_mtime)

                if f.size_bytes == 0:
                    f.integrity = "EMPTY"
                else:
                    # Still unknown if content is valid; mark as pending inspection
                    f.integrity = "OK_PENDING"

            except OSError as e:
                f.integrity = "ERR_ACC"
                f.error_msg = str(e)




    def generate_files_from_directory(
        self,
        cycle_root: str,
    ) -> List[FileInventoryData]:
        """
        Scan:
            <cycle_root>/
                <domain>/
                    <category>/
                        *.nc
        """

        cycle_root = Path(cycle_root)
        inventory: List[FileInventoryData] = []

        for nc_file in cycle_root.rglob("*.nc"):
            if not nc_file.is_file():
                continue

            rel = nc_file.relative_to(cycle_root)
            parts = rel.parts

            # Expect: domain/category/file.nc
            if len(parts) < 3:
                continue

            domain = parts[0]       # ocean / atmos
            category = parts[1]     # adt / sst / etc.

            try:
                stat = nc_file.stat()
                integrity = "OK"
                size_bytes = stat.st_size
                mtime = int(stat.st_mtime)
                error_msg = None
            except Exception as e:
                integrity = "CORRUPT"
                size_bytes = 0
                mtime = 0
                error_msg = str(e)

            obs_space_name = extract_obs_space_name(nc_file.name)

            file_data = FileInventoryData(
                rel_path=str(rel),
                category=category,
                obs_space_name=obs_space_name,
                integrity=integrity,
                size_bytes=size_bytes,
                mtime=mtime,
                obs_count=0,
                error_msg=error_msg,
                properties={"domain": domain},
            )

            inventory.append(file_data)

        return inventory



    """
    Build CycleData objects directly from directory structure
    instead of from logs, then inspect them.

    Expected structure:
        data_root/
            gfs.20260204/
                06/
                    ocean/
                        adt/*.nc
    """


    def discover_inventory_cycles(self, limit: int = None) -> List[CycleData]:
        """
        Discover cycles directly from filesystem and build CycleData objects.
        Deduce run_type from directory name and nc filenames.
        """

        root = Path(self.data_root)
        cycles: List[CycleData] = []

        for model_date_dir in sorted(root.iterdir()):
            if not model_date_dir.is_dir() or model_date_dir.name == "logs":
                continue

            # Directory pattern: <run_type>.<YYYYMMDD>
            if "." not in model_date_dir.name:
                continue

            run_type_dir, date_str = model_date_dir.name.split(".", 1)
            if not date_str.isdigit():
                continue

            for cycle_dir in sorted(model_date_dir.iterdir()):
                if not cycle_dir.is_dir() or not cycle_dir.name.isdigit():
                    continue

                cycle_int = int(cycle_dir.name)

                files = self.generate_files_from_directory(
                    str(cycle_dir),
                )

                if not files:
                    continue

                # Optional: cross-check first file for run_type consistency
                if files:
                    first_file_name = Path(files[0].rel_path).name
                    # filename pattern: <file_run_type>.t06z.<obs_space>.nc
                    file_prefix = first_file_name.split(".")[0]
                    run_type = file_prefix if file_prefix else run_type_dir
                else:
                    run_type = run_type_dir

                task = TaskRunData(
                    task_name=run_type,
                    raw_task_name=run_type_dir,
                    run_type=run_type,
                    logfile="inventory_scan",
                    files=files,
                )

                cycle_data = CycleData(
                    date=date_str,
                    cycle=cycle_int,
                    tasks=[task],
                )

                cycles.append(cycle_data)

        if limit and limit > 0:
            logger.info(f"Inventory discovery limited to {limit} cycles")
            cycles = cycles[-limit:]

        return cycles




    def old_discover_inventory_cycles(self, limit: int = None) -> List[CycleData]:
        """
        Discover cycles directly from filesystem and build CycleData objects.
        Does NOT inspect file system metadata.
        """

        root = Path(self.data_root)
        cycles: List[CycleData] = []

        for model_date_dir in sorted(root.iterdir()):
            if not model_date_dir.is_dir():
                continue

            if "." not in model_date_dir.name:
                continue

            model, date_str = model_date_dir.name.split(".", 1)

            if not date_str.isdigit():
                continue

            for cycle_dir in sorted(model_date_dir.iterdir()):
                if not cycle_dir.is_dir():
                    continue

                if not cycle_dir.name.isdigit():
                    continue

                cycle_int = int(cycle_dir.name)

                files = self.generate_files_from_directory(
                    str(cycle_dir),
                    # data_root=str(root),
                )

                if not files:
                    continue

                cycle_data = CycleData(
                    date=date_str,
                    cycle=cycle_int,
                    tasks=[
                        TaskRunData(
                            task_name=model,
                            raw_task_name=model,
                            run_type="inventory_scan",
                            logfile="inventory_scan",
                            files=files,
                        )
                    ],
                )

                cycles.append(cycle_data)

        if limit and limit > 0:
            logger.info(f"Inventory discovery limited to {limit} cycles")
            cycles = cycles[-limit:]

        return cycles


    def inspect_cycles(self, cycles: List[CycleData], limit: int = None) -> List[CycleData]:
        """
        Inspect filesystem metadata for already-built CycleData.
        Used by both log-based and inventory-based flows.
        """

        if limit and limit > 0:
            logger.info(f"Inspection limited to {limit} cycles")
            cycles = cycles[-limit:]

        for cycle in cycles:
            logger.info(f"Inspecting cycle {cycle.date} {cycle.cycle:02d}")
            for task in cycle.tasks:
                self._inspect_file_system_info(task.files)

        return cycles


    def scan_inventory_cycles(self, limit: int = None) -> List[CycleData]:
        """
        Full inventory-based scan:
          1. Discover cycles from filesystem
          2. Inspect filesystem metadata
        """

        cycles = self.discover_inventory_cycles(limit=limit)
        return self.inspect_cycles(cycles)




    def find_missing_cycles(
        cycles: List[CycleData],
        start_date: str,
        start_cycle: int,
        end_date: str,
        end_cycle: int,
    ) -> List[Tuple[str, int]]:
        """
        Given discovered CycleData and a start/end range,
        return missing (date, cycle) tuples within the range.

        Assumes 6-hour cycle spacing.
        """

        if not cycles:
            # everything missing in range
            existing_set = set()
        else:
            existing_set = {
                datetime.strptime(c.date, "%Y%m%d") + timedelta(hours=c.cycle)
                for c in cycles
            }

        start_dt = datetime.strptime(start_date, "%Y%m%d") + timedelta(hours=start_cycle)
        end_dt = datetime.strptime(end_date, "%Y%m%d") + timedelta(hours=end_cycle)

        if start_dt > end_dt:
            raise ValueError("start must be <= end")

        missing = []
        current = start_dt

        while current <= end_dt:
            if current not in existing_set:
                missing.append(
                    (current.strftime("%Y%m%d"), current.hour)
                )
            current += timedelta(hours=6)

        return missing




    def get_cycle_range(
        cycles: List[CycleData],
    ) -> Optional[Tuple[Tuple[str, int], Tuple[str, int]]]:
        """
        Returns:
            ((first_date, first_cycle), (last_date, last_cycle))

        If cycles is empty → returns None.
        """

        if not cycles:
            return None

        # Convert to datetime
        datetimes = [
            datetime.strptime(c.date, "%Y%m%d") + timedelta(hours=c.cycle)
            for c in cycles
        ]

        first_dt = min(datetimes)
        last_dt = max(datetimes)

        return (
            (first_dt.strftime("%Y%m%d"), first_dt.hour),
            (last_dt.strftime("%Y%m%d"), last_dt.hour),
        )

