import os
import logging
from typing import List

from scanner.models import CycleData, TaskRunData, FileInventoryData
from scanner.persistence import Registrar

logger = logging.getLogger("FileSystemScanner")


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
