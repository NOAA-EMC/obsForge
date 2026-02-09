import logging

from database.monitor_db import MonitorDB
from scanner.log_file_scanner import LogFileScanner
from scanner.file_system_scanner import FileSystemScanner
from scanner.file_content_scanner import FileContentScanner
from scanner.persistence import ScannerStateReader, Registrar


logger = logging.getLogger("Scanner")


from dataclasses import asdict
from pprint import pformat
import sys
from typing import Optional
def dump_cycle(cycle, path: Optional[str] = None):
    """
    Dump CycleData in a human-readable form.

    Args:
        cycle: CycleData instance
        path: None  -> write to stdout
              str   -> write to file at this path
    """
    text = pformat(asdict(cycle), width=120)

    if path is None:
        sys.stdout.write(text + "\n")
    else:
        with open(path, "w") as f:
            f.write(text + "\n")

def dump_cycles(cycles, file=sys.stdout):
    for cycle in cycles:
        dump_cycle(cycle, file)



class Scanner:
    def __init__(self, db_path, data_root):
        self.db_path = db_path
        self.data_root = data_root

        # --- DB + Registrar ---
        db = MonitorDB(db_path)
        self.registrar = Registrar(db)

    def run(self, limit_cycles=None):
        logger.info("starting...")

        # --- Load previous scan state ---
        # unused.....
        state_reader = ScannerStateReader(self.db_path)
        known_state = state_reader.get_known_state()

        # --- Step 1: Log file scan ---
        log_scanner = LogFileScanner(
            self.data_root,
            known_state=known_state,
        )
        log_scan_cycles = log_scanner.scan_cycles(limit=limit_cycles)
        dump_cycles(log_scan_cycles, "j1")
        # logger.info(f"----- log file scan completed {log_scan_cycles}")
        # self.registrar.persist_cycles(log_scan_cycles)
        # logger.info("----- persisted log scan cycles to db")

        # --- Step 2: File system scan ---
        fs_scanner = FileSystemScanner(
            self.data_root,
        )
        # temporary fix:
        fs_scan_cycles = fs_scanner.inspect_cycles(log_scan_cycles)
        dump_cycles(fs_scan_cycles, "j2")
        # logger.info(f"----- file system scan completed {fs_scan_cycles}")
        # self.registrar.persist_cycles(fs_scan_cycles)
        # logger.info("----- persisted file system scan cycles to db")

        # --- Step 3: File content scan ---
        fc_scanner = FileContentScanner(
            self.data_root,
        )
        # temporary fix:
        fc_scan_cycles = fc_scanner.inspect_cycles(fs_scan_cycles)
        dump_cycles(fc_scan_cycles, "j3")
        # logger.info(f"----- file content scan completed {fc_scan_cycles}")
        self.registrar.persist_cycles(fc_scan_cycles)
        logger.info("----- persisted content scan cycles to db")

    def report(self):
        return self.registrar.report()
