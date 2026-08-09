import logging

from database.monitor_db import MonitorDB
from scanner.log_file_scanner import LogFileScanner
from scanner.file_system_scanner import FileSystemScanner
from scanner.file_content_scanner import FileContentScanner
from scanner.persistence import ScannerStateReader, Registrar

from scanner.coverage import CoverageAnalyzer
from collections import defaultdict


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
        # logger.info("starting...")

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
        # dump_cycles(log_scan_cycles, "j1")
        # logger.info(f"----- log file scan completed {log_scan_cycles}")
        # self.registrar.persist_cycles(log_scan_cycles)
        # logger.info("----- persisted log scan cycles to db")

        # --- Step 2: File system scan ---
        fs_scanner = FileSystemScanner(
            self.data_root,
        )
        # temporary fix:
        fs_scan_cycles = fs_scanner.inspect_cycles(log_scan_cycles)
        # dump_cycles(fs_scan_cycles, "j2")
        # logger.info(f"----- file system scan completed {fs_scan_cycles}")
        # self.registrar.persist_cycles(fs_scan_cycles)
        # logger.info("----- persisted file system scan cycles to db")

        # --- Step 3: File content scan ---
        fc_scanner = FileContentScanner(
            self.data_root,
        )
        # temporary fix:
        fc_scan_cycles = fc_scanner.inspect_cycles(fs_scan_cycles)
        # dump_cycles(fc_scan_cycles, "j3")
        # logger.info(f"----- file content scan completed {fc_scan_cycles}")
        self.registrar.persist_cycles(fc_scan_cycles)
        logger.info("----- persisted content scan cycles to db")


    def scan_inventory(self, limit_cycles=None):
        fs_scanner = FileSystemScanner(
            self.data_root,
        )
        fs_scan_cycles = fs_scanner.scan_inventory_cycles(limit_cycles)
        fc_scanner = FileContentScanner(
            self.data_root,
        )
        fc_scan_cycles = fc_scanner.inspect_cycles(fs_scan_cycles)
        self.registrar.persist_cycles(fc_scan_cycles)
        logger.info("----- persisted content scan cycles to db")

        self.report_missing_cycles()


    def report_missing_cycles(self):
        fs_scanner = FileSystemScanner(
            self.data_root,
        )
        cycles = fs_scanner.discover_inventory_cycles()

        # analyzer = CoverageAnalyzer(cycles)
        # report = analyzer.report()
        # print("First cycle:", report["first_cycle"])
        # print("Last cycle:", report["last_cycle"])
        # print("Missing cycles:", report["missing_cycles"])
        # print("Coverage %:", report["coverage_pct"])


        run_type_groups = defaultdict(list)
        for c in cycles:
            for task in c.tasks:
                run_type_groups[task.run_type].append(c)

        analyzer = CoverageAnalyzer(cycles)
        # reports = analyzer.generate_per_run_type_report()

        # for run_type, report in reports.items():
            # print(f"=== {run_type} ===")
            # print("First cycle:", report["first_cycle"])
            # print("Last cycle:", report["last_cycle"])
            # print("Missing cycles:", report["missing_cycles"])
            # print("Coverage %:", report["coverage_pct"])

        missing_obs_report = analyzer.condensed_missing_obs_space_report()

        for run_type, obs_dict in missing_obs_report.items():
            print(f"=== {run_type} ===")
            for obs, ranges in obs_dict.items():
                print(f"{obs}: {ranges}")



        # for run_type, report in reports.items():
            # print(f"=== {run_type} ===")
            # print("First cycle:", report["first_cycle"])
            # print("Last cycle:", report["last_cycle"])
            # print("Missing cycles:", report["missing_cycles"])
            # print("Missing obs spaces:")
            # for cycle, obs_spaces in report["missing_obs_spaces"].items():
                # print(f"  {cycle}: {obs_spaces}")
            # print("Coverage %:", report["coverage_pct"])




    def report(self):
        return self.registrar.report()
