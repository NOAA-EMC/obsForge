import logging

from database.monitor_db import MonitorDB
from scanner.log_file_scanner import LogFileScanner
from scanner.file_system_scanner import FileSystemScanner
from scanner.file_content_scanner import FileContentScanner
from scanner.persistence import ScannerStateReader, Registrar


logger = logging.getLogger("Scanner")


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

        # --- Step 1: Log file scan (files marked DECLARED) ---
        log_scanner = LogFileScanner(
            self.data_root,
            known_state=known_state,
        )
        log_scan_cycles = log_scanner.scan_cycles(limit=limit_cycles)
        logger.info("----- log file scan completed")
        self.registrar.persist_cycles(log_scan_cycles)
        logger.info("----- persisted log scan cycles to db")

        # --- Step 2: File system scan ---
        fs_scanner = FileSystemScanner(
            self.data_root,
        )
        # temporary fix:
        fs_scan_cycles = fs_scanner.inspect_cycles(log_scan_cycles)
        logger.info("----- file system scan completed")
        self.registrar.persist_cycles(fs_scan_cycles)
        logger.info("----- persisted file system scan cycles to db")

        # --- Step 3: File content scan ---
        fc_scanner = FileContentScanner(
            self.data_root,
        )
        # temporary fix:
        fc_scan_cycles = fc_scanner.inspect_cycles(fs_scan_cycles)
        logger.info("----- file content scan completed")
        self.registrar.persist_cycles(fc_scan_cycles)
        logger.info("----- persisted content scan cycles to db")

    def report(self):
        return self.registrar.report()
