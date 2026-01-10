import logging

from database.monitor_db import MonitorDB
from .data_service import InspectionDataService
from .rules import (
    DataQualityRule,
    IodaStructureRule,
    PhysicalRangeRule,
    TimeConsistencyRule,
    VolumeAnomalyRule,
    ZeroObsRule,
)

logger = logging.getLogger("Inspector")


class InventoryInspector:
    """
    The Inspector Engine.

    Responsibilities:
    1. Fetch 'OK' files from the Inventory (via InspectionDataService).
    2. Run a suite of Logic Rules against each file.
    3. Update the file's status in the Database (via MonitorDB).
    """

    def __init__(self, db_path):
        # 1. Reader: For fetching file stats, history, and metadata
        self.db_read = InspectionDataService(db_path)

        # 2. Writer: For updating integrity_status and error_messages
        self.db_write = MonitorDB(db_path)

        # 3. Register Rules (Order matters for efficiency)
        self.rules = [
            # Fast Checks (Metadata)
            IodaStructureRule(),
            ZeroObsRule(),
            TimeConsistencyRule(),

            # Statistical Checks (History)
            VolumeAnomalyRule(),

            # Content Checks (Physics/Data) - Slower
            PhysicalRangeRule(),
            DataQualityRule()
        ]

    def run_checks(self):
        """Main execution method."""
        logger.info("Starting Systematic Inspection...")

        # 1. Find Candidates (Recent files marked 'OK')
        files = self.db_read.get_recent_files(days=1)
        if not files:
            logger.info("No recent files found to inspect.")
            return

        # 2. Pre-fetch Baselines (Optimization)
        # We load historical averages once to avoid 1 query per file
        baselines = {}
        for f in files:
            key = (f['obs_space'], f['run_type'])
            if key not in baselines:
                baselines[key] = self.db_read.get_baseline_stats(
                    f['obs_space'], f['run_type']
                )

        # 3. Build Rule Context
        # Allows rules to access helpers without knowing about the DB class
        context = {
            'baselines': baselines,
            'stats_loader': self.db_read.get_file_stats
        }

        issues = 0

        # 4. Inspection Loop
        for f in files:
            file_errors = []

            # Run all rules
            for rule in self.rules:
                error = rule.check(f, context)
                if error:
                    file_errors.append(error)

            # Determine Status
            status = 'OK'
            msg = None

            if file_errors:
                msg = "; ".join(file_errors)

                # Severity Logic
                critical_keywords = [
                    'Zero', 'Corrupt', 'Structure', 'Time Mismatch',
                    'Overflow', 'Underflow'
                ]

                if any(k in msg for k in critical_keywords):
                    status = 'FAIL'    # Critical Errors (Red)
                elif "INFO:" in msg:
                    status = 'OK'      # Informational (Green/Blue)
                else:
                    status = 'WARNING'  # Volume/Quality Warnings

                # Log it (Skip logging pure INFO messages to reduce noise)
                if "INFO:" not in msg:
                    logger.warning(f"[{status}] {f['file_path']} -> {msg}")
                    issues += 1

            # 5. Write Result
            self.db_write.update_file_status(f['id'], status, msg)

        # Commit all updates at once
        if issues > 0:
            self.db_write.commit()

        logger.info(
            f"Inspection Complete. Found {issues} anomalies in "
            f"{len(files)} files."
        )
