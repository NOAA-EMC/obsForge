import logging
from .data_service import InspectionDataService
from .rules import (
    IodaStructureRule, ZeroObsRule, VolumeAnomalyRule, 
    GeoSpatialRule, DataQualityRule, TimeConsistencyRule
)
from pyobsforge.monitor.database.monitor_db import MonitorDB

logger = logging.getLogger("Inspector")

class InventoryInspector:
    def __init__(self, db_path):
        self.db_read = InspectionDataService(db_path)
        self.db_write = MonitorDB(db_path)
        
        # REGISTER RULES
        self.rules = [
            IodaStructureRule(),
            ZeroObsRule(),
            VolumeAnomalyRule(),
            GeoSpatialRule(),
            DataQualityRule(),
            TimeConsistencyRule()
        ]

    def run_checks(self):
        logger.info("Starting Systematic Inspection...")
        
        # 1. Find Files
        files = self.db_read.get_recent_files(days=1)
        if not files:
            logger.info("No recent files found.")
            return

        # 2. Pre-fetch Baselines
        baselines = {}
        for f in files:
            key = (f['obs_space'], f['run_type'])
            if key not in baselines:
                baselines[key] = self.db_read.get_baseline_stats(f['obs_space'], f['run_type'])
        
        # 3. Context for Rules
        context = {
            'baselines': baselines,
            'stats_loader': self.db_read.get_file_stats
        }
        
        issues = 0

        # 4. Check Loop
        for f in files:
            file_errors = []
            
            for rule in self.rules:
                error = rule.check(f, context)
                if error:
                    file_errors.append(error)
            
            # Status Determination
            status = 'OK'
            msg = None
            if file_errors:
                status = 'WARNING'
                msg = "; ".join(file_errors)
                logger.warning(f"[{status}] {f['file_path']} -> {msg}")
                issues += 1
            
            # 5. Persist Result
            self.db_write.update_file_status(f['id'], status, msg)

        if issues > 0:
            self.db_write.commit()
            
        logger.info(f"Inspection Complete. Found {issues} anomalies.")
