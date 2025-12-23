import logging
from .data_service import InspectionDataService

logger = logging.getLogger("Inspector")

class InventoryInspector:
    """
    Performs high-level logical assessment of the inventory.
    Detects anomalies like zero-observation files or sudden volume drops.
    """
    def __init__(self, db_path):
        self.db = InspectionDataService(db_path)

    def run_checks(self):
        logger.info("Starting Inventory Inspection...")
        
        files = self.db.get_recent_files(days=1)
        if not files:
            logger.info("No recent files found to inspect.")
            return

        issues = 0
        baselines = {} # Cache to avoid repetitive DB hits

        for f in files:
            key = (f['obs_space'], f['run_type'])
            
            # --- RULE 1: Zero Observations ---
            # A file exists (physically OK), but contains no data.
            # This is often a configuration error or upstream outage.
            if f['obs_count'] == 0:
                logger.warning(f"[ZERO OBS] {f['file_path']} is valid NetCDF but empty.")
                issues += 1
                continue

            # --- RULE 2: Volume Anomaly ---
            # The file has data, but significantly less than usual.
            if key not in baselines:
                stats = self.db.get_baseline_stats(f['obs_space'], f['run_type'])
                baselines[key] = stats['threshold'] if stats['threshold'] else 0
            
            threshold = baselines[key]
            
            # Only flag if we have a valid baseline (> 0) and count is low
            if threshold > 0 and f['obs_count'] < threshold:
                logger.warning(f"[LOW VOL] {f['file_path']} has {f['obs_count']} obs (Normal > {threshold*2})")
                issues += 1

        logger.info(f"Inspection Complete. Found {issues} anomalies.")
