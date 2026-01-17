#!/usr/bin/env python3
"""
Phase 2: Data Analyzer.
Responsibility:
1. Query DB for files that are registered but lack statistics.
2. Open files, read raw data arrays.
3. Compute Stats (Min/Max/Mean) and Domain.
4. UPDATE Observation Count in file_inventory.
5. Update DB Statistics.
"""

import argparse
import logging
import sys
import os
import numpy as np
from netCDF4 import Dataset, num2date, date2num

# Reuse your existing DB interface
try:
    from database.monitor_db import MonitorDB
except ImportError:
    sys.path.append(os.path.join(os.path.dirname(__file__), '../'))
    from database.monitor_db import MonitorDB

# Configure Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S",
    stream=sys.stdout
)
logger = logging.getLogger("AnalyzeData")

class DataAnalyzer:
    def __init__(self, db_path):
        self.db = MonitorDB(db_path)

    def find_pending_files(self, limit=None):
        """
        Finds files that are in inventory but have no stats recorded.
        """
        query = """
            SELECT id, file_path as path 
            FROM file_inventory 
            WHERE id NOT IN (SELECT distinct file_id FROM file_variable_statistics)
            ORDER BY file_modified_time DESC
        """
        
        if limit and limit > 0:
            query += f" LIMIT {limit}"
            
        rows = self.db.conn.execute(query).fetchall()
        return rows

    def process_file(self, file_id, rel_path, data_root):
        full_path = os.path.join(data_root, rel_path)
        if not os.path.exists(full_path):
            logger.warning(f"File missing: {full_path}")
            return

        try:
            with Dataset(full_path, 'r') as ds:
                # 1. NEW: Count Observations
                obs_count = self._count_observations(ds)
                
                # 2. NEW: Update Inventory Table immediately
                self.db.conn.execute(
                    "UPDATE file_inventory SET obs_count = ? WHERE id = ?", 
                    (obs_count, file_id)
                )

                # 3. Calculate Domain
                domain = self._extract_domain(ds)
                if domain:
                    self.db.log_file_domain(
                        file_id,
                        domain.get('start'), domain.get('end'),
                        domain.get('min_lat'), domain.get('max_lat'),
                        domain.get('min_lon'), domain.get('max_lon')
                    )

                # 4. Calculate Statistics
                stats, anomalies = self._calculate_statistics(ds)
                
                if stats:
                    self.db.log_variable_statistics(file_id, stats)

            # Commit changes
            self.db.commit()

        except Exception as e:
            logger.error(f"Failed to analyze {rel_path}: {e}")

    def _count_observations(self, ds):
        """
        Determines the number of observations (rows) in the file.
        Matches the legacy logic from the old scanner.
        """
        try:
            if "Location" in ds.dimensions:
                return len(ds.dimensions["Location"])
            elif "nlocs" in ds.dimensions:
                return len(ds.dimensions["nlocs"])
            
            # Fallback: check the first dimension of the first variable in 'ObsValue'
            if 'ObsValue' in ds.groups:
                grp = ds.groups['ObsValue']
                for v_name in grp.variables:
                    dims = grp.variables[v_name].dimensions
                    if dims:
                        return len(ds.dimensions[dims[0]])
        except Exception:
            pass
        return 0

    def _extract_domain(self, ds):
        d = {}
        try:
            t_var = None
            candidates = [
                ('MetaData', 'dateTime'), ('MetaData', 'time'),
                (None, 'time'), (None, 'date')
            ]
            for group, var_name in candidates:
                if group and group in ds.groups and var_name in ds.groups[group].variables:
                    t_var = ds.groups[group].variables[var_name]
                    break
                elif not group and var_name in ds.variables:
                    t_var = ds.variables[var_name]
                    break

            if t_var is not None:
                times = t_var[:]
                if np.ma.is_masked(times): times = times.compressed()
                if len(times) > 0:
                    min_t, max_t = np.min(times), np.max(times)
                    if hasattr(t_var, 'units'):
                        try:
                            unix_units = "seconds since 1970-01-01 00:00:00"
                            cal = getattr(t_var, 'calendar', 'standard')
                            d['start'] = int(date2num(num2date(min_t, t_var.units, calendar=cal), units=unix_units))
                            d['end'] = int(date2num(num2date(max_t, t_var.units, calendar=cal), units=unix_units))
                        except:
                            d['start'], d['end'] = int(min_t), int(max_t)
                    else:
                        d['start'], d['end'] = int(min_t), int(max_t)

            if 'MetaData' in ds.groups:
                md = ds.groups['MetaData']
                for ax, k_min, k_max in [('latitude', 'min_lat', 'max_lat'), ('longitude', 'min_lon', 'max_lon')]:
                    if ax in md.variables:
                        vals = md.variables[ax][:]
                        if np.ma.is_masked(vals): vals = vals.compressed()
                        vals = vals[np.abs(vals) < 1000] 
                        if len(vals) > 0:
                            d[k_min] = float(np.min(vals))
                            d[k_max] = float(np.max(vals))
        except Exception:
            pass
        return d

    def _calculate_statistics(self, ds):
        stats = []
        anomalies = []
        THRESHOLD = 1.5e9
        groups = ['ObsValue', 'MetaData']
        
        for g in groups:
            if g not in ds.groups: continue
            grp = ds.groups[g]
            
            for v_name, v_obj in grp.variables.items():
                if v_obj.dtype == np.str_ or v_obj.dtype == np.object_: continue
                if 'dateTime' in v_name or 'time' in v_name.lower(): continue

                try:
                    v_obj.set_auto_mask(False)
                    raw_data = v_obj[:]
                    clean_data = raw_data[np.abs(raw_data) < THRESHOLD]
                    
                    if len(clean_data) > 0:
                        stats.append({
                            'name': f"{g}/{v_name}",
                            'min': float(np.min(clean_data)),
                            'max': float(np.max(clean_data)),
                            'mean': float(np.mean(clean_data)),
                            'std': float(np.std(clean_data))
                        })
                except Exception:
                    pass
        return stats, anomalies

def main():
    parser = argparse.ArgumentParser(description="Step 2: Analyze Data Content")
    parser.add_argument("--db", required=True, help="Path to SQLite DB")
    parser.add_argument("--data-root", required=True, help="Root scan dir")
    parser.add_argument("--limit", type=int, default=None, help="Max files to process")
    
    args = parser.parse_args()
    
    analyzer = DataAnalyzer(args.db)
    
    # 1. Find ALL work
    logger.info("Querying database for pending files...")
    pending = analyzer.find_pending_files(limit=args.limit)
    
    total = len(pending)
    logger.info(f"Found {total} files pending analysis.")
    
    if total == 0:
        logger.info("Nothing to do.")
        return

    # 2. Do work
    for i, row in enumerate(pending, 1):
        if i % 100 == 0:
             logger.info(f"Progress: {i}/{total} files analyzed...")
        
        analyzer.process_file(row['id'], row['path'], args.data_root)
    
    logger.info(f"Done. Processed {total} files.")

if __name__ == "__main__":
    main()
