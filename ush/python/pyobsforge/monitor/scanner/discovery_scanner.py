import os
import glob
from collections import defaultdict
from netCDF4 import Dataset

from .log_file_parser import parse_master_log, parse_output_files_from_log

class DiscoveryScanner:
    def __init__(self, data_root):
        self.data_root = os.path.abspath(data_root)

    def scan_cycle(self, date_str, cycle_str):
        cycle = int(cycle_str)
        print(f"\n{'='*60}\nSCANNING CYCLE: {date_str} {cycle_str}\n{'='*60}")

        # 1. Master Log Parsing
        master_log_name = f"{date_str}{cycle_str}.log"
        master_log_path = os.path.join(self.data_root, "logs", master_log_name)
        
        if not os.path.exists(master_log_path):
            print(f"  [WRN] Master Log missing: {master_log_path}")
            return {}

        tasks_found = parse_master_log(master_log_path)
        cycle_inventory = {}  # Map: TaskName -> {Categories -> [Files]}

        # 2. Per-Task Discovery
        cycle_log_dir = os.path.join(self.data_root, "logs", f"{date_str}{cycle_str}")
        
        for t in tasks_found:
            t_name = t['task_name']
            
            # Find Log
            candidates = [f"{t_name}_prep.log", f"{t_name}.log"]
            log_path = None
            for c in candidates:
                p = os.path.join(cycle_log_dir, c)
                if os.path.exists(p):
                    log_path = p
                    break
            
            print(f"    -> Task: {t_name:<30} | Status: {t['status']}")
            
            task_files = []
            if log_path:
                # A. Parse Log for "Intention" (Files it claims to create)
                claimed_files = parse_output_files_from_log(log_path, self.data_root)
                
                # B. Expand Directories if log pointed to a folder
                # If log said "Created .../chem", we scan that folder for .nc files
                expanded_files = self._expand_directories(claimed_files)
                
                # C. Validate Existence & Integrity (The Cross-Check)
                task_files = self.validate_file_inventory(expanded_files)
            else:
                print("       [WRN] No individual log found.")

            # D. Build Dynamic Map (Task -> Category -> ObsSpace)
            if task_files:
                cycle_inventory[t_name] = self._organize_by_category(task_files)
                self._print_inventory_summary(cycle_inventory[t_name])
            else:
                if log_path: print("       [OUT] No output files detected in log.")

        return {"tasks": tasks_found, "inventory": cycle_inventory}

    def _expand_directories(self, rel_paths):
        """
        If a path points to a directory, find all .nc/.bufr files inside.
        If it's a file, keep it.
        """
        expanded = set()
        for rel in rel_paths:
            full = os.path.join(self.data_root, rel)
            if os.path.isdir(full):
                # It's a directory, scan it
                for root, _, files in os.walk(full):
                    for f in files:
                        if f.endswith(('.nc', '.bufr', '.txt')): # Add extensions as needed
                            # Add relative path
                            f_abs = os.path.join(root, f)
                            expanded.add(os.path.relpath(f_abs, self.data_root))
            else:
                # It's a file (or missing path), keep as is
                expanded.add(rel)
        return list(expanded)

    def validate_file_inventory(self, rel_paths):
        """
        Cross-checks the list of paths against the filesystem.
        Returns list of dicts with status metadata.
        """
        results = []
        for rel in rel_paths:
            full_path = os.path.join(self.data_root, rel)
            
            # 1. Existence / Size
            if not os.path.exists(full_path):
                status = "MISSING"
                meta = {}
            else:
                try:
                    size = os.path.getsize(full_path)
                    if size == 0:
                        status = "EMPTY"
                        meta = {"size": 0}
                    else:
                        # 2. Content Validation
                        status, meta = self._check_content_integrity(full_path)
                        meta['size'] = size
                except OSError:
                    status = "ERR_ACC"
                    meta = {}

            results.append({
                "path": rel,
                "status": status,
                "meta": meta
            })
        return results

    def _check_content_integrity(self, filepath):
        """Checks internal file structure (NetCDF headers, etc)."""
        if filepath.endswith(".nc"):
            try:
                with Dataset(filepath, 'r') as ds:
                    n_obs = 0
                    if "Location" in ds.dimensions: n_obs = len(ds.dimensions["Location"])
                    elif "nlocs" in ds.dimensions: n_obs = len(ds.dimensions["nlocs"])
                    return "OK", {"obs_count": n_obs}
            except Exception as e:
                return "CORRUPT", {"error": str(e)[:30]}
        
        # Add BUFR or other checks here
        return "OK", {}

    def _organize_by_category(self, validated_files):
        """
        Groups files by Category (parent folder) and deduces Obs Space (filename).
        Returns: {Category: {ObsSpace: FileData}}
        """
        organized = defaultdict(dict)
        for f in validated_files:
            # Logic: 
            # Path: .../gdas.20251209/18/chem/aod_viirs.nc
            # Category = 'chem' (parent dir)
            # Obs Space = 'aod_viirs' (filename minus extension/prefix)
            
            path_parts = f['path'].split(os.sep)
            if len(path_parts) < 2: continue
            
            category = path_parts[-2]
            filename = path_parts[-1]
            
            # Simple Obs Space extraction (remove .nc, remove run_type prefix if standard)
            # e.g. "gdas.t12z.aod.nc" -> "aod"
            # Adjust regex as needed for your naming convention
            obs_space = filename
            
            organized[category][obs_space] = f
            
        return dict(organized)

    def _print_inventory_summary(self, category_map):
        for cat, spaces in category_map.items():
            print(f"       Category: [{cat}]")
            for space, data in spaces.items():
                s = data['status']
                meta = data['meta']
                print(f"         - {space:<30} : {s:<8} {meta}")
