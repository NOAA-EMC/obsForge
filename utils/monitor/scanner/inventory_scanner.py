import glob
import logging
import os
import re
from datetime import datetime

import numpy as np
from netCDF4 import Dataset, num2date

from .log_file_parser import parse_master_log, parse_output_files_from_log
from .models import CycleData, FileInventoryData, TaskRunData
from .persistence import ScannerStateReader

logger = logging.getLogger("InventoryScanner")


class InventoryScanner:
    """
    Scans the filesystem.
    Incremental Logic: Only opens NetCDF files if mtime > known_state.
    """

    VALID_PREFIXES = {'gdas', 'gfs', 'gcdas'}

    def __init__(self, data_root, known_state: dict = None):
        self.data_root = os.path.abspath(data_root)
        # Path -> {mtime}
        self.known_state = known_state or {}
        logger.debug(f"INIT: InventoryScanner root={self.data_root}")

    def scan_filesystem(self, known_cycles: set = None) -> list:
        logs_root = os.path.join(self.data_root, "logs")
        if not os.path.isdir(logs_root):
            logger.error(f"Logs directory not found: {logs_root}")
            return []

        pattern = os.path.join(logs_root, "[0-9]*.log")
        master_logs = sorted(glob.glob(pattern))

        for m_log_path in master_logs:
            filename = os.path.basename(m_log_path)
            m = re.match(r"(\d{8})(\d{2})\.log", filename)
            if not m:
                continue

            date_str, cycle_int = m.group(1), int(m.group(2))
            logger.info(f"Scanning Cycle via Master Log: {filename}")

            cycle_obj = self._process_cycle(date_str, cycle_int, m_log_path)
            if cycle_obj.tasks:
                yield cycle_obj

    def _process_cycle(self, date_str, cycle_int, master_log_path):
        cycle_obj = CycleData(date=date_str, cycle=cycle_int)
        raw_tasks = parse_master_log(master_log_path)
        unique_tasks = {t['task_name']: t for t in raw_tasks}

        cycle_log_dir = os.path.join(
            self.data_root, "logs", f"{date_str}{cycle_int:02d}"
        )

        for t in unique_tasks.values():
            raw_name = t['task_name']
            run_type, task_name = self._normalize_task_name(raw_name)

            task_data = TaskRunData(
                task_name=task_name,
                run_type=run_type,
                logfile="missing",
                job_id=t['job_id'],
                status=t['status'],
                exit_code=t['exit_code'],
                attempt=t['attempt'],
                host=t['host'],
                runtime_sec=t['duration'],
                start_time=t.get('start_time'),
                end_time=t.get('end_time')
            )

            for c in [f"{raw_name}_prep.log", f"{raw_name}.log"]:
                p = os.path.join(cycle_log_dir, c)
                if os.path.exists(p):
                    task_data.logfile = p
                    files = parse_output_files_from_log(p, self.data_root)
                    task_data.files = self.validate_file_inventory(
                        self._expand_directories(files)
                    )
                    break

            cycle_obj.tasks.append(task_data)
        return cycle_obj

    def _normalize_task_name(self, raw_name):
        parts = raw_name.split('_')
        prefix = parts[0]
        if prefix in self.VALID_PREFIXES:
            return prefix, raw_name[len(prefix)+1:]
        return 'unknown', raw_name

    def _expand_directories(self, rel_paths):
        expanded = set()
        for rel in rel_paths:
            full = os.path.join(self.data_root, rel)
            if os.path.isdir(full):
                for root, _, files in os.walk(full):
                    for f in files:
                        if f.endswith(('.nc', '.bufr')):
                            expanded.add(
                                os.path.relpath(
                                    os.path.join(root, f), self.data_root
                                )
                            )
            else:
                expanded.add(rel)
        return list(expanded)

    def validate_file_inventory(self, rel_paths):
        inventory_list = []
        for rel in rel_paths:
            full_path = os.path.join(self.data_root, rel)
            path_parts = rel.split(os.sep)
            category = path_parts[-2] if len(path_parts) > 1 else "unknown"
            filename = path_parts[-1]
            obs_space = self._clean_obs_space_name(filename)

            integrity = "UNKNOWN"
            size = 0
            obs_count = 0
            mtime = 0
            error = None
            props = {}
            stats = []
            domain = None

            if not os.path.exists(full_path):
                integrity = "MISSING"
            else:
                try:
                    stat_info = os.stat(full_path)
                    size = stat_info.st_size
                    mtime = int(stat_info.st_mtime)

                    if size == 0:
                        integrity = "EMPTY"
                    else:
                        # INCREMENTAL CHECK
                        history = self.known_state.get(rel)
                        prev_mtime = history['mtime'] if history else 0

                        if mtime > prev_mtime:
                            # CHANGED: Deep Scan
                            res = self._check_content_integrity(full_path)
                            integrity, meta, stats, domain, anomalies = res
                            obs_count = meta.get('obs', 0)
                            error = meta.get('err')
                            props = meta
                            if anomalies:
                                props['outliers'] = anomalies
                        else:
                            # UNCHANGED: Skip Parsing
                            # We send placeholder data.
                            # The DB Gatekeeper will ignore this.
                            integrity = "OK_SKIPPED"

                except OSError as e:
                    integrity = "ERR_ACC"
                    error = str(e)

            inventory_list.append(FileInventoryData(
                rel_path=rel,
                category=category,
                obs_space_name=obs_space,
                integrity=integrity,
                size_bytes=size,
                mtime=mtime,
                obs_count=obs_count,
                error_msg=error,
                properties=props,
                stats=stats,
                domain=domain
            ))
        return inventory_list

    def _clean_obs_space_name(self, filename):
        prefixes = "|".join(self.VALID_PREFIXES)
        pattern = rf"^(?:{prefixes})\.t[0-9]{{2}}z\.(.+)\.(?:nc|bufr)$"
        m = re.match(pattern, filename, re.IGNORECASE)
        if m:
            return m.group(1)
        return os.path.splitext(filename)[0]

    def _check_content_integrity(self, filepath):
        if not filepath.endswith(".nc"):
            return "OK", {"obs": 0}, [], None, []
        try:
            with Dataset(filepath, 'r') as ds:
                meta = {}
                n = 0
                if "Location" in ds.dimensions:
                    n = len(ds.dimensions["Location"])
                elif "nlocs" in ds.dimensions:
                    n = len(ds.dimensions["nlocs"])
                if n == 0 and 'ObsValue' in ds.groups:
                    grp = ds.groups['ObsValue']
                    for var_name in grp.variables:
                        if grp.variables[var_name].dimensions:
                            dim_name = grp.variables[var_name].dimensions[0]
                            n = len(ds.dimensions[dim_name])
                            break
                meta['obs'] = n
                for attr in ds.ncattrs():
                    try:
                        meta[attr] = str(getattr(ds, attr))
                    except Exception:
                        pass

                meta['schema'] = self._extract_full_schema(ds)
                self._infer_dimensionality(meta['schema'])
                domain = self._extract_domain(ds)
                stats, anomalies = self._calculate_statistics(ds)
                return "OK", meta, stats, domain, anomalies
        except Exception as e:
            return "CORRUPT", {"err": str(e)}, [], None, []

    def _extract_full_schema(self, ds_or_group, parent_path=""):
        schema = {}
        for var_name, var_obj in ds_or_group.variables.items():
            full_name = (
                f"{parent_path}/{var_name}" if parent_path else var_name
            )
            schema[full_name] = {
                'type': str(var_obj.dtype),
                'dims': ",".join(var_obj.dimensions),
                'ndim': 1
            }
        for group_name, group_obj in ds_or_group.groups.items():
            new_path = (
                f"{parent_path}/{group_name}" if parent_path else group_name
            )
            schema.update(self._extract_full_schema(group_obj, new_path))
        return schema

    def _infer_dimensionality(self, schema):
        for path, meta in schema.items():
            if path.startswith("MetaData/"):
                meta['ndim'] = 1
            elif "Surface" in path:
                meta['ndim'] = 2
            elif path.startswith(("ObsValue/", "ObsError/", "PreQC/")):
                is_3d = any(
                    v in schema for v in
                    ['MetaData/depth', 'MetaData/pressure']
                )
                meta['ndim'] = 3 if is_3d else 2
            else:
                meta['ndim'] = 1

    def _extract_domain(self, ds):
        d = {}
        try:
            if 'MetaData' in ds.groups:
                md = ds.groups['MetaData']
                if 'dateTime' in md.variables:
                    t_var = md.variables['dateTime']
                    times = t_var[:]
                    if np.ma.is_masked(times):
                        times = times.compressed()
                    if len(times) > 0:
                        min_t, max_t = np.min(times), np.max(times)
                        if hasattr(t_var, 'units'):
                            try:
                                d['start'] = int(num2date(
                                    min_t, units=t_var.units,
                                    calendar=getattr(t_var, 'calendar', 'standard')
                                ).timestamp())
                                d['end'] = int(num2date(
                                    max_t, units=t_var.units,
                                    calendar=getattr(t_var, 'calendar', 'standard')
                                ).timestamp())
                            except Exception:
                                d['start'], d['end'] = int(min_t), int(max_t)
                        else:
                            d['start'], d['end'] = int(min_t), int(max_t)
                
                axes = [
                    ('latitude', 'min_lat', 'max_lat'),
                    ('longitude', 'min_lon', 'max_lon')
                ]
                for ax, k_min, k_max in axes:
                    if ax in md.variables:
                        vals = md.variables[ax][:]
                        if np.ma.is_masked(vals):
                            vals = vals.compressed()
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
            if g not in ds.groups:
                continue
            grp = ds.groups[g]
            for v_name, v_obj in grp.variables.items():
                if v_obj.dtype == np.str_ or v_obj.dtype == np.object_:
                    continue
                if 'dateTime' in v_name or 'time' in v_name.lower():
                    continue
                try:
                    d = v_obj[:]
                    if np.ma.is_masked(d):
                        d = d.compressed()
                    if np.any(np.abs(d) > THRESHOLD):
                        anomalies.append(f"Unmasked Fill Value in {v_name}")
                    d = d[np.abs(d) < THRESHOLD]
                    if len(d) > 0:
                        stats.append({
                            'name': f"{g}/{v_name}",
                            'min': float(np.min(d)),
                            'max': float(np.max(d)),
                            'mean': float(np.mean(d)),
                            'std': float(np.std(d))
                        })
                except Exception:
                    pass
        return stats, anomalies
