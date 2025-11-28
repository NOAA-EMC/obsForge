#!/usr/bin/env python3

from logging import getLogger
from os import path
import pathlib
from typing import Dict, Any
from wxflow import (
    AttrDict,
    Executable,
    FileHandler,
    Task,
    add_to_datetime,
    to_isotime,
    to_timedelta,
    logit,
    parse_j2yaml,
    parse_yaml,
    save_as_yaml,
)
from pyobsforge.task.sfcshp import SfcShp
import netCDF4

logger = getLogger(__name__.split('.')[-1])


import os
import re
# import csv 
# import matplotlib.pyplot as plt 
# # import plotext as plt
from datetime import datetime, timedelta
import math
import statistics

from pyobsforge.monitor_db.obsforge_monitor_db import ObsforgeMonitorDB
import sqlite3
from netCDF4 import Dataset

import glob
from os.path import join, basename



# -------------------------------
# Helper: Extract cycle
# -------------------------------
def extract_cycle_from_lines(lines):
    cycle_candidates = []
    valid_cycles = {"00", "06", "12", "18"}

    p_tXXz = re.compile(r"\bcycle\s*=\s*t([0-9]{2})z\b")
    p_export_tXXz = re.compile(r"\bexport\s+cycle\s*=\s*t([0-9]{2})z\b")
    p_export_cyc = re.compile(r"\bexport\s+cyc=['\"]?([0-9]{2})['\"]?")
    p_current_cycle_dt = re.compile(r"current cycle:\s*([0-9\-]+\s+[0-9:]+)")
    p_previous_cycle = re.compile(r"previous cycle:", re.IGNORECASE)

    for line in lines:
        if p_previous_cycle.search(line):
            continue

        m = p_tXXz.search(line) or p_export_tXXz.search(line)
        if m:
            cyc = m.group(1)
            if cyc in valid_cycles:
                cycle_candidates.append(cyc)
            continue

        m = p_export_cyc.search(line)
        if m:
            cyc = m.group(1)
            if cyc in valid_cycles:
                cycle_candidates.append(cyc)
            continue

        m = p_current_cycle_dt.search(line)
        if m:
            dt_str = m.group(1)
            try:
                dt = datetime.strptime(dt_str, "%Y-%m-%d %H:%M:%S")
                cyc = dt.strftime("%H")
                if cyc in valid_cycles:
                    cycle_candidates.append(cyc)
            except Exception:
                pass

    if not cycle_candidates:
        return None

    unique = set(cycle_candidates)
    if len(unique) > 1:
        raise ValueError(f"Inconsistent cycle definitions found: {sorted(unique)}")

    return cycle_candidates[0]

# -------------------------------
# Helper: Extract run_type
# -------------------------------
def extract_run_type_from_lines(lines):
    pattern = re.compile(r"export\s+RUN=['\"]?(gdas|gfs)['\"]?", re.IGNORECASE)
    run_types = []

    for line in lines:
        m = pattern.search(line)
        if m:
            run_types.append(m.group(1).lower())

    if not run_types:
        return None

    unique = set(run_types)
    if len(unique) > 1:
        raise ValueError(f"Inconsistent run_type definitions found: {sorted(unique)}")

    return run_types[0]

# -------------------------------
# Helper: Extract start/end/elapsed/error
# -------------------------------
def extract_job_times_from_lines(lines, job_script):
    begin_pattern = re.compile(rf"Begin {re.escape(job_script)} at (.+)")
    end_pattern = re.compile(
        rf"End {re.escape(job_script)} at ([0-9:]+).*?error code (\d+).*?\(time elapsed: ([0-9:]+)\)"
    )

    start_date = end_date = elapsed_time = error_code = None

    for line in lines:
        m = begin_pattern.search(line)
        if m:
            start_str = m.group(1).strip()
            try:
                start_date = datetime.strptime(start_str, "%a %b %d %H:%M:%S %Z %Y")
            except ValueError:
                try:
                    start_date = datetime.strptime(start_str, "%a %b %d %H:%M:%S %Y")
                except ValueError:
                    start_date = start_str
            continue

        m = end_pattern.search(line)
        if m:
            end_str = m.group(1).strip()
            error_code = int(m.group(2))
            elapsed_str = m.group(3).strip()

            if start_date and isinstance(start_date, datetime):
                end_date = datetime.strptime(
                    f"{start_date.strftime('%Y-%m-%d')} {end_str}",
                    "%Y-%m-%d %H:%M:%S"
                )
            else:
                end_date = end_str

            try:
                h, mn, s = map(int, elapsed_str.split(":"))
                elapsed_time = timedelta(hours=h, minutes=mn, seconds=s)
            except Exception:
                elapsed_time = elapsed_str
            continue

    return start_date, end_date, elapsed_time, error_code

# -------------------------------
# Main parse function
# -------------------------------
@logit(logger)
def parse_job_log(logfile_path: str, job_script: str):
    if not os.path.isfile(logfile_path):
        raise FileNotFoundError(f"Log file does not exist: {logfile_path}")

    with open(logfile_path, "r", encoding="utf-8") as f:
        lines = f.readlines()

    cycle = extract_cycle_from_lines(lines)
    run_type = extract_run_type_from_lines(lines)
    start_date, end_date, elapsed_time, error_code = extract_job_times_from_lines(lines, job_script)

    if start_date is None and end_date is None and cycle is None and run_type is None:
        return None

    return {
        "start_date": start_date,
        "end_date": end_date,
        "elapsed_time": elapsed_time,
        "error_code": error_code,
        "cycle": cycle,
        "run_type": run_type
    }



'''
helper function for parsing the log
'''
def elapsed_to_seconds(elapsed):
    if isinstance(elapsed, timedelta):
        return int(elapsed.total_seconds())
    if isinstance(elapsed, str):
        h, m, s = map(int, elapsed.split(":"))
        return h*3600 + m*60 + s
    return None


# this might be moved to the db class (?)
def is_valid_sqlite(db_path: str) -> bool:
    """
    Returns True if db_path is a valid SQLite database, False otherwise.
    """

    # File must exist
    if not os.path.isfile(db_path):
        return False

    # Must be large enough to contain SQLite header
    if os.path.getsize(db_path) < 100:
        return False

    # Try opening the DB and running PRAGMA integrity_check
    try:
        conn = sqlite3.connect(db_path)
        cur = conn.cursor()
        cur.execute("PRAGMA integrity_check;")
        result = cur.fetchone()
        conn.close()

        return result and result[0].lower() == "ok"

    except sqlite3.Error:
        return False



def read_number_of_ioda_obs(ncfile):
    """
    Returns the number of observations in an IODA-style NetCDF file.
    Specifically, it reads the 'Location' dimension.
    """

    if not os.path.isfile(ncfile):
        raise FileNotFoundError(f"File not found: {ncfile}")

    with Dataset(ncfile, "r") as ds:
        if "Location" not in ds.dimensions:
            raise KeyError("The NetCDF file does not contain a 'Location' dimension.")
        return len(ds.dimensions["Location"])



# marine dump names -- copied, to be corrected
def obs_space_output_file_name(obs_space: str, cfg):
    return f"{cfg['RUN']}.t{cfg['cyc']:02d}z.{obs_space}.nc"

def gather_output_file_names(cfg, providers):
    filenames = []
    for provider, obs_spaces in providers.items():
        for obs_space in obs_spaces["list"]:
            output_file_name = obs_space_output_file_name(obs_space, cfg)
            filenames.append(output_file_name)
    return filenames



def get_marine_obs_map(task_config):
    """
    Return a mapping of obs_type → {
        'dest_dir': <destination directory>, 
        'dest_files': [list of destination file paths]
    }
    """
    # Build YYYYMMDD and base COMROOT path
    yyyymmdd = task_config['PDY'].strftime('%Y%m%d')

    comout = join(task_config['COMROOT'],
                  task_config['PSLOT'],
                  f"{task_config['RUN']}.{yyyymmdd}",
                  f"{task_config['cyc']:02d}",
                  'ocean')

    PREFIX = f"{task_config.RUN}.t{task_config.cyc:02d}z."

    obs_types = ['sst', 'adt', 'icec', 'sss']
    results = {}

    for obs_type in obs_types:

        # Destination directory for this obs type
        dest_dir = join(comout, obs_type)

        # Find matching IODA files
        pattern = join(
            task_config['DATA'],
            f"{PREFIX}*{obs_type}_*.nc"
        )
        ioda_files = glob.glob(pattern)
        logger.info(f'obs_type =|{obs_type}|')
        logger.info(f'pattern =|{pattern}|')
        logger.info(f'dest_dir =|{dest_dir}|')
        logger.info(f'ioda_files =|{ioda_files}|')

        # Build list of destination file paths
        dest_files = [
            join(dest_dir, basename(f)) for f in ioda_files
        ]

        # Store results
        results[obs_type] = {
            'dest_dir': dest_dir,
            'dest_files': dest_files
        }

    return results




class ObsforgeMonitor(Task):
    def __init__(self, config: Dict[str, Any]) -> None:
        super().__init__(config)

        # logger.info(f"@@@@@@@@@@++++++++++++ : {self.task_config} ========================>  ")

        self.db_path = self.task_config.database

        logger.info(f"Using database: {self.db_path}")
        for name, cfg in self.task_config.dump_tasks.items():
            logger.info(f"dump tasks : {name} ==>  {cfg}")

        self.marinedump_config = self.task_config.dump_tasks['marinedump']
        logger.info(f"++++++++++++ : {self.marinedump_config} ========================>  ")

# marine_obsprep = MarineObsPrep()
        output_file_names = gather_output_file_names(self.task_config, self.marinedump_config.providers)
        logger.info(f"========= OUTPUT FILE NAMES: {output_file_names}")


        result = get_marine_obs_map(self.task_config)
        for obs_type, info in result.items():
            logger.info(f"Obs type: {obs_type}")
            logger.info(f"  Destination directory: {info['dest_dir']}")
            logger.info("  Files:")
            for f in info['dest_files']:
                logger.info(f">>>>>>>>>>>>>>>>>>>>>>>>>>>   {f}")



    @logit(logger)
    def initialize(self) -> None:
        logger.info("running init")

        # db initialization -- to be redone
        # need to check if the db exists, if not initialize

        self.db = ObsforgeMonitorDB(self.db_path)

        if not is_valid_sqlite(self.db_path):
            logger.info("Database doesn't exist or is invalid — initializing...")
            self.db.init_db()
        logger.info(f"Using database: {self.db_path}")

        for provider, obs_spaces in self.marinedump_config.providers.items():
            logger.info(f"========= provider: {provider}")
            for obs_space in obs_spaces["list"]:
                logger.info(f"========= obs_space: {obs_space}")




    @logit(logger)
    def execute(self) -> None:
        logger.info("running execute")

        log_path = '/lfs/h2/emc/obsproc/noscrub/edward.givelberg/orun/COMROOT/obsforge/logs/2025112000/gdas_marine_dump_prep.log'

        task_run_id = self.log_task_run(log_path)




        ncfile_path = '/lfs/h2/emc/obsproc/noscrub/edward.givelberg/orun/COMROOT/obsforge/gdas.20251111/18/chem/gdas.t18z.retrieval_aod_viirs_n20.nc'
        n_obs = read_number_of_ioda_obs(ncfile_path)

        obs_space_id = 7
        self.db.log_task_run_detail(
            task_run_id=task_run_id,
            obs_space_id=obs_space_id,
            obs_count=n_obs,
            runtime_sec=0.0     # test None here
        )   

        logger.info(f"logged number of obs {ncfile_path}  ==>>>>> {n_obs}")


        # Add details for each obs space
        # for s_id in obs_spaces:
            # db.log_task_run_detail(
                # task_run_id=run_id,
                # obs_space_id=s_id,
                # obs_count=random.randint(10_000, 200_000),
                # runtime_sec=random.randint(10, 200)
            # )   


    @logit(logger)
    def finalize(self) -> None:
        logger.info("running finalize")


    # likely should be a standalone method taking db as a parameter; 
    # or a db method
    @logit(logger)
    def log_task_run(self, log_path):
        job_info = parse_job_log(log_path, "marinedump.sh")
        # cycle = extract_cycle(log_path)

        logger.info("===========================================")
        logger.info(f"log_path ==>>>>> {log_path}")
        logger.info(job_info["start_date"])
        logger.info(job_info["end_date"])
        logger.info(job_info["error_code"])
        logger.info(job_info["elapsed_time"])
        logger.info(f'Elapsed time: {elapsed_to_seconds(job_info["elapsed_time"])} seconds')
        logger.info(f'Parsed job_info = {job_info}')
        logger.info("===========================================")

        # TODO: perhaps extract the name from log file
        task_id = 3     # needs lookup by name
        today = datetime.utcnow().date()
        cycle = job_info["cycle"]
        run_type = job_info["run_type"]
        start_date = job_info["start_date"]
        end_date = job_info["end_date"]
        # logger.info(f"extracted cycle ==>>>>> {cycle}")
        # logger.info(f"extracted run_type ==>>>>> {run_type}")

        task_run_id = self.db.log_task_run(
            task_id=task_id,
            date=today.isoformat(),
            cycle=cycle,
            run_type=run_type,
            start_time=start_date.isoformat(),
            end_time=end_date.isoformat(),
            notes=None
        )

        logger.info(f"logged task run id ==>>>>> {task_run_id}")
        logger.info("===========================================")

        return task_run_id
