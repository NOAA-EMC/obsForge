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



def parse_time_elapsed(timestr: str) -> int:
    """HH:MM:SS -> seconds (int). Returns 0 on parse failure."""
    try:
        h, m, s = map(int, timestr.strip().split(":"))
        return h * 3600 + m * 60 + s 
    except Exception:
        return 0


@logit(logger)
def extract_timing_from_log(log_path):
    pattern = re.compile(
        r"End marinebufrdump\.sh at .*? with error code (\d+) \(time elapsed: (\d{2}:\d{2}:\d{2})\)"
    )   

    with open(log_path, "r") as f:
        lines = f.readlines()

    # Search backwards
    for line in reversed(lines):
        match = pattern.search(line)
        if match:
            error_code = int(match.group(1))
            if error_code == 0:
                elapsed = parse_time_elapsed(match.group(2))
            else:
                elapsed = 0  # present but failed run
            print(f"Elapsed for {log_path} = {elapsed}")
            logger.info(f"elapsed =======: {elapsed}")
            return elapsed

    # If nothing matched
    print(f"No matching log entry found in {log_path}")
    return None


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



class ObsforgeMonitor(Task):
    def __init__(self, config: Dict[str, Any]) -> None:
        super().__init__(config)

        self.db_path = self.task_config.database
        logger.info(f"Using database: {self.db_path}")


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


    @logit(logger)
    def execute(self) -> None:
        logger.info("running execute")

        log_path = '/lfs/h2/emc/obsproc/noscrub/edward.givelberg/orun/COMROOT/obsforge/logs/2025111118/gdas_marine_bufr_dump_prep.log'
        elapsed = extract_timing_from_log(log_path)
        logger.info("===========================================")
        logger.info(f"log_path ==>>>>> {log_path}")
        logger.info(f"running time ==>>>>> {elapsed}")
        logger.info("===========================================")


        today = datetime.utcnow().date()
        day = today

        task_id = 3     # needs lookup by name
        date_str = today.isoformat()
        cycle = 12

        start = datetime(day.year, day.month, day.day, cycle, 0, 0)
        end = start + timedelta(seconds=elapsed)

        task_run_id = self.db.log_task_run(
            task_id=task_id,
            date=date_str,
            cycle=cycle,
            run_type="gdas",
            start_time=start.isoformat(),
            end_time=end.isoformat(),
            notes=None
        )

        logger.info(f"logged task run id ==>>>>> {task_run_id}")
        logger.info("===========================================")

    @logit(logger)
    def finalize(self) -> None:
        logger.info("running finalize")

