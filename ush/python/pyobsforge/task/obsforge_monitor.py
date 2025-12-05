#!/usr/bin/env python3

import os
from logging import getLogger

from wxflow import Task

from pyobsforge.monitor.monitor_db import MonitorDB
import pyobsforge.monitor.timeutil as timeutil
from pyobsforge.monitor.cycle_monitor import CycleMonitor


logger = getLogger("ObsforgeMonitor")


class ObsforgeMonitor(Task):
    """
    Master monitoring task (wxflow Task).
    delegates core monitoring to CycleMonitor.
    """

    def __init__(self, config):
        super().__init__(config)
        self.config = config
        
        db = MonitorDB(config.database)
        
        self.cycle_monitor = CycleMonitor(
            data_root=config.data_root, 
            db=db, 
            task_cfgs=config.tasks
        )


    def run(self):
        logger.info("=== Obsforge Monitor Dispatcher Starting ===")
        # logger.debug(f"=== Obsforge Monitor Dispatcher config {self.config} ===")

        # Determine timestamps
        if "time_range" in self.config:
            start = self.config.time_range["start"]
            end = self.config.time_range["end"]
            timestamps = list(timeutil.iter_timestamps(start, end))
            logger.info(f"Standalone mode: {len(timestamps)} timestamps")
        else:
            pdy_raw = self.config.PDY        # any Rocoto format
            cyc_raw = self.config.cyc        # maybe "0", maybe "00", maybe weird
            ts = timeutil.normalize_rocoto_timestamp(pdy_raw, cyc_raw)

            timestamps = [timeutil.parse_timestamp(ts)]
            logger.info(f"Rocoto mode: timestamp {ts}")

        for date, cycle in timestamps:
            self.cycle_monitor.run_cycle(date, cycle)
