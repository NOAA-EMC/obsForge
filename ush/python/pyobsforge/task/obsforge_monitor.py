#!/usr/bin/env python3

from logging import getLogger
import os
from os.path import join, basename
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

import netCDF4
from netCDF4 import Dataset

import re
from datetime import datetime, timedelta
import math
import statistics

import sqlite3

import glob

from pyobsforge.monitor_db.obsforge_monitor_db import ObsforgeMonitorDB
from pyobsforge.task.log_file_parser import *
from pyobsforge.task.monitor_util import *
from pyobsforge.task.monitored_task import MonitoredTask


logger = getLogger(__name__.split('.')[-1])


class ObsforgeMonitor(Task):
    def __init__(self, config: Dict[str, Any]) -> None:
        super().__init__(config)

        self.db_path = self.task_config.database
        logger.info(f"Using database: {self.db_path}")

        monitored_tasks_config = self.task_config.tasks
        self.monitored_tasks = self.load_monitored_tasks(monitored_tasks_config, self.task_config.dump_tasks)



        # logger.info(f"@@@@@@@@@@++++++++++++ : {self.task_config} ========================>  ")

        # for name, cfg in self.task_config.dump_tasks.items():
            # logger.info(f"dump tasks : {name} ==>  {cfg}")
        '''
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
        '''



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

        '''
        for provider, obs_spaces in self.marinedump_config.providers.items():
            logger.info(f"========= provider: {provider}")
            for obs_space in obs_spaces["list"]:
                logger.info(f"========= obs_space: {obs_space}")
        '''



    @logit(logger)
    def execute(self) -> None:

        obs_type = 'JJJJJ'

        for task_name, task in self.monitored_tasks.items():
            task_run_id = task.log_task_run(self.db)
            if task_run_id:
                task.log_task_run_details(self.db, task_run_id, obs_type)


    @logit(logger)
    def finalize(self) -> None:
        logger.info("running finalize")


    def load_monitored_tasks(self, tasks_cfg, dump_tasks):
        monitored_tasks = {}

        for name, info in tasks_cfg.items():
            if name not in dump_tasks:
                raise KeyError(f"Monitored task '{name}' not found in dump_tasks.")

            task = MonitoredTask(
                name=name,
                logfile=info["logfile"],
                nc_dir=info["nc_dir"],
                dump_task_config=dump_tasks[name],
                logger=logger,
            )

            logger.info(f"Loaded monitored task: {task}")
            monitored_tasks[name] = task

        return monitored_tasks
