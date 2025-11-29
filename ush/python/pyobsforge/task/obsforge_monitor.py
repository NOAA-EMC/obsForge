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

logger = getLogger(__name__.split('.')[-1])



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

        ncdir = '/lfs/h2/emc/obsproc/noscrub/edward.givelberg/orun/RUNDIR/obsforge/gdas.2025112000/marinedump.1012686/'

        obs_type = 'JJJJJ'
        self.log_task_run_details(task_run_id, obs_type, ncdir)


    @logit(logger)
    def finalize(self) -> None:
        logger.info("running finalize")


    # likely should be a standalone method taking db as a parameter; 
    # or a db method
    @logit(logger)
    def log_task_run(self, log_path):
        job_info = parse_job_log(log_path, "marinedump.sh")

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


    def log_task_run_details(self, task_run_id, obs_type, ncdir):
        results = parse_obs_dir(obs_type, ncdir)

        print(f"Processed obs type: {obs_type}")
        for obs_space, obs_space_info in results.items():
            print_obs_space_description(obs_type, 
                obs_space,
                obs_space_info['filename'],
                obs_space_info['n_obs'])

            nc_filename = obs_space_info['filename']
            n_obs = obs_space_info['n_obs']
            obs_space_id = 7    # get this from the obs_space name
            self.db.log_task_run_detail(
                task_run_id=task_run_id,
                obs_space_id=obs_space_id,
                obs_count=n_obs,
                runtime_sec=0.0     # test None here
            )   
            logger.info(f"logged number of obs {nc_filename}  ==>>>>> {n_obs}")
