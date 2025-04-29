#!/usr/bin/env python3

import glob
import os
from logging import getLogger
from typing import Dict, Any

from wxflow import (AttrDict, Task, add_to_datetime, to_timedelta,
                    logit, FileHandler)
from pyobsforge.obsdb.jrr_aod_db import JrrAodDatabase
from pyobsforge.task.run_nc2ioda import run_nc2ioda

logger = getLogger(__name__.split('.')[-1])


class AerosolObsPrep(Task):
    """
    Class for preparing and managing aerosol observations
    """
    def __init__(self, config: Dict[str, Any]) -> None:
        super().__init__(config)

        _window_begin = add_to_datetime(self.task_config.current_cycle, -to_timedelta(f"{self.task_config['assim_freq']}H") / 2)
        _window_end = add_to_datetime(self.task_config.current_cycle, +to_timedelta(f"{self.task_config['assim_freq']}H") / 2)

        local_dict = AttrDict(
            {
                'window_begin': _window_begin,
                'window_end': _window_end,
                'OPREFIX': f"{self.task_config.RUN}.t{self.task_config.cyc:02d}z.",
                'APREFIX': f"{self.task_config.RUN}.t{self.task_config.cyc:02d}z."
            }
        )

        # task_config is everything that this task should need
        self.task_config = AttrDict(**self.task_config, **local_dict)

        # Initialize the JRR_AOD database
        self.jrr_aod_db = JrrAodDatabase(db_name="jrr_aod_obs.db",
                                         dcom_dir=self.task_config.DCOMROOT,
                                         obs_dir="jrr_aod")

    @logit(logger)
    def initialize(self) -> None:
        """
        """
        # Update the database with new files
        self.jrr_aod_db.ingest_files()

    @logit(logger)
    def execute(self) -> None:
        """
        """
        for platform in self.task_config.platforms:
            print(f"========= platform: {platform}")
            input_files = self.jrr_aod_db.get_valid_files(window_begin=self.task_config.window_begin,
                                                          window_end=self.task_config.window_end,
                                                          dst_dir='jrr_aod',
                                                          satellite=platform)
            logger.info(f"number of valid files: {len(input_files)}")

            if len(input_files) > 0:
                print(f"number of valid files: {len(input_files)}")
                obs_space = 'jrr_aod'
                output_file = f"{self.task_config['RUN']}.t{self.task_config['cyc']:02d}z.{obs_space}.tm00.nc"
                context = {'provider': 'VIIRSAOD',
                           'window_begin': self.task_config.window_begin,
                           'window_end': self.task_config.window_end,
                           'thinning_threshold': 0,
                           'input_files': input_files,
                           'output_file': output_file}
                result = run_nc2ioda(self.task_config, obs_space, context)
                logger.info(f"run_nc2ioda result: {result}")

    @logit(logger)
    def finalize(self) -> None:
        """
        """
        # Copy the processed ioda files to the destination directory
        logger.info("Copying ioda files to destination COMROOT directory")
        yyyymmdd = self.task_config['PDY'].strftime('%Y%m%d')

        comout = os.path.join(self.task_config['COMROOT'],
                              self.task_config['PSLOT'],
                              f"{self.task_config['RUN']}.{yyyymmdd}",
                              f"{self.task_config['cyc']:02d}",
                              'chem')

        # Loop through the observation types
        obs_types = ['jrr_aod']
        src_dst_obs_list = []  # list of [src_file, dst_file]
        for obs_type in obs_types:
            # Create the destination directory
            comout_tmp = os.path.join(comout, obs_type)
            FileHandler({'mkdir': [comout_tmp]}).sync()

            # Glob the ioda files
            ioda_files = glob.glob(os.path.join(self.task_config['DATA'],
                                                f"{self.task_config['OPREFIX']}*{obs_type}_*.nc"))
            for ioda_file in ioda_files:
                logger.info(f"ioda_file: {ioda_file}")
                src_file = ioda_file
                dst_file = os.path.join(comout_tmp, basename(ioda_file))
                src_dst_obs_list.append([src_file, dst_file])

        logger.info("Copying ioda files to destination COMROOT directory")
        logger.info(f"src_dst_obs_list: {src_dst_obs_list}")

        FileHandler({'copy': src_dst_obs_list}).sync()
