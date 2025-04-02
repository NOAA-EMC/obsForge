#!/usr/bin/env python3

from logging import getLogger
from typing import Dict, Any

from wxflow import (AttrDict, Task, add_to_datetime, to_timedelta,
                    logit)
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
        print("finalize")
