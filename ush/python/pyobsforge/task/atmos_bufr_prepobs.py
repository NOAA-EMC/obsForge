#!/usr/bin/env python3

import glob
import os
from logging import getLogger
from typing import Dict, Any

from wxflow import (AttrDict, Task, add_to_datetime, to_timedelta,
                    logit, FileHandler)
import pathlib

logger = getLogger(__name__.split('.')[-1])


class AtmosBufrObsPrep(Task):
    """
    Class for preparing and managing atmospheric BUFR observations
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
        # Do nothing right now

    @logit(logger)
    def execute(self) -> None:
        """
        """
        print("Do nothing for now")

    @logit(logger)
    def finalize(self) -> None:
        """
        """
        # create an empty file to tell external processes the obs are ready
        ready_file = pathlib.Path(os.path.join(comout, f"{self.task_config['OPREFIX']}obsforge_atmos_bufr_status.log"))
        ready_file.touch()
