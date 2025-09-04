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


    @logit(logger)
    def initialize(self) -> None:
        """
        Initialize an atmospheric BUFR observation prep task

        This method will initialize an atmospheric BUFR observation prep task.
        This includes:
        - Staging input BUFR files
        - Staging configuration files
        """
        # Do nothing right now

    @logit(logger)
    def execute(self) -> None:
        """
        Execute converters from BUFR to IODA format for atmospheric observations
        """
        #  ${obsforge_dir}/build/bin/bufr2netcdf.x "$input_file" "${mapping_file}" "$output_file"
        print("Do nothing for now")

    @logit(logger)
    def finalize(self) -> None:
        """
        Finalize an atmospheric BUFR observation prep task

        This method will finalize an atmospheric BUFR observation prep task.
        This includes:
        - Creating an output directory in COMOUT
        - Copying output IODA files to COMOUT
        - Creating a "ready" file in COMOUT to signal that the observations are ready
        """
        comout = os.path.join(self.task_config['COMROOT'],
                              self.task_config['PSLOT'],
                              f"{self.task_config['RUN']}.{yyyymmdd}",
                              f"{self.task_config['cyc']:02d}",
                              'atmos')
        # create an empty file to tell external processes the obs are ready
        ready_file = pathlib.Path(os.path.join(comout, f"{self.task_config['OPREFIX']}obsforge_atmos_bufr_status.log"))
        ready_file.touch()
