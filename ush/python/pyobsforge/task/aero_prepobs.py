#!/usr/bin/env python3

import os
import glob
import gzip
import tarfile
import re
from logging import getLogger
from typing import List, Dict, Any, Union

from wxflow import (AttrDict, FileHandler, rm_p, rmdir,
                    Task, add_to_datetime, to_timedelta, to_datetime,
                    datetime_to_YMD,
                    chdir, Executable, WorkflowException,
                    parse_j2yaml, save_as_yaml, logit)

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

    @logit(logger)
    def initialize(self) -> None:
        """
        """
        print("initialize")

    @logit(logger)
    def execute(self) -> None:
        """
        """
        print("execute")

    @logit(logger)
    def finalize(self) -> None:
        """
        """
        print("finalize")
