#!/usr/bin/env python3

from logging import getLogger
from typing import Dict, Any
from wxflow import AttrDict, Task, add_to_datetime, to_timedelta, logit

logger = getLogger(__name__.split('.')[-1])


class MarineBufrObsPrep(Task):
    """
    Class for preparing and managing marine observations
    """
    def __init__(self, config: Dict[str, Any]) -> None:
        super().__init__(config)

        _window_begin = add_to_datetime(self.task_config.current_cycle, -to_timedelta(f"{self.task_config['assim_freq']}H") / 2)
        _window_end = add_to_datetime(self.task_config.current_cycle, +to_timedelta(f"{self.task_config['assim_freq']}H") / 2)

        local_dict = AttrDict(
            {
                'window_begin': _window_begin,
                'window_end': _window_end,
                'PREFIX': f"{self.task_config.RUN}.t{self.task_config.cyc:02d}z.",
            }
        )

        # task_config is everything that this task should need
        self.task_config = AttrDict(**self.task_config, **local_dict)

    @logit(logger)
    def initialize(self) -> None:
        """
        """
        logger.info("running init")

    @logit(logger)
    def execute(self) -> None:
        """
        """
        logger.info("running execute")

    @logit(logger)
    def finalize(self) -> None:
        """
        """
        logger.info("running finalize")
