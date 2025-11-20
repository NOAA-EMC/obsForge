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


class ObsforgeMonitor(Task):
    def __init__(self, config: Dict[str, Any]) -> None:
        super().__init__(config)

        yyyymmdd = self.task_config.current_cycle.strftime("%Y%m%d")
        cycstr = self.task_config.current_cycle.strftime("%H")

        RUN = self.task_config.RUN
        OCNOBS2IODAEXEC = path.join(self.task_config.HOMEobsforge, 'build/bin/obsforge_obsprovider2ioda.x')

        _window_begin = add_to_datetime(self.task_config.current_cycle, -to_timedelta(f"{self.task_config['assim_freq']}H") / 2)
        _window_end = add_to_datetime(self.task_config.current_cycle, +to_timedelta(f"{self.task_config['assim_freq']}H") / 2)

        local_dict = AttrDict(
            {
                'COMIN_OBSPROC': f"{self.task_config.COMROOT}/{self.task_config.PSLOT}/{RUN}.{yyyymmdd}/{cycstr}/ocean/insitu",
                'window_begin': to_isotime(_window_begin),
                'window_end': to_isotime(_window_end),
                'OCNOBS2IODAEXEC': OCNOBS2IODAEXEC,
                'PREFIX': f"{RUN}.t{cycstr}z.",
                'bufr2ioda_config_temp': f"{self.task_config.HOMEobsforge}/parm/{self.task_config.BUFR2IODA_CONFIG_TEMP}",
                'cycstr': cycstr,
                'yyyymmdd': yyyymmdd
            }
        )

        self.task_config = AttrDict(**self.task_config, **local_dict)

    @logit(logger)
    def initialize(self) -> None:
        logger.info("running init")
        providers = self.task_config.providers
        logger.info(f"Providers: {providers}")

    @logit(logger)
    def execute(self) -> None:
        logger.info("running execute")
        HOMEobsforge = self.task_config.HOMEobsforge

    @logit(logger)
    def finalize(self) -> None:
        logger.info("running finalize")
