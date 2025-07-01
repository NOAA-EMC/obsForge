#!/usr/bin/env python3

# for development purposes
from datetime import datetime

from logging import getLogger
from os import path
import subprocess
from typing import Dict, Any
from wxflow import AttrDict, FileHandler, Task, add_to_datetime, to_timedelta, logit, parse_j2yaml, save_as_yaml

logger = getLogger(__name__.split('.')[-1])


class MarineBufrObsPrep(Task):
    """
    Class for preparing and managing marine observations
    """
    def __init__(self, config: Dict[str, Any]) -> None:
        super().__init__(config)

        # TODO: for development purposes, remove when dumpdir and all the cycles line up
        self.task_config.current_cycle = datetime(2025, 4, 6, 12, 0, 0)
        self.task_config.current_cycle = datetime(2023, 6, 1, 0, 0, 0)
        self.task_config.RUN = "gdas"
        self.task_config.PDY = self.task_config.current_cycle.strftime("%Y%m%d")
        self.task_config.cyc = self.task_config.current_cycle.strftime("%H")

        _window_begin = add_to_datetime(self.task_config.current_cycle, -to_timedelta(f"{self.task_config['assim_freq']}H") / 2)
        _window_end = add_to_datetime(self.task_config.current_cycle, +to_timedelta(f"{self.task_config['assim_freq']}H") / 2)

        local_dict = AttrDict(
            {
                # TODO: make this what it's supposed to be 
                'COMOUT': f"{self.task_config.HOMEobsforge}/COMROOT/obsforge/gfs.20250428/18/ocean/",
                'window_begin': _window_begin,
                'window_end': _window_end,
                'PREFIX': f"{self.task_config.RUN}.t{self.task_config.cyc}z.",
                'bufr2ioda_config_temp': f"{self.task_config.HOMEobsforge}/parm/{self.task_config.BUFR2IODA_CONFIG_TEMP}",
                'DMPDIR': f"{self.task_config.DMPDIR_BUFR}/{self.task_config.RUN}.{self.task_config.PDY}/{self.task_config.cyc}/atmos"
            }
        )

        # task_config is everything that this task should need
        self.task_config = AttrDict(**self.task_config, **local_dict)

    @logit(logger)
    def initialize(self) -> None:
        """
        """
        logger.info("running init")
        providers = self.task_config.providers
        logger.info(f"Providers: {providers}")

        DATA = self.task_config.DATA
        DMPDIR_BUFR = self.task_config.DMPDIR_BUFR
        RUN = self.task_config.RUN
        PDY = self.task_config.PDY
        cyc = self.task_config.cyc
        PREFIX = self.task_config.PREFIX

        local_dict = AttrDict({'DATA': DATA,
                               'PDY': PDY,
                               'cyc': cyc,
                               'current_cycle': self.task_config.current_cycle,
                               'PREFIX': PREFIX,
                               'RUN': RUN})

        bufr_files_to_copy = []

        for provider in providers:

            provider.update(local_dict)
            provider_config = parse_j2yaml(self.task_config.bufr2ioda_config_temp, provider)
            logger.info(f"Provider config for {provider}: {provider_config}")
            provider.ioda_filename = provider_config['ioda_filename']
            save_as_yaml(provider_config, f"{self.task_config.DATA}/bufr2ioda_{provider['name']}.yaml")

            source_dump_filename = path.join(DMPDIR_BUFR, provider_config['dump_filename'])
            local_dump_filename = path.join(DATA, provider_config['local_dump_filename'])
            bufr_files_to_copy.append([source_dump_filename, local_dump_filename])

        FileHandler({'copy_opt': bufr_files_to_copy}).sync()

    @logit(logger)
    def execute(self) -> None:
        """
        """
        logger.info("running execute")
        HOMEobsforge = self.task_config.HOMEobsforge
        providers = self.task_config.providers

        for key, value in self.task_config.items():
            logger.info(f"task_config: {key} = {value}")

        for provider in providers:
            provider_name = provider['name']
            logger.info(f"Processing provider: {provider_name}")
            bufrconverter = f"{HOMEobsforge}/utils/b2i/bufr2ioda_{provider_name}.py"
            bufrconverterconfig = f"{self.task_config.DATA}/bufr2ioda_{provider_name}.yaml"

        try:
            subprocess.run(['python', bufrconverter, '-c', bufrconverterconfig], check=True)
        except subprocess.CalledProcessError as e:
            logger.warning(f"bufr2ioda converter failed with error  >{e}<, \
                return code {e.returncode}")

    @logit(logger)
    def finalize(self) -> None:
        """
        """
        logger.info("running finalize")

        providers = self.task_config.providers

        ioda_files_to_copy = []

        for provider in providers:
            ioda_filename = provider['ioda_filename']
            source_ioda_filename = path.join(self.task_config.DATA, ioda_filename)
            if path.exists(source_ioda_filename):
                destination_ioda_filename = path.join(self.task_config.COMOUT, ioda_filename)
                ioda_files_to_copy.append([source_ioda_filename, destination_ioda_filename])

        FileHandler({'copy_opt': ioda_files_to_copy}).sync()
