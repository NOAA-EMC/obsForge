#!/usr/bin/env python3

from logging import getLogger
from os import path
from typing import Dict, Any
from wxflow import (
    AttrDict,
    Executable,
    FileHandler,
    Task,
    add_to_datetime,
    to_timedelta,
    logit,
    parse_j2yaml,
    save_as_yaml,
)

logger = getLogger(__name__.split('.')[-1])


class MarineBufrObsPrep(Task):
    """
    Class for preparing and managing marine observations
    """
    def __init__(self, config: Dict[str, Any]) -> None:
        super().__init__(config)

        logger.info(f"self.task_config.PDY: {self.task_config.PDY}")
        logger.info(f"self.task_config.cyc: {self.task_config.cyc}")

        yyyymmdd = self.task_config.current_cycle.strftime("%Y%m%d")
        cyc = self.task_config.current_cycle.strftime("%H")
        RUN = self.task_config.RUN

        _window_begin = add_to_datetime(self.task_config.current_cycle, -to_timedelta(f"{self.task_config['assim_freq']}H") / 2)
        _window_end = add_to_datetime(self.task_config.current_cycle, +to_timedelta(f"{self.task_config['assim_freq']}H") / 2)

        local_dict = AttrDict(
            {
                'COMIN_OBSPROC': f"{self.task_config.COMROOT}/obsforge/{RUN}.{yyyymmdd}/{cyc}/ocean/insitu",
                'window_begin': _window_begin,
                'window_end': _window_end,
                'yyyymmdd': yyyymmdd,
                'PREFIX': f"{RUN}.t{cyc}z.",
                'bufr2ioda_config_temp': f"{self.task_config.HOMEobsforge}/parm/{self.task_config.BUFR2IODA_CONFIG_TEMP}"
            }
        )

        self.task_config = AttrDict(**self.task_config, **local_dict)

    @logit(logger)
    def initialize(self) -> None:
        """
        """
        logger.info("running init")
        providers = self.task_config.providers
        logger.info(f"Providers: {providers}")

        DATA = self.task_config.DATA

        keys = ['DATA', 'RUN', 'yyyymmdd', 'cyc', 'PREFIX', 'current_cycle', 'ocean_basin']
        local_dict = AttrDict()
        for key in keys:
            local_dict[key] = self.task_config.get(key)

        bufr_files_to_copy = []

        for provider in providers:

            provider.update(local_dict)
            provider_config = parse_j2yaml(self.task_config.bufr2ioda_config_temp, provider)
            logger.info(f"Provider config for {provider}: {provider_config}")
            provider['ioda_filename'] = provider_config['ioda_filename']
            save_as_yaml(provider_config, f"{DATA}/bufr2ioda_{provider['name']}.yaml")

            source_dump_filename = path.join(self.task_config.DMPDIR, provider_config['dump_filename'])
            local_dump_filename = path.join(DATA, provider_config['local_dump_filename'])
            bufr_files_to_copy.append([source_dump_filename, local_dump_filename])

        FileHandler({'copy_opt': bufr_files_to_copy}).sync()

        FileHandler({'mkdir': [self.task_config.COMIN_OBSPROC]}).sync()

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

            converter = Executable('python')
            converter.add_default_arg(bufrconverter)
            converter.add_default_arg('-c')
            converter.add_default_arg(bufrconverterconfig)
            try:
                logger.debug(f"Executing {converter}")
                converter()
            except Exception as e:
                logger.warning(f"Converter failed for {provider_name}")
                logger.warning(f"Execution failed for {converter}: {e}")
                logger.debug("Exception details", exc_info=True)
                continue  # skip to the next provider

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
                destination_ioda_filename = path.join(self.task_config.COMIN_OBSPROC, ioda_filename)
                ioda_files_to_copy.append([source_ioda_filename, destination_ioda_filename])

        FileHandler({'copy_opt': ioda_files_to_copy}).sync()
