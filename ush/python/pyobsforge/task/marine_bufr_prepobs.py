#!/usr/bin/env python3

from logging import getLogger
from os import path
from datetime import timedelta
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

        obs_cycle_dict = AttrDict({key: self.task_config[key] for key in ['DATA', 'DMPDIR', 'RUN', 'ocean_basin']})
        bufr_files_to_copy = []

        for provider in providers:

            try:
                obs_window_back = provider['window']['back']
                obs_window_forward = provider['window']['forward']
            except KeyError:
                obs_window_back = 0
                obs_window_forward = 0

            # figure out which cycles of bufr obs to convert
            obs_cycles = []
            for i in range(-obs_window_back, obs_window_forward + 1):
                interval = to_timedelta(f"{self.task_config['assim_freq']}H") * i
                obs_cycles.append(self.task_config.current_cycle + interval)
            logger.debug(f"Window cdates {obs_cycles}")

            keys = [ 'data_format', 'name', 'subsets', 'source', 'data_type', 'data_description', 'data_provider', 'dump_tag']
            for key in keys:
                obs_cycle_dict[key] = provider.get(key)

            obs_cycles_to_convert = []
            for obs_cycle in obs_cycles:
               
                obs_cycle_cyc = obs_cycle.strftime("%H")
                obs_cycle_dict.update({
                    'obs_cycle_cyc': obs_cycle_cyc,
                    'obs_cycle': obs_cycle,
                    'obs_cycle_PREFIX': f"{obs_cycle_dict['RUN']}.t{obs_cycle_cyc}z."
                 }) 
                obs_cycle_config = parse_j2yaml(self.task_config.bufr2ioda_config_temp, obs_cycle_dict)
                if path.exists(obs_cycle_config.dump_filename):
                    save_as_yaml(obs_cycle_config, obs_cycle_config.bufr2ioda_yaml)
                    bufr_files_to_copy.append([obs_cycle_config.dump_filename, obs_cycle_config.local_dump_filename])
                    obs_cycles_to_convert.append(obs_cycle_config)

            provider['obs_cycles_to_convert'] = obs_cycles_to_convert

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
            #bufrconverterconfig = f"{self.task_config.DATA}/bufr2ioda_{provider_name}.yaml"
            obs_cycle_configs = provider.obs_cycles_to_convert
            for obs_cycle_config in obs_cycle_configs:

               converter = Executable('python')
               converter.add_default_arg(bufrconverter)
               converter.add_default_arg('-c')
               converter.add_default_arg(obs_cycle_config.bufr2ioda_yaml)
               try:
                   logger.debug(f"Executing {converter}")
                   converter()
               except Exception as e:
                   logger.warning(f"Converter failed for {provider_name}")
                   logger.warning(f"Execution failed for {converter}: {e}")
                   logger.debug("Exception details", exc_info=True)
                   continue  # skip to the next obs_cycle_config

    @logit(logger)
    def finalize(self) -> None:
        """
        """
        logger.info("running finalize")

        providers = self.task_config.providers

        ioda_files_to_copy = []

        for provider in providers:
            ioda_filename = provider['ioda_filename']
            logger.info(f"ioda_filename: {ioda_filename}")
            source_ioda_filename = path.join(self.task_config.DATA, ioda_filename)
            if path.exists(source_ioda_filename):
                destination_ioda_filename = path.join(self.task_config.COMIN_OBSPROC, ioda_filename)
                ioda_files_to_copy.append([source_ioda_filename, destination_ioda_filename])

        FileHandler({'copy_opt': ioda_files_to_copy}).sync()
