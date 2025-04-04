#!/usr/bin/env python3

from logging import getLogger
from typing import Dict, Any
from wxflow import AttrDict, Task, add_to_datetime, to_timedelta, logit, FileHandler
from pyobsforge.task.providers import ProviderConfig
from multiprocessing import Process, Manager
from os.path import join


logger = getLogger(__name__.split('.')[-1])


class MarineObsPrep(Task):
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

        # Initialize the Providers
        self.ghrsst = ProviderConfig.from_task_config("ghrsst", self.task_config)

        # Initialize the list of processed ioda files
        self.ioda_files = []

    @logit(logger)
    def initialize(self) -> None:
        """
        """
        # Update the database with new files
        self.ghrsst.db.ingest_files()

    @logit(logger)
    def execute(self) -> None:
        """
        """
        with Manager() as manager:
            # Use a Manager list to share ioda_files across processes
            shared_ioda_files = manager.list()

            processes = []
            for provider, obs_spaces in self.task_config.providers.items():
                logger.info(f"========= provider: {provider}")
                for obs_space in obs_spaces["list"]:
                    logger.info(f"========= obs_space: {obs_space}")
                    obs_type, instrument, platform, proc_level = obs_space.split("_")
                    platform = platform.upper()
                    instrument = instrument.upper()
                    logger.info(f"Processing {platform.upper()} {instrument.upper()}")

                    # Start a new process
                    process = Process(target=self.process_obs_space,
                                      args=(provider, obs_space, instrument, platform, shared_ioda_files))
                    process.start()
                    processes.append(process)

            # Wait for all processes to complete
            for process in processes:
                process.join()

            # Convert the Manager list to a regular list
            self.ioda_files = list(shared_ioda_files)
            logger.info(f"Final ioda_files: {self.ioda_files}")

    @logit(logger)
    def process_obs_space(self,
                          provider: str,
                          obs_space: str,
                          instrument: str,
                          platform: str,
                          shared_ioda_files) -> None:
        if provider == "ghrsst":
            output_file = f"{self.task_config['RUN']}.t{self.task_config['cyc']:02d}z.{obs_space}.tm00.nc"
            result = self.ghrsst.process_obs_space(provider, obs_space, instrument, platform,
                                                   obs_type="SSTsubskin",
                                                   output_file=output_file,
                                                   window_begin=self.task_config.window_begin,
                                                   window_end=self.task_config.window_end,
                                                   task_config=self.task_config)
            # If file was created successfully, add to the shared list
            if result and output_file:
                shared_ioda_files.append(output_file)
                logger.info(f"Appended {output_file} to shared_ioda_files")
            return result
        else:
            logger.error(f"Provider {provider} not supported")

    @logit(logger)
    def finalize(self) -> None:
        """
        """
        # Copy the processed ioda files to the destination directory
        logger.info("Copying ioda files to destination COMROOT directory")
        logger.info(f"COMROOT: {self.task_config['COMROOT']}")
        logger.info(f"DATA: {self.task_config['DATA']}")
        logger.info(f"GHRSST ioda files: {self.ioda_files}")
        src_dst_obs_list = []  # list of [src_file, dst_file]
        for ioda_file in self.ioda_files:
            src_file = join(self.task_config['DATA'], ioda_file)
            dst_file = join(self.task_config['COMROOT'], ioda_file)
            src_dst_obs_list.append([src_file, dst_file])

        logger.info("Copying ioda files to destination COMROOT directory")
        logger.info(f"src_dst_obs_list: {src_dst_obs_list}")

        yyyymmdd = self.task_config['PDY'].strftime('%Y%m%d')
        comout = join(self.task_config['COMROOT'],
                      f"{self.task_config['RUN']}.{yyyymmdd}{self.task_config['cyc']:02d}")
        FileHandler({'mkdir': [comout]}).sync()
        FileHandler({'copy': src_dst_obs_list}).sync()
