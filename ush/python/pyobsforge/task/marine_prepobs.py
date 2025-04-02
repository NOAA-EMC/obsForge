#!/usr/bin/env python3

from logging import getLogger
from typing import Dict, Any
from wxflow import AttrDict, Task, add_to_datetime, to_timedelta, logit
from pyobsforge.obsdb.ghrsst_db import GhrSstDatabase
from multiprocessing import Process
from pyobsforge.task.run_nc2ioda import run_nc2ioda

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
                'OPREFIX': f"{self.task_config.RUN}.t{self.task_config.cyc:02d}z.",
                'APREFIX': f"{self.task_config.RUN}.t{self.task_config.cyc:02d}z."
            }
        )

        # task_config is everything that this task should need
        self.task_config = AttrDict(**self.task_config, **local_dict)

        # Initialize the GHRSST database
        self.ghrsst_db = GhrSstDatabase(db_name="sst_obs.db",
                                        dcom_dir=self.task_config.DCOMROOT,
                                        obs_dir="sst")

    @logit(logger)
    def initialize(self) -> None:
        """
        """
        # Update the database with new files
        self.ghrsst_db.ingest_files()

    @logit(logger)
    def execute(self) -> None:
        """
        """
        processes = []
        for provider, obs_spaces in self.task_config.providers.items():
            logger.info(f"========= provider: {provider}")

            # Get the obs space QC configuration
            bounds_min = obs_spaces["qc config"]["min"]
            bounds_max = obs_spaces["qc config"]["max"]
            binning_stride = obs_spaces["qc config"]["stride"]
            binning_min_number_of_obs = obs_spaces["qc config"]["min number of obs"]

            # Process each obs space
            for obs_space in obs_spaces["list"]:
                logger.info(f"========= obs_space: {obs_space}")
                # extract the instrument and platform from the obs_space
                obs_type, instrument, platform, proc_level = obs_space.split("_")
                platform = platform.upper()
                instrument = instrument.upper()
                logger.info(f"Processing {platform.upper()} {instrument.upper()}")

                # Start a new process
                process = Process(target=self.process_obs_space,
                                  args=(provider, obs_space, instrument, platform,
                                        bounds_min, bounds_max,
                                        binning_stride, binning_min_number_of_obs))
                process.start()
                processes.append(process)

        # Wait for all processes to complete
        completed_processes = []
        for process in processes:
            process.join()
            completed_processes.append(process)
        logger.info(f"completed processes: {completed_processes}")

    @logit(logger)
    def process_obs_space(self,
                          provider: str,
                          obs_space: str,
                          instrument: str,
                          platform: str,
                          bounds_min: float,
                          bounds_max: float,
                          binning_stride: float,
                          binning_min_number_of_obs: int) -> None:
        if provider == "ghrsst":
            return self.process_ghrsst(provider, obs_space, instrument, platform,
                                       bounds_min, bounds_max, binning_stride, binning_min_number_of_obs)
        else:
            logger.error(f"Provider {provider} not supported")

    @logit(logger)
    def process_ghrsst(self,
                       provider: str,
                       obs_space: str,
                       instrument: str,
                       platform: str,
                       bounds_min: float,
                       bounds_max: float,
                       binning_stride: float,
                       binning_min_number_of_obs: int) -> None:
        """
        Process a single observation space by querying the database for valid files,
        copying them to the appropriate directory, and running the ioda converter.

        Args:
            provider (str): The data provider name.
            obs_space (str): The observation space identifier.
            instrument (str): The instrument used for the observations.
            platform (str): The satellite platform name.
            bounds_min (float): Minimum QC bound for observations.
            bounds_max (float): Maximum QC bound for observations.
            binning_stride (float): Stride value for binning observations.
            binning_min_number_of_obs (int): Minimum number of observations required for binning.
        """
        # Query the database for valid files
        input_files = self.ghrsst_db.get_valid_files(window_begin=self.task_config.window_begin,
                                                     window_end=self.task_config.window_end,
                                                     dst_dir=obs_space,
                                                     instrument=instrument,
                                                     satellite=platform,
                                                     obs_type="SSTsubskin")
        logger.info(f"number of valid files: {len(input_files)}")

        # Process the observations if the obs space is not empty
        if len(input_files) > 0:
            # Configure the ioda converter
            output_file = f"{self.task_config['RUN']}.t{self.task_config['cyc']:02d}z.{obs_space}.tm00.nc"
            context = {'provider': provider.upper(),
                       'window_begin': self.task_config.window_begin,
                       'window_end': self.task_config.window_end,
                       'bounds_min': bounds_min,
                       'bounds_max': bounds_max,
                       'binning_stride': binning_stride,
                       'binning_min_number_of_obs': binning_min_number_of_obs,
                       'input_files': input_files,
                       'output_file': output_file}
            result = run_nc2ioda(self.task_config, obs_space, context)
            logger.info(f"run_nc2ioda result: {result}")

    @logit(logger)
    def finalize(self) -> None:
        """
        """
        logger.info("finalize")
