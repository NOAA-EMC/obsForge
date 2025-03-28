#!/usr/bin/env python3

from logging import getLogger
from typing import Dict, Any

from wxflow import (AttrDict, Task, add_to_datetime, to_timedelta,
                    logit)
from pyobsforge.obsdb.ghrsst_db import GhrSstDatabase

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

        # Loop through obs spaces
        for provider in self.task_config.providers.ghrsst:
            logger.info(f"========= provider: {provider}")
            # extract the instrument and platform from the obs_space
            obs_type, instrument, platform, proc_level = provider.split("_")
            platform = platform.upper()
            instrument = instrument.upper()
            logger.info(f"Processing {platform.upper()} {instrument.upper()}")

            # Query the database for valid files
            valid_files = self.ghrsst_db.get_valid_files(
                window_begin=self.task_config.window_begin,
                window_end=self.task_config.window_end,
                instrument=instrument,
                satellite=platform,
                obs_type="SSTsubskin"
            )

            logger.info(f"number of valide files: {len(valid_files)}")

    @logit(logger)
    def finalize(self) -> None:
        """
        """
        print("finalize")
