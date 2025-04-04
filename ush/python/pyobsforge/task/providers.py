from logging import getLogger
from pyobsforge.obsdb.ghrsst_db import GhrSstDatabase
from typing import Any
from dataclasses import dataclass
from wxflow import AttrDict
from pyobsforge.task.run_nc2ioda import run_nc2ioda

logger = getLogger(__name__.split('.')[-1])


@dataclass
class QCConfig:
    bounds_min: float
    bounds_max: float
    binning_stride: float
    binning_min_number_of_obs: int

    @classmethod
    def from_dict(cls, config: dict) -> "QCConfig":
        return cls(
            bounds_min=config["min"],
            bounds_max=config["max"],
            binning_stride=config["stride"],
            binning_min_number_of_obs=config["min number of obs"]
        )

class ProviderConfig:
    def __init__(self, qc_config: QCConfig, db: Any):  # Replace `Any` with a more specific type if desired
        self.qc_config = qc_config
        self.db = db

    @classmethod
    def from_task_config(cls, provider_name: str, task_config: AttrDict) -> "ProviderConfig":
        qc_raw = task_config.providers[provider_name]["qc config"]
        qc = QCConfig.from_dict(qc_raw)

        if provider_name == "ghrsst":
            db = GhrSstDatabase(
                db_name=f"{provider_name}.db",
                dcom_dir=task_config.DCOMROOT,
                obs_dir="sst"
            )
        else:
            raise NotImplementedError(f"DB setup for provider {provider_name} not yet implemented")

        return cls(qc_config=qc, db=db)

    def process_obs_space(self,
                          provider: str,
                          obs_space: str,
                          instrument: str,
                          platform: str,
                          obs_type: str,
                          output_file: str,
                          window_begin,
                          window_end,
                          task_config) -> None:
        """
        Process a single observation space by querying the database for valid files,
        copying them to the appropriate directory, and running the ioda converter.

        Args:
            provider (str): The data provider name.
            obs_space (str): The observation space identifier.
            instrument (str): The instrument used for the observations.
            platform (str): The satellite platform name.
        """
        # Query the database for valid files
        input_files = self.db.get_valid_files(window_begin=window_begin,
                                              window_end=window_end,
                                              dst_dir=obs_space,
                                              instrument=instrument,
                                              satellite=platform,
                                              obs_type=obs_type)
        logger.info(f"number of valid files: {len(input_files)}")

        # Process the observations if the obs space is not empty
        if len(input_files) > 0:
            # Configure the ioda converter
            context = {'provider': provider.upper(),
                       'window_begin': window_begin,
                       'window_end': window_end,
                       'bounds_min': self.qc_config.bounds_min,
                       'bounds_max': self.qc_config.bounds_max,
                       'binning_stride': self.qc_config.binning_stride,
                       'binning_min_number_of_obs': self.qc_config.binning_min_number_of_obs,
                       'input_files': input_files,
                       'output_file': output_file}
            result = run_nc2ioda(task_config, obs_space, context)
            logger.info(f"run_nc2ioda result: {result}")
        else:
            logger.warning(f"No valid files found for {obs_space} with {instrument} on {platform}")
