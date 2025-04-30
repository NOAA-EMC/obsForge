from logging import getLogger
from pyobsforge.obsdb.ghrsst_db import GhrSstDatabase
from pyobsforge.obsdb.rads_db import RADSDatabase
from pyobsforge.obsdb.nesdis_amsr2_db import NesdisAmsr2Database
from pyobsforge.obsdb.smap_db import SmapDatabase
from pyobsforge.obsdb.smos_db import SmosDatabase
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
    error_ratio: float

    @classmethod
    def from_dict(cls, config: dict) -> "QCConfig":
        # Initialize with default values
        instance = cls(
            bounds_min=0.0,
            bounds_max=0.0,
            binning_stride=0.0,
            binning_min_number_of_obs=0,
            error_ratio=0.0
        )

        # Only set attributes for keys that are defined in config
        if "min" in config:
            instance.bounds_min = config["min"]
        if "max" in config:
            instance.bounds_max = config["max"]
        if "stride" in config:
            instance.binning_stride = config["stride"]
        if "min number of obs" in config:
            instance.binning_min_number_of_obs = config["min number of obs"]
        if "error ratio" in config:
            instance.error_ratio = config["error ratio"]

        return instance


class ProviderConfig:
    def __init__(self, qc_config: QCConfig, db: Any):  # Replace `Any` with a more specific type if desired
        self.qc_config = qc_config
        self.db = db

    @classmethod
    def from_task_config(cls, provider_name: str, task_config: AttrDict) -> "ProviderConfig":
        qc_raw = task_config.providers[provider_name]["qc config"]
        qc = QCConfig.from_dict(qc_raw)

        print(f"@@@@@@@@@@@@@@@@@@@@@@@@ provider: {provider_name}")

        if provider_name == "ghrsst":
            db = GhrSstDatabase(db_name=f"{provider_name}.db", dcom_dir=task_config.DCOMROOT, obs_dir="sst")
        elif provider_name == "rads":
            db = RADSDatabase(db_name=f"{provider_name}.db", dcom_dir=task_config.DCOMROOT, obs_dir="wgrdbul/adt")
        elif provider_name == "nesdis_amsr2":
            db = NesdisAmsr2Database(db_name=f"{provider_name}.db", dcom_dir=task_config.DCOMROOT, obs_dir="seaice/pda")
        elif provider_name == "smap":
            db = SmapDatabase(db_name=f"{provider_name}.db", dcom_dir=task_config.DCOMROOT, obs_dir="wtxtbul/satSSS/SMAP")
        elif provider_name == "smos":
            db = SmosDatabase(db_name=f"{provider_name}.db", dcom_dir=task_config.DCOMROOT, obs_dir="wtxtbul/satSSS/SMOS")
        else:
            raise NotImplementedError(f"DB setup for provider {provider_name} not yet implemented")

        return cls(qc_config=qc, db=db)

    def process_obs_space(self, **kwargs) -> None:
        """
        Process a single observation space by querying the database for valid files,
        copying them to the appropriate directory, and running the ioda converter.

        Args:
            **kwargs: Keyword arguments including:
                provider: Provider name
                obs_space: Observation space name
                instrument: Instrument name
                platform: Platform name
                obs_type: Observation type
                output_file: Output file path
                window_begin: Beginning of time window
                window_end: End of time window
                task_config: Task configuration
        """
        # Extract parameters from kwargs
        provider = kwargs.get('provider')
        obs_space = kwargs.get('obs_space')
        instrument = kwargs.get('instrument')
        platform = kwargs.get('platform')
        obs_type = kwargs.get('obs_type')
        output_file = kwargs.get('output_file')
        window_begin = kwargs.get('window_begin')
        window_end = kwargs.get('window_end')
        task_config = kwargs.get('task_config')

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
                       'input_files': input_files,
                       'output_file': output_file}

            # Only add QC config attributes if they exist
            if hasattr(self.qc_config, 'bounds_min'):
                context['bounds_min'] = self.qc_config.bounds_min
            if hasattr(self.qc_config, 'error_ratio'):
                context['error_ratio'] = self.qc_config.error_ratio
            if hasattr(self.qc_config, 'bounds_max'):
                context['bounds_max'] = self.qc_config.bounds_max
            if hasattr(self.qc_config, 'binning_stride'):
                context['binning_stride'] = self.qc_config.binning_stride
            if hasattr(self.qc_config, 'binning_min_number_of_obs'):
                context['binning_min_number_of_obs'] = self.qc_config.binning_min_number_of_obs
            result = run_nc2ioda(task_config, obs_space, context)
            logger.info(f"run_nc2ioda result: {result}")
        else:
            logger.warning(f"No valid files found for {obs_space} with {instrument} on {platform}")
