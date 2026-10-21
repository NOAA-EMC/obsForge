#!/usr/bin/env python3
# exobsforge_global_atmos_aws_dump.py
# This script will collect and preprocess
# Arctic Weather Satellite (AWS) NetCDF observations and convert to IODA format
# for global atmospheric analyses

import os

from wxflow import AttrDict, Logger, cast_strdict_as_dtypedict, parse_j2yaml
from pyobsforge.task.atmos_aws_prepobs import AtmosAwsObsPrep

# Initialize root logger
logger = Logger(level="DEBUG", colored_log=True)


def merge_observation_defaults(task_yaml, section_name, default_obs=None):
    """
    Merge default observation configs into a YAML-loaded task dict.

    Parameters
    ----------
    task_yaml : dict
        The full YAML-loaded dictionary.
    section_name : str
        The top-level key containing observations (e.g., 'atmossatnetcdf').
    default_obs : dict, optional
        Default observation parameters. If None, uses a standard default.

    Returns
    -------
    dict
        Updated YAML dict with merged defaults.
    """
    if default_obs is None:
        default_obs = {
            "method": "script2netcdf",
            "input_dir": None,
            "file_filter": "*.nc",
            "output_file": None,
            "script_file": "aws_netcdf2ioda.py",
            "mapping_file": None,
            "aux_file": [],
            "mpi": 1,
        }

    section = task_yaml.get(section_name, {})
    observations = section.get("observations", {})
    logger.debug(
        f"Merging defaults into section '{section_name}' with observations: {list(observations.keys())}"
    )

    merged_observations = {}
    for obs_name, obs_cfg in observations.items():
        logger.debug(f"Merging observation '{obs_name}' with config: {obs_cfg}")
        obs_cfg = obs_cfg or {}  # Handle None or empty dict
        merged_cfg = {**default_obs, **obs_cfg}

        # Fill dynamic defaults based on obs_name
        if merged_cfg["output_file"] is None:
            merged_cfg["output_file"] = f"{obs_name}.nc"
        if merged_cfg["mapping_file"] is None:
            merged_cfg["mapping_file"] = f"{obs_name}.yaml"
        if merged_cfg["file_filter"] is None:
            merged_cfg["file_filter"] = f"*{obs_name}*.nc"

        merged_observations[obs_name] = merged_cfg

    # Update the section with merged observations
    section["observations"] = merged_observations
    task_yaml[section_name] = section

    return task_yaml


if __name__ == "__main__":

    # Take configuration from environment and cast it as python dictionary
    config_env = cast_strdict_as_dtypedict(os.environ)

    # Take configuration from YAML file to augment/append config dict
    config_yaml = parse_j2yaml(
        os.path.join(config_env["HOMEobsforge"], "parm", "config.yaml"),
        config_env,
    )

    # Ensure keys are not duplicated between environment and YAML config
    obsforge_dict = {}
    for key, value in config_yaml.get("obsforge", {}).items():
        if key not in config_env.keys():
            obsforge_dict[key] = value

    # Determine RUN type (global vs regional)
    run_mod = config_env.get("RUN", "gfs").lower()

    # Select configuration file
    if run_mod in ("gfs", "gdas"):
        config_file = config_env.get("TASK_CONFIG_YAML", "atmos_sat_config.yaml")
    else:
        config_file = config_env.get("TASK_CONFIG_YAML", "atmos_sat_config_reg.yaml")

    full_config_path = os.path.join(config_env["HOMEobsforge"], "parm", config_file)
    # Fallback to base atmos_sat_config.yaml if regional variant is not yet implemented
    if not os.path.exists(full_config_path):
        full_config_path = os.path.join(config_env["HOMEobsforge"], "parm", "atmos_sat_config.yaml")

    task_yaml = parse_j2yaml(full_config_path, config_env)

    # Identify target observation section
    section_name = "atmossatnetcdf"
    if section_name not in task_yaml:
        for candidate in ("atmos_sat_netcdf", "atmosawsnetcdf", "atmos_aws_prepobs"):
            if candidate in task_yaml:
                section_name = candidate
                break

    # Merge defaults for the observation section
    task_yaml = merge_observation_defaults(task_yaml, section_name)

    # Combine configs together
    config = AttrDict(**config_env, **obsforge_dict)
    config = AttrDict(**config, **task_yaml.get(section_name, {}))

    # Run the task lifecycle
    atmosAwsObs = AtmosAwsObsPrep(config)
    atmosAwsObs.initialize()
    atmosAwsObs.execute()
    atmosAwsObs.finalize()
