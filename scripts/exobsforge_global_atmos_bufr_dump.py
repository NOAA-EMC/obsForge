#!/usr/bin/env python3
# exobsforge_global_atmos_bufr_dump.py
# This script will collect and preprocess
# observations in BUFR format and convert to IODA format
# for global atmospheric analyses
import os

from wxflow import AttrDict, Logger, cast_strdict_as_dtypedict, parse_j2yaml
from pyobsforge.task.atmos_bufr_prepobs import AtmosBufrObsPrep

# Initialize root logger
logger = Logger(level='DEBUG', colored_log=True)


def merge_observation_defaults(task_yaml, section_name, default_obs=None):
    """
    Merge default observation configs into a YAML-loaded task dict.
    Parameters
    ----------
    task_yaml : dict
        The full YAML-loaded dictionary.
    section_name : str
        The top-level key containing observations (e.g., 'atmosbufrdump').
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
            "input_file": None,
            "output_file": None,
            "mapping_file": None,
            "script_file": None,
        }

    section = task_yaml.get(section_name, {})
    observations = section.get('observations', {})
    logger.debug(f"Merging defaults into section '{section_name}' with observations: {list(observations.keys())}")

    merged_observations = {}
    for obs_name, obs_cfg in observations.items():
        logger.debug(f"Merging observation '{obs_name}' with config: {obs_cfg}")
        obs_cfg = obs_cfg or {}  # handle None or empty dict
        merged_cfg = {**default_obs, **obs_cfg}

        # Fill dynamic defaults based on obs_name
        if merged_cfg["input_file"] is None:
            merged_cfg["input_file"] = f"{obs_name}.tm00.bufr_d"
        if merged_cfg["output_file"] is None:
            merged_cfg["output_file"] = f"{obs_name}.nc"
        if merged_cfg["mapping_file"] is None:
            merged_cfg["mapping_file"] = f"{obs_name}.yaml"
        if merged_cfg["script_file"] is None:
            merged_cfg["script_file"] = f"{obs_name}.py"

        merged_observations[obs_name] = merged_cfg

    # Update the section with merged observations
    section['observations'] = merged_observations
    task_yaml[section_name] = section

    return task_yaml


if __name__ == '__main__':

    # Take configuration from environment and cast it as python dictionary
    config_env = cast_strdict_as_dtypedict(os.environ)
    # Take configuration from YAML file to augment/append config dict
    config_yaml = parse_j2yaml(os.path.join(config_env['HOMEobsforge'], 'parm', 'config.yaml'), config_env)
    # ensure we are not duplicating keys between the environment and the YAML config
    obsforge_dict = {}
    for key, value in config_yaml['obsforge'].items():
        if key not in config_env.keys():
            obsforge_dict[key] = value

    task_yaml = parse_j2yaml(os.path.join(config_env['HOMEobsforge'], 'parm', 'atmos_bufr_dump_config.yaml'), config_env)

    # Merge defaults for the 'atmosbufrdump' section
    task_yaml = merge_observation_defaults(task_yaml, 'atmosbufrdump')

    # Combine configs together
    config = AttrDict(**config_env, **obsforge_dict)
    config = AttrDict(**config, **task_yaml['atmosbufrdump'])

    atmosBufrObs = AtmosBufrObsPrep(config)
    atmosBufrObs.initialize()
    atmosBufrObs.execute()
    atmosBufrObs.finalize()
