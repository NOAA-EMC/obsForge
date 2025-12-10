#!/usr/bin/env python3
# exobsforge_obsforgemonitor.py
# This script monitors obsforge data processing
import os
from wxflow import AttrDict, Logger, cast_strdict_as_dtypedict, parse_j2yaml
# from pyobsforge.task.obsforge_monitor import ObsforgeMonitor                    
from pyobsforge.task.monitor_task import ObsforgeMonitorTask

logger = Logger(level='DEBUG', colored_log=True)

if __name__ == '__main__':

    # Take configuration from environment and cast it as python dictionary
    config_env = cast_strdict_as_dtypedict(os.environ)

    # Load YAML (with optional Jinja)
    yaml_path = os.path.join(config_env['HOMEobsforge'], 'parm', 'config.yaml')
    config_yaml = parse_j2yaml(yaml_path, config_env)

    obsforge_dict = {}
    for key, value in config_yaml['obsforge'].items():
        if key not in config_env.keys():
            obsforge_dict[key] = value

    monitor_yaml_path = os.path.join(config_env['HOMEobsforge'], 'parm', 'monitor_config.yaml')
    task_yaml = parse_j2yaml(monitor_yaml_path, config_env)

    # Combine configs together
    base_config = AttrDict(**config_env, **obsforge_dict)
    base_config = AttrDict(**base_config, **task_yaml['obsforgemonitor'])


    # Build list of dump tasks
    # TODO: we may need to apply jinja to some dump tasks
    dump_tasks = AttrDict()
    for section_name, section_config in config_yaml.items():
        if section_name == "obsforge":
            continue
        if section_name == "obsforgemonitor":
            continue
        dump_tasks[section_name] = AttrDict(section_config)

    # Final config object
    config = AttrDict(**base_config)
    config.dump_tasks = dump_tasks

    # monitor = ObsforgeMonitor(config)
    monitor = ObsforgeMonitorTask(config)
    monitor.run()
