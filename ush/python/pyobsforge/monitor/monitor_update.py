#!/usr/bin/env python3

import sys
import argparse
import yaml
from wxflow import AttrDict, Logger
from pyobsforge.monitor.obsforge_monitor import ObsforgeMonitor

logger = Logger(level='DEBUG', colored_log=True)

def load_config(yaml_path):
    try:
        with open(yaml_path, 'r') as f:
            raw = yaml.safe_load(f)
    except Exception as e:
        logger.error(f"Failed to load YAML: {e}")
        sys.exit(1)
    return AttrDict(raw['obsforgemonitor'])

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--config', required=True, help="Path to monitor_config.yaml")
    args = parser.parse_args()

    config = load_config(args.config)

    if 'database' not in config or 'data_root' not in config:
        logger.error("Configuration must contain 'database' and 'data_root'.")
        sys.exit(1)

    logger.info(f"Initializing Monitor. DB: {config.database}")

    monitor = ObsforgeMonitor(config)
    monitor.run()

if __name__ == "__main__":
    main()

