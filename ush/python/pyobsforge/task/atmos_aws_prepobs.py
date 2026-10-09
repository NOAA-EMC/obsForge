#!/usr/bin/env python3
#
# (C) Copyright 2026 UCAR / obsForge-atmos
#
# Class for preparing and managing Arctic Weather Satellite (AWS) NetCDF observations.
#

import glob
import json
import multiprocessing as mp
import os
import pathlib
from logging import getLogger
from typing import Any, Dict

from wxflow import (
    AttrDict,
    Executable,
    FileHandler,
    Task,
    YAMLFile,
    add_to_datetime,
    logit,
    save_as_yaml,
    to_timedelta,
)

logger = getLogger(__name__.split(".")[-1])


def mp_aws_converter(ob_name, exec_cmd):
    """Worker function to execute conversion tasks in parallel."""
    try:
        logger.debug(f"Executing {exec_cmd}")
        exec_cmd()
    except Exception as e:
        logger.warning(f"AWS NetCDF Conversion failed for {ob_name}")
        logger.warning(f"Execution failed for {exec_cmd}: {e}")
        logger.debug("Exception details", exc_info=True)


class AtmosAwsObsPrep(Task):
    """
    Class for preparing and managing Arctic Weather Satellite (AWS) NetCDF observation processing tasks
    within obsForge-atmos / wxflow.
    """

    def __init__(self, config: Dict[str, Any]) -> None:
        super().__init__(config)

        # Calculate time window boundaries
        assim_freq_hours = float(self.task_config.get("assim_freq", 6))
        half_window_hours = assim_freq_hours / 2.0

        _window_begin = add_to_datetime(
            self.task_config.current_cycle,
            -to_timedelta(f"{half_window_hours}H"),
        )
        _window_end = add_to_datetime(
            self.task_config.current_cycle,
            +to_timedelta(f"{half_window_hours}H"),
        )

        # Determine COMIN_OBSPROC directory structure based on RUN
        run_mod = self.task_config.RUN.lower()
        if run_mod in ("gfs", "gdas"):
            comin_obsproc = os.path.join(
                self.task_config.OBSPROC_COMROOT,
                f"{self.task_config.RUN}.{self.task_config.current_cycle.strftime('%Y%m%d')}",
                f"{self.task_config.cyc:02d}",
                "atmos",
            )
        else:
            comin_obsproc = os.path.join(
                self.task_config.OBSPROC_COMROOT,
                f"{self.task_config.RUN}.{self.task_config.current_cycle.strftime('%Y%m%d')}",
            )

        local_dict = AttrDict(
            {
                "window_begin": _window_begin,
                "window_end": _window_end,
                "half_window_hours": half_window_hours,
                "OPREFIX": f"{self.task_config.RUN}.t{self.task_config.cyc:02d}z.",
                "APREFIX": f"{self.task_config.RUN}.t{self.task_config.cyc:02d}z.",
                "COMIN_OBSPROC": comin_obsproc,
            }
        )

        self.task_config = AttrDict(**self.task_config, **local_dict)

    @logit(logger)
    def initialize(self) -> None:
        """
        Initialize AWS NetCDF observation preparation task:
        - Check for existence of input AWS NetCDF files
        - Stage NetCDF input files, mapping files, and converter scripts
        - Register observations for processing
        """
        self.script2netcdf_obs = {}
        copylist = []
        sub_dir_list = []

        for ob_name, ob_data in self.task_config.observations.items():
            logger.debug(f"Processing observation: {ob_name}: {ob_data}")

            input_files = ob_data.get("input_file", [])
            if isinstance(input_files, str):
                input_files = [input_files]

            skip_this_observation = False
            for f in input_files:
                src = os.path.join(
                    self.task_config.COMIN_OBSPROC,
                    f"{self.task_config.OPREFIX}{f}",
                )

                if not os.path.exists(src):
                    logger.warning(
                        f"[{ob_name}] AWS NetCDF file missing: {src}. Skipping observation."
                    )
                    skip_this_observation = True
                    break

                try:
                    if os.path.getsize(src) == 0:
                        logger.warning(
                            f"[{ob_name}] AWS NetCDF file is empty: {src}. Skipping observation."
                        )
                        skip_this_observation = True
                        break
                except Exception as e:
                    logger.warning(
                        f"[{ob_name}] Failed to stat AWS NetCDF file {src}: {e}. Skipping observation."
                    )
                    skip_this_observation = True
                    break

            if skip_this_observation:
                continue

            # Normalize configuration input types
            input_files = ob_data.get("input_file", [])
            script_files = ob_data.get("script_file", [])
            aux_files = ob_data.get("aux_file", [])
            preserve_rel_path = ob_data.get("preserve_rel_path", None)

            if isinstance(input_files, str):
                input_files = [input_files]
            if isinstance(script_files, str):
                script_files = [script_files]

            staged_inputs, staged_scripts = [], []

            # Stage AWS NetCDF input files
            for f in input_files:
                src = os.path.join(
                    self.task_config.COMIN_OBSPROC,
                    f"{self.task_config.OPREFIX}{f}",
                )
                dest = os.path.join(
                    self.task_config.DATA, os.path.basename(src)
                )

                if preserve_rel_path:
                    comin_obsproc = self.task_config.COMIN_OBSPROC
                    last_three = os.path.join(
                        *comin_obsproc.split(os.sep)[-3:]
                    )
                    sub_dir_tmp = os.path.join(
                        self.task_config.DATA, last_three
                    )
                    if sub_dir_tmp not in sub_dir_list:
                        sub_dir_list.append(sub_dir_tmp)
                    dest = os.path.join(sub_dir_tmp, os.path.basename(src))

                copylist.append([src, dest])
                staged_inputs.append(dest)

            # Stage converter Python scripts
            for f in script_files:
                src = os.path.join(
                    self.task_config.HOMEobsforge,
                    "sorc",
                    "spoc",
                    "dump",
                    "scripts",
                    "atmosphere",
                    f,
                )
                dest = os.path.join(
                    self.task_config.DATA, os.path.basename(src)
                )
                copylist.append([src, dest])
                staged_scripts.append(dest)

            # Stage auxiliary files
            for f in aux_files:
                src = os.path.join(
                    self.task_config.HOMEobsforge,
                    "sorc",
                    "spoc",
                    "dump",
                    "aux",
                    f,
                )
                dest = os.path.join(
                    self.task_config.DATA, os.path.basename(src)
                )
                copylist.append([src, dest])

            # Register script converter configurations
            self.script2netcdf_obs[ob_name] = {
                "staged_inputs": staged_inputs,
                "output_file": os.path.join(
                    self.task_config.DATA, ob_data["output_file"]
                ),
                "script_file": staged_scripts,
                "mpi": ob_data.get("mpi", 1),
            }

        # Stage files into execution workspace
        if sub_dir_list:
            FileHandler({"mkdir": sub_dir_list}).sync()
        FileHandler({"copy_opt": copylist}).sync()

    @logit(logger)
    def execute(self) -> None:
        """
        Execute AWS NetCDF to IODA converter scripts.
        Pipes cycle time window arguments (-t YYYYMMDDHH -half X.X) into execution arguments.
        """
        exec_cmd_list = []
        mpi_count = 0

        # Cycle time string formatted for -t (e.g., '2026090100')
        cycle_str = self.task_config.current_cycle.strftime("%Y%m%d%H")
        half_win_str = str(self.task_config.half_window_hours)

        for ob_name, ob_data in self.script2netcdf_obs.items():
            staged_inputs = ob_data["staged_inputs"]
            output_file = ob_data["output_file"]
            script_files = ob_data.get("script_file", [])

            if not script_files:
                logger.error(
                    f"No script_file provided for observation '{ob_name}'. Skipping."
                )
                continue

            script_file = script_files[0]
            mpi = ob_data.get("mpi", 1)

            logger.info(
                f"Converting AWS NetCDF files to {output_file} using {script_file} "
                f"for cycle {cycle_str} (Window: ±{half_win_str}h)"
            )

            # Build command argument list with -i, -o, -t, and -half
            cmd_args = [
                script_file,
                "-i",
            ] + staged_inputs + [
                "-o",
                output_file,
                "-t",
                cycle_str,
                "-half",
                half_win_str,
            ]

            if mpi > 1:
                mpi_count += int(mpi)
                if self.task_config.MPI_LAUNCHER.lower() == "mpiexec":
                    exec_cmd = Executable("mpiexec")
                    args = ["-n", str(mpi), "python"] + cmd_args
                else:  # srun launcher
                    exec_cmd = Executable("srun")
                    args = [
                        "--export",
                        "All",
                        "-n",
                        str(mpi),
                        "--mem",
                        "0G",
                        "--time",
                        "00:30:00",
                        "python",
                    ] + cmd_args
            else:
                exec_cmd = Executable("python")
                args = cmd_args

            for arg in args:
                exec_cmd.add_default_arg(arg)

            exec_cmd_list.append((ob_name, exec_cmd))

        # Parallel process execution across workers
        num_workers = min(
            len(exec_cmd_list) + mpi_count + 5, max(1, mp.cpu_count() - 1)
        )
        logger.info(f"Number of worker processes to use: {num_workers}")

        with mp.Pool(num_workers) as pool:
            pool.starmap(mp_aws_converter, exec_cmd_list)

    @logit(logger)
    def finalize(self) -> None:
        """
        Finalize task by copying output IODA NetCDF files to COMOUT and
        generating status summary log.
        """
        run_mod = self.task_config.RUN.lower()

        if run_mod in ("gfs", "gdas"):
            comout = os.path.join(
                self.task_config["COMROOT"],
                self.task_config["PSLOT"],
                f"{self.task_config.RUN}.{self.task_config.current_cycle.strftime('%Y%m%d')}",
                f"{self.task_config.cyc:02d}",
                "atmos",
            )
        else:
            comout = os.path.join(
                self.task_config["COMROOT"],
                self.task_config["PSLOT"],
                f"{self.task_config.RUN}.{self.task_config.current_cycle.strftime('%Y%m%d')}",
                f"{self.task_config.cyc:02d}",
            )

        output_files = glob.glob(os.path.join(self.task_config.DATA, "*.nc"))
        copy_list = []
        for output_file in output_files:
            filename = os.path.basename(output_file)
            if "Coeff" not in filename:
                destination_file = os.path.join(
                    comout, f"{self.task_config['OPREFIX']}{filename}"
                )
                copy_list.append([output_file, destination_file])

        FileHandler({"mkdir": [comout], "copy_opt": copy_list}).sync()

        # Generate summary log and execute restriction filter
        ready_file = pathlib.Path(
            os.path.join(
                comout,
                f"{self.task_config['OPREFIX']}obsforge_atmos_aws_status.log",
            )
        )
        summary_dict = {
            "time window": {
                "begin": self.task_config.window_begin.strftime(
                    "%Y-%m-%dT%H:%M:%SZ"
                ),
                "end": self.task_config.window_end.strftime(
                    "%Y-%m-%dT%H:%M:%SZ"
                ),
                "bound to include": "begin",
            },
            "input directory": str(comout),
            "output file": str(ready_file),
        }

        save_as_yaml(
            summary_dict, os.path.join(self.task_config.DATA, "stats.yaml")
        )

        exec_cmd = Executable(
            os.path.join(
                self.task_config.HOMEobsforge,
                "build",
                "bin",
                "ioda-summary.x",
            )
        )
        exec_cmd.add_default_arg(
            os.path.join(self.task_config.DATA, "stats.yaml")
        )

        try:
            logger.info(f"Creating summary file {ready_file}")
            exec_cmd()
        except Exception as e:
            logger.warning(
                f"Failed to create summary file {ready_file}: {e}. Creating empty ready file."
            )
            ready_file.touch()

        # Run ioda_restriction_filter if available
        stats_yaml = os.path.join(self.task_config.DATA, "stats.yaml")
        import sys

        sys.path.append(
            os.path.join(self.task_config.HOMEobsforge, "build", "bin")
        )

        try:
            from ioda_restriction_filter import (
                run_rsrd_exprsrd as restriction_filter,
            )

            restriction_filter(stats_yaml)
        except Exception as e:
            logger.warning(f"ioda_restriction_filter.py failed: {e}")
