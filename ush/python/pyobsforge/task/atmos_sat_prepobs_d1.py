#!/usr/bin/env python3
#
# (C) Copyright 2026 UCAR / obsForge-atmos
#
# Class for preparing and managing Arctic Weather Satellite (AWS) observations.
#

import glob
import multiprocessing as mp
import os
import pathlib
import re
import sys
from datetime import datetime, timedelta
from logging import getLogger
from typing import Any, Dict, List, Optional, Tuple

from wxflow import (
    AttrDict,
    Executable,
    FileHandler,
    Task,
    YAMLFile,
    add_to_datetime,
    logit,
    parse_task_config,
    save_as_yaml,
    to_timedelta,
)

logger = getLogger(__name__.split(".")[-1])


def mp_aws_converter(ob_name: str, exec_cmd: Executable) -> None:
    """Worker function to execute conversion tasks in parallel."""
    try:
        logger.debug(f"Executing {exec_cmd}")
        exec_cmd()
    except Exception as e:
        logger.warning(f"AWS NetCDF Conversion failed for {ob_name}")
        logger.warning(f"Execution failed for {exec_cmd}: {e}")
        logger.debug("Exception details", exc_info=True)


def parse_aws_file_timestamps(
    filepath: str,
) -> Tuple[Optional[datetime], Optional[datetime]]:
    """
    Extract start and end timestamps from AWS WMO granule filenames.
    Pattern matches: ..._<start_time 14-digits>_<end_time 14-digits>_...
    Example: ..._G_O_20260831235157_20260901013112_C_N____.nc
    """
    fname = os.path.basename(filepath)
    match = re.search(r"_(\d{14})_(\d{14})_", fname)
    if match:
        try:
            t_start = datetime.strptime(match.group(1), "%Y%m%d%H%M%S")
            t_end = datetime.strptime(match.group(2), "%Y%m%d%H%M%S")
            return t_start, t_end
        except ValueError:
            pass
    return None, None


def resolve_candidate_dirs(
    dir_template: str, window_begin: datetime, window_end: datetime
) -> List[str]:
    """
    Build candidate search paths spanning the window (including month/year boundaries).
    Handles strftime format tokens (e.g., Y%Y/M%m) as well as static directories.
    """
    candidate_dirs = []

    if "%" in dir_template:
        curr = window_begin
        while curr <= window_end:
            d = curr.strftime(dir_template)
            if d not in candidate_dirs:
                candidate_dirs.append(d)
            curr += timedelta(hours=1)
        d_end = window_end.strftime(dir_template)
        if d_end not in candidate_dirs:
            candidate_dirs.append(d_end)
    else:
        candidate_dirs.append(dir_template)
        # Check standard Y%Y/M%m subdirectories if given a root path
        curr = window_begin
        while curr <= window_end:
            sub_d = os.path.join(
                dir_template, f"Y{curr.year}", f"M{curr.strftime('%m')}"
            )
            if sub_d not in candidate_dirs:
                candidate_dirs.append(sub_d)
            curr += timedelta(hours=1)

    return candidate_dirs


class AtmosAwsObsPrep(Task):
    """
    Class for preparing and managing Arctic Weather Satellite (AWS) observation
    processing tasks within obsForge-atmos / wxflow.
    """

    def __init__(self, config: Dict[str, Any]) -> None:
        super().__init__(config)

        # Calculate time window boundaries
        # For assim_freq=6: ±3h around cycle time
        # 00z: [day-1 21:00 to day 03:00]
        # 06z: [03:00 to 09:00]
        # 12z: [09:00 to 15:00]
        # 18z: [15:00 to 21:00]
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
        Scan input directory for AWS NetCDF files overlapping the assimilation window,
        stage matched files into $DATA/inputs, and register commands.
        """
        self.script2netcdf_obs = {}
        self.registered_outputs = []
        copylist = []
        staged_inputs_dir = os.path.join(self.task_config.DATA, "inputs")
        mkdir_list = [self.task_config.DATA, staged_inputs_dir]
        staged_mappings = []

        # Retrieve observations section
        observations = self.task_config.get("observations")
        if observations is None:
            for section in ("atmossatnetcdf", "atmos_sat_netcdf", "atmos_aws_prepobs"):
                if section in self.task_config and "observations" in self.task_config[section]:
                    observations = self.task_config[section]["observations"]
                    break
        if observations is None:
            observations = {}

        w_begin = self.task_config.window_begin
        w_end = self.task_config.window_end
        logger.info(
            f"Scanning AWS observations for cycle {self.task_config.current_cycle} "
            f"within window [{w_begin} to {w_end}]"
        )

        for ob_name, ob_data in observations.items():
            logger.debug(f"Processing observation configuration: {ob_name}")

            # Determine raw input directory
            raw_dir_pattern = ob_data.get(
                "input_dir",
                self.task_config.get(
                    "AWS_DATAROOT", self.task_config.COMIN_OBSPROC
                ),
            )
            candidate_dirs = resolve_candidate_dirs(
                raw_dir_pattern, w_begin, w_end
            )
            file_filter = ob_data.get(
                "file_filter", ob_data.get("input_file", "*.nc")
            )

            matched_files = []
            for cdir in candidate_dirs:
                if not os.path.isdir(cdir):
                    logger.debug(f"Candidate directory does not exist: {cdir}")
                    continue

                for fpath in glob.glob(os.path.join(cdir, file_filter)):
                    try:
                        if os.path.getsize(fpath) == 0:
                            logger.warning(f"Skipping empty file: {fpath}")
                            continue
                    except Exception as e:
                        logger.warning(f"Could not stat file {fpath}: {e}")
                        continue

                    t_start, t_end = parse_aws_file_timestamps(fpath)
                    if t_start and t_end:
                        # Granule overlaps window if start <= window_end and end >= window_begin
                        if t_start <= w_end and t_end >= w_begin:
                            matched_files.append((t_start, fpath))
                    else:
                        # Fallback for explicit filenames without standard WMO timestamps
                        matched_files.append((w_begin, fpath))

            # Deduplicate and sort chronologically
            matched_files = sorted(
                list({path: t for t, path in matched_files}.items()),
                key=lambda x: x[1],
            )
            discovered_files = [path for path, _ in matched_files]

            if not discovered_files:
                logger.warning(
                    f"[{ob_name}] No matching AWS NetCDF files found for window "
                    f"[{w_begin} to {w_end}]. Skipping observation."
                )
                continue

            logger.info(
                f"[{ob_name}] Selected {len(discovered_files)} AWS NetCDF granule(s) for assimilation window:"
            )
            for f in discovered_files:
                logger.info(f"  -> {os.path.basename(f)}")

            # Normalize configuration files
            script_files = ob_data.get("script_file", [])
            mapping_files = ob_data.get("mapping_file", [])
            aux_files = ob_data.get("aux_file", [])

            if isinstance(script_files, str):
                script_files = [script_files]
            if isinstance(mapping_files, str):
                mapping_files = [mapping_files]
            if isinstance(aux_files, str):
                aux_files = [aux_files]

            # Stage inputs into $DATA/inputs to isolate them from IODA output NetCDF files
            staged_inputs = []
            for src in discovered_files:
                dest = os.path.join(staged_inputs_dir, os.path.basename(src))
                copylist.append([src, dest])
                staged_inputs.append(dest)

            # Stage converter scripts
            staged_scripts = []
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
                dest = os.path.join(self.task_config.DATA, os.path.basename(src))
                copylist.append([src, dest])
                staged_scripts.append(dest)

            # Stage mapping files
            for f in mapping_files:
                src = os.path.join(
                    self.task_config.HOMEobsforge,
                    "sorc",
                    "spoc",
                    "dump",
                    "config",
                    "atmosphere",
                    f,
                )
                dest = os.path.join(self.task_config.DATA, os.path.basename(src))
                copylist.append([src, dest])
                staged_mappings.append(dest)

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
                dest = os.path.join(self.task_config.DATA, os.path.basename(src))
                copylist.append([src, dest])

            output_file = os.path.join(
                self.task_config.DATA, ob_data["output_file"]
            )
            self.registered_outputs.append(output_file)

            self.script2netcdf_obs[ob_name] = {
                "staged_inputs": staged_inputs,
                "output_file": output_file,
                "script_file": staged_scripts,
                "mpi": ob_data.get("mpi", 1),
            }

        # Stage files into execution workspace
        FileHandler({"mkdir": mkdir_list}).sync()
        if copylist:
            FileHandler({"copy_opt": copylist}).sync()

        # Update mapping file timestamps if needed
        for dest in staged_mappings:
            try:
                yaml_file = YAMLFile(dest)
                if (
                    "bufr" in yaml_file
                    and "variables" in yaml_file["bufr"]
                    and "timestamp" in yaml_file["bufr"]["variables"]
                    and "timeoffset" in yaml_file["bufr"]["variables"]["timestamp"]
                ):
                    yaml_file["bufr"]["variables"]["timestamp"]["timeoffset"][
                        "referenceTime"
                    ] = self.task_config.current_cycle.strftime(
                        "%Y-%m-%dT%H:%M:%SZ"
                    )
                    yaml_file.save(f"{dest}.tmp")
                    os.replace(f"{dest}.tmp", dest)
            except Exception as e:
                logger.warning(f"Failed to update referenceTime in {dest}: {e}")

    @logit(logger)
    def execute(self) -> None:
        """
        Execute AWS converters in parallel across worker pools.
        """
        exec_cmd_list = []
        mpi_count = 0

        cycle_str = self.task_config.current_cycle.strftime("%Y%m%d%H")
        half_win_str = str(self.task_config.half_window_hours)

        for ob_name, ob_data in self.script2netcdf_obs.items():
            staged_inputs = ob_data["staged_inputs"]
            output_file = ob_data["output_file"]
            script_files = ob_data.get("script_file", [])

            if not script_files:
                logger.error(f"No script_file provided for '{ob_name}'. Skipping.")
                continue

            script_file = script_files[0]
            mpi = ob_data.get("mpi", 1)

            logger.info(
                f"Converting AWS granules to {output_file} using {script_file} "
                f"for cycle {cycle_str} (Window: ±{half_win_str}h, MPI={mpi})"
            )

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
                launcher = self.task_config.get("MPI_LAUNCHER", "srun").lower()
                if launcher == "mpiexec":
                    exec_cmd = Executable("mpiexec")
                    args = ["-n", str(mpi), "python"] + cmd_args
                else:
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

        if not exec_cmd_list:
            logger.warning("No AWS conversion commands to execute.")
            return

        num_workers = min(
            len(exec_cmd_list) + mpi_count + 5, max(1, mp.cpu_count() - 1)
        )
        logger.info(
            f"Executing {len(exec_cmd_list)} task(s) across {num_workers} parallel workers"
        )

        with mp.Pool(num_workers) as pool:
            pool.starmap(mp_aws_converter, exec_cmd_list)

    @logit(logger)
    def finalize(self) -> None:
        """
        Copy generated IODA files to COMOUT, write summary log, and run filter.
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

        # Look for generated .nc files in DATA root (excluding $DATA/inputs)
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

        # Summary log
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

        stats_yaml = os.path.join(self.task_config.DATA, "stats.yaml")
        save_as_yaml(summary_dict, stats_yaml)

        exec_cmd = Executable(
            os.path.join(
                self.task_config.HOMEobsforge,
                "build",
                "bin",
                "ioda-summary.x",
            )
        )
        exec_cmd.add_default_arg(stats_yaml)

        try:
            logger.info(f"Creating summary file {ready_file}")
            exec_cmd()
        except Exception as e:
            logger.warning(
                f"Failed to create summary file {ready_file}: {e}. Creating empty ready file."
            )
            ready_file.touch()

        # Restriction filter
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


AtmosAwsPrepObs = AtmosAwsObsPrep

if __name__ == "__main__":
    config = parse_task_config(sys.argv[1:])
    task = AtmosAwsObsPrep(config)
    task.run()
