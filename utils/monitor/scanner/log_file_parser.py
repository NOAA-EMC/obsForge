import logging
import os
import re
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)


# -------------------------------
# Helper: Extract cycle
# -------------------------------

def extract_cycle_from_lines(lines):
    cycle_candidates = []
    valid_cycles = {"00", "06", "12", "18"}

    p_tXXz = re.compile(r"\bcycle\s*=\s*t([0-9]{2})z\b")
    p_export_tXXz = re.compile(r"\bexport\s+cycle\s*=\s*t([0-9]{2})z\b")
    p_export_cyc = re.compile(r"\bexport\s+cyc=['\"]?([0-9]{2})['\"]?")
    p_current_cycle_dt = re.compile(
        r"current cycle:\s*([0-9\-]+\s+[0-9:]+)"
    )
    p_previous_cycle = re.compile(r"previous cycle:", re.IGNORECASE)

    for line in lines:
        if p_previous_cycle.search(line):
            continue

        m = p_tXXz.search(line) or p_export_tXXz.search(line)
        if m:
            cyc = m.group(1)
            if cyc in valid_cycles:
                cycle_candidates.append(cyc)
            continue

        m = p_export_cyc.search(line)
        if m:
            cyc = m.group(1)
            if cyc in valid_cycles:
                cycle_candidates.append(cyc)
            continue

        m = p_current_cycle_dt.search(line)
        if m:
            dt_str = m.group(1)
            try:
                dt = datetime.strptime(dt_str, "%Y-%m-%d %H:%M:%S")
                cyc = dt.strftime("%H")
                if cyc in valid_cycles:
                    cycle_candidates.append(cyc)
            except Exception:
                pass

    if not cycle_candidates:
        return None

    unique = set(cycle_candidates)
    if len(unique) > 1:
        raise ValueError(
            f"Inconsistent cycle definitions found: {sorted(unique)}"
        )

    return cycle_candidates[0]


# -------------------------------
# Helper: Extract run_type
# -------------------------------

def extract_run_type_from_lines(lines):
    pattern = re.compile(
        r"export\s+RUN=['\"]?(gdas|gfs)['\"]?", re.IGNORECASE
    )
    run_types = []

    for line in lines:
        m = pattern.search(line)
        if m:
            run_types.append(m.group(1).lower())

    if not run_types:
        return None

    unique = set(run_types)
    if len(unique) > 1:
        raise ValueError(
            f"Inconsistent run_type definitions found: {sorted(unique)}"
        )

    return run_types[0]


# -------------------------------
# Helper: Extract start/end/elapsed/error
# -------------------------------

def extract_job_times_from_lines(lines, job_script):
    begin_pattern = re.compile(rf"Begin {re.escape(job_script)} at (.+)")
    end_pattern = re.compile(
        rf"End {re.escape(job_script)} at ([0-9:]+).*?"
        rf"error code (\d+).*?\(time elapsed: ([0-9:]+)\)"
    )

    start_date = end_date = elapsed_time = error_code = None

    for line in lines:
        m = begin_pattern.search(line)
        if m:
            start_str = m.group(1).strip()
            try:
                start_date = datetime.strptime(
                    start_str, "%a %b %d %H:%M:%S %Z %Y"
                )
            except ValueError:
                try:
                    start_date = datetime.strptime(
                        start_str, "%a %b %d %H:%M:%S %Y"
                    )
                except ValueError:
                    start_date = start_str
            continue

        m = end_pattern.search(line)
        if m:
            end_str = m.group(1).strip()
            error_code = int(m.group(2))
            elapsed_str = m.group(3).strip()

            if start_date and isinstance(start_date, datetime):
                end_date = datetime.strptime(
                    f"{start_date.strftime('%Y-%m-%d')} {end_str}",
                    "%Y-%m-%d %H:%M:%S"
                )
            else:
                end_date = end_str

            try:
                h, mn, s = map(int, elapsed_str.split(":"))
                elapsed_time = timedelta(hours=h, minutes=mn, seconds=s)
            except Exception:
                elapsed_time = elapsed_str
            continue

    return start_date, end_date, elapsed_time, error_code


# -------------------------------
# Main parse function
# -------------------------------

def parse_job_log(logfile_path: str, job_script_name: str):
    job_script = f'{job_script_name}.sh'
    if not os.path.isfile(logfile_path):
        raise FileNotFoundError(f"Log file does not exist: {logfile_path}")

    with open(logfile_path, "r", encoding="utf-8") as f:
        lines = f.readlines()

    cycle = extract_cycle_from_lines(lines)
    run_type = extract_run_type_from_lines(lines)
    start_date, end_date, elapsed_time, error_code = \
        extract_job_times_from_lines(lines, job_script)

    if (start_date is None and end_date is None and
            cycle is None and run_type is None):
        return None

    return {
        "start_date": start_date,
        "end_date": end_date,
        "elapsed_time": elapsed_time,
        "error_code": error_code,
        "cycle": cycle,
        "run_type": run_type
    }


def elapsed_to_seconds(elapsed):
    if isinstance(elapsed, timedelta):
        return int(elapsed.total_seconds())

    if isinstance(elapsed, str):
        h, m, s = map(int, elapsed.split(":"))
        return h * 3600 + m * 60 + s

    return None


def parse_master_log(filepath):
    """
    Parses the Workflow master log to find task execution details.
    Target Line Format:
    YYYY-MM-DD HH:MM:SS ... :: host :: Task NAME, jobid=ID, ...
    """
    tasks = []
    pattern = re.compile(
        r"^(\d{4}-\d{2}-\d{2}\s\d{2}:\d{2}:\d{2}).*?::\s+(\w+)\s+::\s+"
        r"Task\s+([\w_]+),\s+jobid=(\d+),\s+in\s+state\s+([A-Z]+).*?"
        r"ran\s+for\s+([\d\.]+)\s+seconds,\s+exit\s+status=(\d+),\s+try=(\d+)"
    )

    try:
        with open(filepath, 'r') as f:
            for line in f:
                clean_line = re.sub(r'\x1B\[[0-?]*[ -/]*[@-~]', '', line)
                match = pattern.search(clean_line)
                if match:
                    tasks.append({
                        "timestamp": match.group(1),
                        "host": match.group(2),
                        "task_name": match.group(3),
                        "job_id": match.group(4),
                        "status": match.group(5),
                        "duration": float(match.group(6)),
                        "exit_code": int(match.group(7)),
                        "attempt": int(match.group(8))
                    })
    except Exception as e:
        logger.error(f"Error reading master log {filepath}: {e}")

    return tasks


# -------------------------------
# 2. Output File Parser
# -------------------------------

def parse_output_files_from_log(filepath, data_root):
    """
    Scans a task log for 'file_utils' copies to find output files.
    Returns a list of relative paths found in the log.
    """
    files_found = set()
    abs_root = os.path.abspath(data_root)

    # Matches: "... - file_utils : Copied /src/file.nc to /dest/file.nc"
    copy_pattern = re.compile(
        r"file_utils\s+:\s+Copied\s+.*?\s+to\s+([^\s]+)"
    )
    # Matches: "... - file_utils : Created /dest/dir"
    create_pattern = re.compile(r"file_utils\s+:\s+Created\s+([^\s]+)")

    try:
        with open(filepath, 'r') as f:
            for line in f:
                # Clean ANSI codes
                clean_line = re.sub(
                    r'\x1B\[[0-?]*[ -/]*[@-~]', '', line
                ).strip()

                m = copy_pattern.search(clean_line)
                if not m:
                    m = create_pattern.search(clean_line)

                if m:
                    dest_path = m.group(1).rstrip(".,;:'\"")

                    if not os.path.isabs(dest_path):
                        continue

                    abs_dest = os.path.abspath(dest_path)
                    if abs_dest.startswith(abs_root):
                        rel_path = os.path.relpath(abs_dest, abs_root)
                        files_found.add(rel_path)
    except Exception:
        pass

    return list(files_found)
