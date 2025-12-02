VALID_CYCLES = ["00", "06", "12", "18"]


def parse_timestamp(ts: str):
    """Convert 'YYYYMMDDHH' → (YYYYMMDD, HH)."""
    ts = str(ts)
    date = ts[:8]
    cycle = ts[8:]
    if cycle not in VALID_CYCLES:
        raise ValueError(f"Invalid cycle '{cycle}'")
    return date, cycle


def iter_timestamps(start_ts: str, end_ts: str):
    """
    Iterate timestamps from YYYYMMDDHH to YYYYMMDDHH in 6-hour increments.
    Cycles: 00, 06, 12, 18
    """
    from datetime import datetime, timedelta

    s_date, s_cycle = parse_timestamp(start_ts)
    e_date, e_cycle = parse_timestamp(end_ts)

    dt = datetime.strptime(s_date + s_cycle, "%Y%m%d%H")
    end_dt = datetime.strptime(e_date + e_cycle, "%Y%m%d%H")

    while dt <= end_dt:
        date = dt.strftime("%Y%m%d")
        cycle = dt.strftime("%H")
        yield date, cycle
        dt += timedelta(hours=6)

