import re

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


def normalize_rocoto_timestamp(pdy_raw, cyc_raw):
    """
    Normalize ANY Rocoto timestamp input into YYYYMMDDHH.

    pdy_raw examples:
       "2025-12-02"
       "20251202"
       "2025/12/02"
       "2025-12-02 00:00:00.000"
       "2025-12-02T00"
       etc.

    cyc_raw examples:
       "0"
       "00"
       "6"
       "18"
       "00:00:00.000"
       None (missing)
    """

    # ---------------------------------------------------------
    # 1. Extract date (YYYYMMDD) from PDY using regex
    # ---------------------------------------------------------

    # Try to find Y M D parts in any punctuation-separated string
    m = re.search(r"(\d{4})\D*(\d{2})\D*(\d{2})", str(pdy_raw))
    if not m:
        raise ValueError(f"Cannot extract date from PDY='{pdy_raw}'")

    yyyy, mm, dd = m.groups()
    pdy = f"{yyyy}{mm}{dd}"  # normalized

    # ---------------------------------------------------------
    # 2. Normalize cycle hour
    # ---------------------------------------------------------

    if cyc_raw is None:
        # try extracting hours from pdy_raw if missing entirely
        m2 = re.search(r"\b(\d{1,2})[:.]?\d?\d?\b", str(pdy_raw))
        if m2:
            cyc_raw = m2.group(1)
        else:
            raise ValueError(f"Cannot extract cycle hour from cyc='{cyc_raw}' or PDY='{pdy_raw}'")

    # Extract first integer hour from the cycle string
    m3 = re.search(r"(\d{1,2})", str(cyc_raw))
    if not m3:
        raise ValueError(f"Cannot parse cycle hour from cyc='{cyc_raw}'")

    cyc = m3.group(1).zfill(2)  # "0" → "00", "6" → "06"

    # ---------------------------------------------------------
    # 3. Final timestamp: YYYYMMDDHH
    # ---------------------------------------------------------
    return f"{pdy}{cyc}"
