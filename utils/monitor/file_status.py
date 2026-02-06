class FileStatus:
    # --- Log-derived (intent) ---
    DECLARED   = "DECLARED"    # Found in logs only

    # --- Filesystem-derived ---
    MISSING    = "MISSING"     # Declared but not found
    EXISTS     = "EXISTS"      # Found on disk, not inspected
    EMPTY      = "EMPTY"       # Exists but size == 0
    ERR_ACCESS = "ERR_ACCESS"  # Stat/open failure

    # --- Content-derived (Inspector) ---
    OK         = "OK"          # Valid content, obs_count > 0
    ZEROOBS    = "ZEROOBS"     # Valid content, obs_count == 0
    WARNING    = "WARNING"     # Valid but anomalies
    CORRUPT    = "CORRUPT"     # Unreadable / parse failure
