import logging
import sys
import time


def configure_logging(level=logging.INFO, log_file=None, use_utc=True):
    """
    Configure root logger once.
    Call this only from top-level scripts.
    """

    formatter = logging.Formatter(
        fmt="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S"
    )

    if use_utc:
        formatter.converter = time.gmtime

    handlers = []

    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    handlers.append(console_handler)

    # Optional file handler
    if log_file:
        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(formatter)
        handlers.append(file_handler)

    logging.basicConfig(
        level=level,
        handlers=handlers,
        force=True  # important if re-running in interactive sessions
    )
