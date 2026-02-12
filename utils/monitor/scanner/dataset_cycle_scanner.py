import os
import re
import logging
from typing import Dict

from .dataset import Dataset


def dataset_cycle_scanner(data_root: str, db_path: str) -> None:
    """
    Phase 1 Scanner:
    ----------------
    Discovers:
        - Datasets
        - Their cycles (date + hour)

    Directory layout expected:

        <data_root>/
            gfs.20260204/
                00/
                06/
                12/
                18/
            gdas.20260204/
                00/
                ...

    This function:
        1. Parses dataset and date from directory names.
        2. Validates cycle hour directories.
        3. Delegates persistence to Dataset object.

    It does NOT:
        - Inspect file contents
        - Inspect obs spaces
        - Perform IODA validation
    """

    # Define the valid hour values explicitly.
    # This enforces domain rules strictly.
    VALID_HOURS = {"00", "06", "12", "18"}

    # Regex to match directories like:
    #   gfs.20260204
    #   gdas.20260131
    #
    # Captures:
    #   name  -> dataset name
    #   date  -> 8-digit cycle date
    DATASET_DIR_PATTERN = re.compile(
        r"^(?P<name>[A-Za-z0-9_]+)\.(?P<date>\d{8})$"
    )

    logging.info(f"Scanning data root: {data_root}")

    # Dictionary used to cache Dataset objects.
    # This prevents creating multiple objects for the same dataset.
    # Key: dataset_name
    # Value: Dataset instance
    dataset_objects: Dict[str, Dataset] = {}

    # Iterate over everything in the data root directory.
    for entry in os.listdir(data_root):

        # Construct absolute path to the entry.
        entry_path = os.path.join(data_root, entry)

        # Skip if not a directory.
        # We only care about directories at this stage.
        if not os.path.isdir(entry_path):
            continue

        # Attempt to match dataset.date pattern.
        match = DATASET_DIR_PATTERN.match(entry)

        # If directory does not match expected pattern, skip it.
        # Example skipped directories:
        #   logs/
        #   tmp/
        if not match:
            logging.debug(f"Skipping non-run directory: {entry}")
            continue

        # Extract dataset name (e.g., 'gfs')
        dataset_name = match.group("name")

        # Extract cycle date (e.g., '20260204')
        cycle_date = match.group("date")

        logging.info(f"Discovered run directory: {dataset_name}.{cycle_date}")

        # Create Dataset object only once per dataset.
        if dataset_name not in dataset_objects:

            # Instantiate Dataset domain object.
            # This object owns persistence logic internally.
            dataset = Dataset(db_path, dataset_name)

            # Cache it for reuse.
            dataset_objects[dataset_name] = dataset

            logging.info(f"Registered dataset: {dataset_name}")

        else:
            # Retrieve previously created Dataset object.
            dataset = dataset_objects[dataset_name]

        # Now scan subdirectories for cycle hours.
        # Expected subdirs: 00, 06, 12, 18
        for hour_entry in os.listdir(entry_path):

            # Construct absolute path to hour directory.
            hour_path = os.path.join(entry_path, hour_entry)

            # Skip if not a directory.
            if not os.path.isdir(hour_path):
                continue

            # Enforce strict domain rule:
            # Only accept exact valid cycle hours.
            if hour_entry not in VALID_HOURS:
                logging.warning(
                    f"Ignoring invalid cycle hour '{hour_entry}' "
                    f"in {entry_path}"
                )
                continue

            # At this point:
            #   dataset_name is valid
            #   cycle_date is valid
            #   hour_entry is valid
            cycle_hour = hour_entry

            logging.info(
                f"Registering cycle: "
                f"{dataset_name} {cycle_date} {cycle_hour}"
            )

            # Delegate cycle registration to Dataset object.
            # Dataset handles:
            #   - ensuring dataset exists in DB
            #   - inserting cycle into dataset_cycles
            #   - avoiding duplicates
            dataset.add_cycle(cycle_date, cycle_hour)

    logging.info("Phase 1 scanning complete.")

