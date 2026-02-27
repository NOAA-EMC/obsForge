import os
import re
import logging

from datetime import datetime, date
from typing import List, Optional, Tuple, Set

from sqlalchemy import (
    select,
)
# from sqlalchemy.orm import declarative_base, relationship, Session
from sqlalchemy.orm import Session

# from .db_base import Base  # SQLAlchemy declarative base
from .dataset_orm import (
    DatasetORM, 
    DatasetCycleORM, 
    DatasetFieldORM,
    DatasetFileORM
)
from .obs_space_orm import ObsSpaceORM

from .file import File
from .obs_space import ObsSpace
from .dataset_cycle import DatasetCycle
from .dataset_field import DatasetField
from .dataset_file import DatasetFile


logger = logging.getLogger(__name__)


class Dataset:
    """
    Domain object representing a dataset.

    In-memory representation first.
    Persist explicitly using to_db(session).
    """

    def __init__(
        self,
        name: str,
        root_dir: str,
        id: Optional[int] = None
    ):
        # db quantities
        self.id = id
        self.name = name
        self.root_dir = root_dir

        self.dataset_fields: List[DatasetField] = []
        self.dataset_cycles: List[DatasetCycle] = []

    def __repr__(self) -> str:
        return (
            f"Dataset {self.name}, "
            f"id = {self.id}, "
            f"{self.root_dir}, \n"
            f"{len(self.dataset_cycles)} cycles, "
            f"{len(self.dataset_fields)} fields"
        )

    @staticmethod
    def _select_cycles(cycles: list["DatasetCycle"], n: Optional[int] = None) -> list["DatasetCycle"]:
        if not cycles:
            return []
        if n is None:
            selected = cycles
        elif n == 0:
            return []
        elif n > 0:
            selected = cycles[:n]
        else:  # n < 0
            selected = cycles[n:]
        return selected


    # --------------------------------------------------------
    # In-memory operations
    # --------------------------------------------------------

    def add_field(self, field):
        self.dataset_fields.append(field)


    def add_cycle(self, cycle: "DatasetCycle"):
        """
        Add a cycle to the dataset.

        For every field in the cycle:
          - If the dataset does not yet have a field with the same obs_space,
            add it.
          - Otherwise merge the files from the cycle field into the existing field.
        """

        # design problem --> consistency issue
        if cycle.dataset is not self:
            raise ValueError("Cycle does not belong to this dataset")

        self.dataset_cycles.append(cycle)

        for cycle_field in cycle.fields:
            # Find all fields in dataset with same obs_space name
            same_name_fields = [
                f for f in self.dataset_fields
                if f.obs_space.name == cycle_field.obs_space.name
            ]

            if not same_name_fields:
                # existing_field = None
                cycle_field.dataset = self
                self.dataset_fields.append(cycle_field)
                # logger.info(f"Added new {cycle_field} to dataset {self}")
            else:
                existing_field = same_name_fields[0]

                if cycle_field.obs_space.compare(existing_field.obs_space):
                    continue  # skip this cycle_field

                '''
                existing_hash = existing_field.obs_space.netcdf_structure.structure_hash
                new_hash = cycle_field.obs_space.netcdf_structure.structure_hash

                if existing_hash != new_hash:
                    logger.error(
                        "ObsSpace name conflict with different structure hash: "
                        f"name={cycle_field.obs_space.name}, "
                        f"existing_hash={existing_hash}, "
                        f"new_hash={new_hash}"
                    )
                    continue  # skip this cycle_field
                '''

                # add the file from this field to the existing field
                # there is exactly one file
                ds_file = cycle_field.files[0]
                ds_file.dataset_field = existing_field
                existing_field.files.append(ds_file)
                # logger.info(f"Added to existing field {cycle_field} to dataset {self}")

        logger.info(f"Added {cycle} to dataset {self}")


    # --------------------------------------------------------
    # Persistence
    # --------------------------------------------------------
    # every entity that has id must:
    # 1. Convert to ORM
    # 2. Add to session
    # 3. Flush
    # 4. Copy generated id back to domain object

    def to_orm(self) -> "DatasetORM":
        return DatasetORM(
            id=self.id,
            name=self.name,
            root_dir=self.root_dir,
        )

    def to_db_self(self, session):
        """
        Persist the Dataset itself in the database.
        """
        from .dataset_orm import DatasetORM  # Import ORM class here only

        # logger.info("Dataset.to_db_self")

        # Check if this dataset already exists
        existing = session.scalar(
            select(DatasetORM).where(DatasetORM.name == self.name)
        )

        if existing:
            # Update the id to link children later
            self.id = existing.id
            return existing

        orm_obj = self.to_orm()
        session.add(orm_obj)
        session.flush()  # Ensure id is assigned
        self.id = orm_obj.id
        return orm_obj

    def to_db(self, session: "Session", n: Optional[int] = None) -> None:
        self.to_db_self(session)

        # Persist fields without persisting files
        # persisting the obs spaces and underlying NETCDF structures
        for field in self.dataset_fields:
            field.to_db(session)
            logger.info(f"persisted {field}")

        self.to_db_cycles(session, n)
        logger.info(f"to_db {self}")

    def to_db_cycles(self, session, n):
        cycles = Dataset._select_cycles(self.dataset_cycles, n)
        # Persist cycles, including files
        for cycle in cycles:
            cycle.to_db(session)


    def discover_cycles(self) -> list[tuple[date, str]]:
        """
        Discover available cycles in the dataset root directory.

        Returns
        -------
        List of (cycle_date, cycle_hour) tuples sorted chronologically.
        Does NOT modify in-memory state.
        """

        if not self.root_dir or not os.path.isdir(self.root_dir):
            logger.warning(f"Invalid root_dir for dataset '{self.name}'")
            return []

        DATASET_DIR_PATTERN = re.compile(
            rf"^{re.escape(self.name)}\.(\d{{8}})$"
        )

        discovered = []

        for entry in os.listdir(self.root_dir):
            entry_path = os.path.join(self.root_dir, entry)

            if not os.path.isdir(entry_path):
                continue

            match = DATASET_DIR_PATTERN.match(entry)
            if not match:
                continue

            cycle_date_str = match.group(1)

            try:
                cycle_date = datetime.strptime(
                    cycle_date_str, "%Y%m%d"
                ).date()
            except ValueError:
                logger.warning(f"Invalid date format: {cycle_date_str}")
                continue

            for hour_entry in os.listdir(entry_path):
                hour_path = os.path.join(entry_path, hour_entry)

                if (
                    os.path.isdir(hour_path)
                    and hour_entry in DatasetCycle.VALID_HOURS
                ):
                    discovered.append((cycle_date, hour_entry))

        # Always return sorted (important!)
        return sorted(discovered, key=lambda x: (x[0], x[1]))



    def read_cycle(self, cycle_date: date, cycle_hour: str) -> Optional[DatasetCycle]:
        cycle_dir = DatasetCycle.cycle_dir(self, cycle_date, cycle_hour)
        if not os.path.isdir(cycle_dir):
            logger.warning(f"Cycle directory does not exist: {cycle_dir}")
            return None

        cycle = DatasetCycle.from_directory(self, cycle_dir)
        return cycle

    def read_cycles(self, n: Optional[int] = None) -> list["DatasetCycle"]:
        """
        Parameters
        ----------
        n:
            > 0  → read first n cycles
            < 0  → read last |n| cycles
            None → read all cycles
            0    → read none
        """

        discovered = self.discover_cycles()

        selected = Dataset._select_cycles(discovered, n)

        for cycle_date, cycle_hour in selected:
            cycle = self.read_cycle(cycle_date, cycle_hour)
            self.add_cycle(cycle)

    # def compute_derived_attributes(self):
        # for cycle in self.cycles:
            # cycle.compute_derived_attributes()
# 
    # def to_db_derived_attributes(session, self):
        # for cycle in self.cycles:
            # cycle.to_db_derived_attributes(session)

