import os
import re
import logging
logger = logging.getLogger(__name__)

from datetime import datetime, date
from typing import List, Optional, Tuple, Set

from sqlalchemy import (
    select,
    and_
)
from sqlalchemy.orm import Session

from .dataset_orm import (
    DatasetORM, 
    CycleORM, 
    FieldORM,
    DatasetFileORM
)
from .obs_space_orm import ObsSpaceORM

from .file import File
from .obs_space import ObsSpace
from .netcdf_structure import NetcdfStructure
from .cycle import Cycle
from .field import Field
from .dataset_file import DatasetFile


class Dataset:
    """
    Domain object representing a dataset.

    In-memory representation independent of db persistence.
    Persist explicitly using to_db(session).

    Data arrives in cycles, and is persisted in cycles,
    but the Dataset is a collection of fields.
    A field is a collection of files of the same ObsSpace type,
    with at most 1 file per cycle.
    """

    def __init__(
        self,
        name: str,
        root_dir: str,
        id: Optional[int] = None
    ):
        self.id = id
        self.name = name
        self.root_dir = root_dir

        self.fields: List[Field] = []
        self.cycles: List[Cycle] = []

    def __repr__(self) -> str:
        return (
            f"Dataset {self.name}, "
            f"id = {self.id}, "
            f"{self.root_dir}, \n"
            f"{len(self.cycles)} cycles, "
            f"{len(self.fields)} fields"
        )


    """
    Parameters
    ----------
    n:
        > 0  → read first n cycles
        < 0  → read last |n| cycles
        None → read all cycles
        0    → read none
    """
    @staticmethod
    def _select_cycles(cycles: list["Cycle"], n: Optional[int] = None) -> list["Cycle"]:
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

    @classmethod
    def from_db_self(cls, orm: "DatasetORM") -> "Dataset":
        if not orm:
            return None
        instance = cls(
            name=orm.name,
            root_dir=orm.root_dir,
            id=orm.id
        )
        return instance

    def find_field_by_id(self, field_id: int) -> Optional[Field]:
        """Finds a loaded Field domain object by its database ID."""
        for field in self.fields:
            if field.id == field_id:
                return field
        return None

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
        from .dataset_orm import DatasetORM

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

    def to_db(self, repo, n: Optional[int] = None) -> None:
        repo.save_dataset(self)

        for field in self.fields:
            repo.save_field(field)

        cycles = Dataset._select_cycles(self.cycles, n)
        for cycle in cycles:
            repo.save_cycle(cycle)

    def add_cycle(self, cycle):
        self.cycles.append(cycle)
        self.cycles.sort()

    def build_cycle(self, cycle_date, cycle_hour, scan_results):
        cycle = Cycle(self, cycle_date, cycle_hour)
        # logger.info("build_cycle:")

        for file, obs_space_name in scan_results:
            nc_structure = NetcdfStructure.from_file(file.path)
            # logger.info(f"FILE = {file.path}\nHASH = {nc_structure.structure_hash}")
            if nc_structure is None:
                logger.warning(f"Unable to read netcdf structure for {f.path}") 
                continue
            obs_space = ObsSpace(obs_space_name, nc_structure)

            field = self.get_or_create_field(obs_space)
            if not field:
                continue

            ds_file = DatasetFile.from_file(file, field, cycle)

            field.add_file(ds_file)
            cycle.add_file(ds_file)

        return cycle

    def get_or_create_field(self, obs_space: ObsSpace) -> Field:
        for f in self.fields:
            if f.obs_space.name == obs_space.name:
                if not f.obs_space.compare(obs_space):
                    # raise ValueError(
                    logger.error(
                        f"ObsSpace structure mismatch for '{obs_space.name}'"
                    )
                    return None
                return f

        new_field = Field(self, obs_space)
        self.fields.append(new_field)
        return new_field


    def load_cycles_from_db(self, session: Session) -> None:
        """
        Loads all Cycle identities (Date/Hour) without loading files.
        """
        if self.id is None:
            logger.error(f"Cannot load cycles for dataset '{self.name}': ID is missing.")
            return

        # Query all cycles for this dataset
        stmt = (
            select(CycleORM)
            .where(CycleORM.dataset_id == self.id)
            .order_by(CycleORM.cycle_date.desc(), CycleORM.cycle_hour.desc())
        )
        cycle_orms = session.scalars(stmt).all()
        logger.info(f"QUERY: Dataset ID is {self.id}")
        logger.info(f"RESULT: Found {len(cycle_orms)} cycles in DB")

        self.cycles = []
        for c_orm in cycle_orms:
            cycle_domain = Cycle.from_orm(c_orm, self)
            self.cycles.append(cycle_domain)

        logger.info(f"Found {len(self.cycles)} cycles for dataset '{self.name}'")
