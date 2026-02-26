import os
import re
import logging
from typing import Optional
from sqlalchemy import select
from sqlalchemy.orm import Session

from .obs_space_orm import ObsSpaceORM
from .ioda_structure import IodaStructure

logger = logging.getLogger(__name__)


class ObsSpace:
    """
    Domain object representing an ObsSpace.
    It is a global type definition.
    """

    def __init__(
        self,
        name: str,
        ioda_structure: IodaStructure,
        id: Optional[int] = None,
        ioda_structure_id: Optional[int] = None,
    ):
        self.name = name
        self.ioda_structure = ioda_structure

        self.id = id

        # to be deprecated:
        self.ioda_structure_id = ioda_structure_id

    def __repr__(self) -> str:
        return f"ObsSpace name='{self.name}', id={self.id}"
        # return f"ObsSpace(name='{self.name}', id={self.id}, struct_id={self.ioda_structure_id})"


    # methods for parsing the name
    SEPARATOR = "."
    EXTENSION = "nc"
    NAME_INDEX = 2
    EXPECTED_PARTS = 4

    @classmethod
    def get_search_pattern(cls, prefix: str, hour: str) -> str:
        """Generates the glob: {prefix}.t{hour}z.*.nc"""
        if isinstance(hour, int):
            hour = f"{hour:02d}"
        
        return cls.SEPARATOR.join([prefix, f"t{hour}z", "*", cls.EXTENSION])

    # prefix = name of the data set
    @classmethod
    def parse_name_from_filename(cls, path: str, prefix: Optional[str] = None) -> Optional[str]:
        """
        Parses the name using FILENAME_PARTS. 
        If prefix is provided, it enforces a strict match against the first part.
        """
        filename = os.path.basename(path)
        parts = filename.split(cls.SEPARATOR)

        # 1. Structural & Extension Check
        if len(parts) != cls.EXPECTED_PARTS or parts[-1] != cls.EXTENSION:
            return None

        # 2. Strict Prefix Check (Optional)
        if prefix is not None and parts[0] != prefix:
            return None

        # 3. Cycle string validation (tNNz)
        if not re.fullmatch(r"t\d{2}z", parts[1]):
            return None

        # 4. Extract based on the defined index
        return parts[cls.NAME_INDEX]


    @classmethod
    def from_file(cls, file_path: str, prefix: Optional[str] = None) -> Optional["ObsSpace"]:
        """
        Static in-memory constructor.
        Passes the optional prefix (=name of dataset) 
            to the parser for stricter validation.
        """
        name = cls.parse_name_from_filename(file_path, prefix=prefix)
        if name is None:
            return None

        structure = IodaStructure.from_file(file_path)
        if structure is None:
            return None

        this_obs_space = cls(name=name, ioda_structure=structure)
        # logger.debug(f"constructed {this_obs_space} from {file_path}")
        return this_obs_space


    def to_orm(self):
        return ObsSpaceORM(
            name=self.name,
            ioda_structure_id=self.ioda_structure.id,
            # ioda_structure_id=self.ioda_structure_id,
        )

    def to_db(self, session: Session) -> int:
        """Ensure this ObsSpace exists in the DB. Idempotent."""

        if self.id is not None:
            return self.id

        current_ioda_structure_id = self.ioda_structure.to_db(session)

        existing = session.execute(
            select(ObsSpaceORM).where(ObsSpaceORM.name == self.name)
        ).scalar_one_or_none()

        if existing:
            if current_ioda_structure_id != existing.ioda_structure_id:
                logger.error(
                    f"STRUCTURAL DISCREPANCY DETECTED\n"
                    f"ObsSpace: {self.name}\n"
                    # f"File: {file_path}\n"
                    f"Expected Ioda Struct ID: {existing.ioda_structure_id}\n"
                    f"Actual Ioda Struct ID:   {current_ioda_structure_id}"
                )

            self.id = existing.id
            return self.id

        orm_obj = self.to_orm()
        session.add(orm_obj)
        session.commit()
        # session.flush() # Changed from commit() to allow Dataset to manage transaction
        self.id = orm_obj.id
        # logger.debug(f"to_db {self}")

        return self.id
