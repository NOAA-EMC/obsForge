import os
import re
import logging
from typing import Optional
from sqlalchemy import select
from sqlalchemy.orm import Session

from .obs_space_orm import ObsSpaceORM

logger = logging.getLogger(__name__)


class ObsSpace:
    """
    Domain object representing an ObsSpace.
    It is a global type definition.
    """

    def __init__(
        self,
        name: str,
        id: Optional[int] = None,
        ioda_structure_id: Optional[int] = None,
    ):
        self.id = id
        self.name = name
        self.ioda_structure_id = ioda_structure_id


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

        return cls(name=name, ioda_structure=structure)


    def to_orm(self):
        return ObsSpaceORM(
            name=self.name,
            ioda_structure_id=self.ioda_structure_id,
        )

    def verify_and_set_structure(self, session: Session, new_struct_id: int, file_path: str):
        """
        Validates a file's structure against the ObsSpace blueprint.
        If no blueprint exists, the new_struct_id becomes the blueprint.
        """
        # Ensure we have the latest state from DB if we don't have an ID yet
        if self.ioda_structure_id is None:
            self.sync_from_db(session)

        if self.ioda_structure_id is None:
            logger.info(f"ObsSpace '{self.name}': Setting initial IODA structure blueprint from {file_path}")
            self.ioda_structure_id = new_struct_id
            # Immediately update the DB to 'lock in' the blueprint for this type
            self.update_structure_in_db(session)
        
        elif self.ioda_structure_id != new_struct_id:
            logger.error(
                f"STRUCTURAL DISCREPANCY DETECTED\n"
                f"ObsSpace: {self.name}\n"
                f"File: {file_path}\n"
                f"Expected Struct ID: {self.ioda_structure_id}\n"
                f"Actual Struct ID:   {new_struct_id}"
            )
            # You could raise a custom Exception here if you want to stop processing
    
    def sync_from_db(self, session: Session):
        """Refresh local state from DB based on name."""
        existing = session.execute(
            select(ObsSpaceORM).where(ObsSpaceORM.name == self.name)
        ).scalar_one_or_none()
        
        if existing:
            self.id = existing.id
            self.ioda_structure_id = existing.ioda_structure_id

    def update_structure_in_db(self, session: Session):
        """Persists the assigned ioda_structure_id to the global ObsSpace definition."""
        if self.id is None:
            self.to_db(session)
            
        session.query(ObsSpaceORM).filter(ObsSpaceORM.id == self.id).update(
            {"ioda_structure_id": self.ioda_structure_id}
        )
        session.flush()

    # --------------------------------------------------------
    # Persistence
    # --------------------------------------------------------

    def to_db(self, session: Session) -> int:
        """Ensure this ObsSpace exists in the DB. Idempotent."""
        if self.id is not None:
            return self.id

        existing = session.execute(
            select(ObsSpaceORM).where(ObsSpaceORM.name == self.name)
        ).scalar_one_or_none()

        if existing:
            self.id = existing.id
            self.ioda_structure_id = existing.ioda_structure_id
            return self.id

        orm_obj = self.to_orm()
        session.add(orm_obj)
        session.flush() # Changed from commit() to allow Dataset to manage transaction
        self.id = orm_obj.id
        return self.id

    @classmethod
    def from_db(cls, session: Session, obs_space_id: int) -> Optional["ObsSpace"]:
        orm_obj = session.get(ObsSpaceORM, obs_space_id)
        if orm_obj is None:
            return None

        return cls(
            id=orm_obj.id,
            name=orm_obj.name,
            ioda_structure_id=orm_obj.ioda_structure_id,
        )

    def __repr__(self) -> str:
        return f"ObsSpace(name='{self.name}', id={self.id}, struct_id={self.ioda_structure_id})"
