import os
import re
import logging
from typing import Optional
from sqlalchemy import select
from sqlalchemy.orm import Session

from .obs_space_orm import ObsSpaceORM
from .netcdf_structure import NetcdfStructure

logger = logging.getLogger(__name__)


class ObsSpace:
    """
    Domain object representing an ObsSpace.
    It is a global type definition.
    """

    def __init__(
        self,
        name: str,
        netcdf_structure: NetcdfStructure,
        id: Optional[int] = None,
        netcdf_structure_id: Optional[int] = None,
    ):
        self.name = name
        self.netcdf_structure = netcdf_structure

        self.id = id

        # to be deprecated:
        self.netcdf_structure_id = netcdf_structure_id

    def __repr__(self) -> str:
        return f"ObsSpace name='{self.name}', id={self.id}"
        # return f"ObsSpace(name='{self.name}', id={self.id}, struct_id={self.netcdf_structure_id})"


    def compare(self, other: "ObsSpace") -> bool:
        """
        Compare two ObsSpace objects.

        Returns:
            True  -> same name and same structure
            False -> names differ OR structures differ

        Logs an error if names match but structures differ.
        """

        if not isinstance(other, ObsSpace):
            logger.error(
                f"Cannot compare ObsSpace with object of type {type(other)}"
            )
            return False

        # Different names → not equal, no error
        if self.name != other.name:
            return False

        # Same name, check structure hash
        hash_a = self.netcdf_structure.structure_hash
        hash_b = other.netcdf_structure.structure_hash

        if hash_a != hash_b:
            logger.error(
                f"STRUCTURAL CONFLICT for ObsSpace '{self.name}'\n"
                f"Hash A: {hash_a}\n"
                f"Hash B: {hash_b}"
            )
            return False

        return True

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

        structure = NetcdfStructure.from_file(file_path)
        if structure is None:
            return None

        this_obs_space = cls(name=name, netcdf_structure=structure)
        # logger.debug(f"constructed {this_obs_space} from {file_path}")
        return this_obs_space


    def to_orm(self):
        return ObsSpaceORM(
            name=self.name,
            netcdf_structure_id=self.netcdf_structure.id,
            # netcdf_structure_id=self.netcdf_structure_id,
        )

    def to_db(self, session: Session) -> int:
        """Ensure this ObsSpace exists in the DB. Idempotent."""

        if self.id is not None:
            return self.id

        current_netcdf_structure_orm = self.netcdf_structure.to_db(session)
        current_netcdf_structure_id = current_netcdf_structure_orm.id

        existing = session.execute(
            select(ObsSpaceORM).where(ObsSpaceORM.name == self.name)
        ).scalar_one_or_none()

        if existing:
            # can reuse the compare method of this class here
            if current_netcdf_structure_id != existing.netcdf_structure_id:
                logger.error(
                    f"STRUCTURAL DISCREPANCY DETECTED\n"
                    f"ObsSpace: {self.name}\n"
                    # f"File: {file_path}\n"
                    f"Expected Netcdf Struct ID: {existing.netcdf_structure_id}\n"
                    f"Actual Netcdf Struct ID:   {current_netcdf_structure_id}"
                )

            self.id = existing.id
            return self.id

        orm_obj = self.to_orm()
        session.add(orm_obj)
        # session.commit()
        session.flush()
        self.id = orm_obj.id
        # logger.debug(f"to_db {self}")

        return self.id
