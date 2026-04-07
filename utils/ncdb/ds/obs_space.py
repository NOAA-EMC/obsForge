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
    ):
        self.name = name
        self.netcdf_structure = netcdf_structure

        self.id = id

    def __repr__(self) -> str:
        # return f"<ObsSpace name='{self.name}', id={self.id}>"
        return (
            f"<ObsSpace {self.name}, "
            f"id={self.id}, "
            f"{self.netcdf_structure}>"
        )

    @classmethod
    def from_orm(cls, orm: ObsSpaceORM) -> "ObsSpace":
        if not orm:
            return None

        # logger.info(f"ObsSpace.from_orm orm.netcdf_structure = {orm.netcdf_structure}")
        # 1. Reconstruct the NetcdfStructure domain object
        # Note: orm.netcdf_structure is the NetcdfStructureORM instance
        structure_domain = NetcdfStructure.from_orm(orm.netcdf_structure)
        # logger.info(f"ObsSpace.from_orm reconstructed = {structure_domain}")

        # 2. Return the assembled ObsSpace domain object
        instance = cls(
            name=orm.name,
            netcdf_structure=structure_domain,
            id=orm.id
        )
        # logger.info(f"ObsSpace.from_orm = {instance}")
        return instance

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

    def to_db(self, session: Session) -> ObsSpaceORM:
        """
        Persist ObsSpace.

        Rules:
            - ObsSpace is uniquely identified by name
            - Structure is fully persisted before ObsSpace
            - If structure hash differs, log error and continue
            - Domain object is fully synced with DB
        """

        self.netcdf_structure.to_db(session)

        stmt = select(ObsSpaceORM).where(ObsSpaceORM.name == self.name)
        existing = session.execute(stmt).scalar_one_or_none()

        if not existing:
            new_row = ObsSpaceORM(
                name=self.name,
                netcdf_structure_id=self.netcdf_structure.id,
            )
            session.add(new_row)
            session.flush()

            self.id = new_row.id
            return new_row

        db_hash = existing.netcdf_structure.structure_hash
        incoming_hash = self.netcdf_structure.structure_hash

        if db_hash != incoming_hash:
            logger.error(
                f"STRUCTURAL CONFLICT for ObsSpace '{self.name}'\n"
                f"DB hash:       {db_hash}\n"
                f"Incoming hash: {incoming_hash}\n"
                f"Using DB structure."
            )

        self.id = existing.id

        return existing
