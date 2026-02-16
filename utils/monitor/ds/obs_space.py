from typing import Optional
from sqlalchemy import Column, Integer, String, ForeignKey
from sqlalchemy.orm import relationship
from sqlalchemy import select
from sqlalchemy.orm import Session

from .db_base import Base
from .obs_space_orm import ObsSpaceORM


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

    def to_orm(self):
        return ObsSpaceORM(
            name=self.name,
            ioda_structure_id=self.ioda_structure_id,
        )

    # --------------------------------------------------------
    # Persistence
    # --------------------------------------------------------

    def to_db(self, session: Session) -> int:
        """
        Ensure this ObsSpace exists in the DB.
        Idempotent.
        """

        if self.id is not None:
            return self.id

        existing = session.execute(
            select(ObsSpaceORM).where(
                ObsSpaceORM.name == self.name
            )
        ).scalar_one_or_none()

        if existing:
            self.id = existing.id
            self.ioda_structure_id = existing.ioda_structure_id
            return self.id

        # orm_obj = ObsSpaceORM(
            # name=self.name,
            # ioda_structure_id=self.ioda_structure_id,
        # )

        orm_obj = self.to_orm()
        session.add(orm_obj)
        session.commit()
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
        return f"ObsSpace(name='{self.name}', id={self.id})"
