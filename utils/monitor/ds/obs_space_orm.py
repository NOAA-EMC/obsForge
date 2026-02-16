from sqlalchemy import Column, Integer, String

from .db_base import Base


class ObsSpaceORM(Base):
    """
    Global ObsSpace type definition.
    """

    __tablename__ = "obs_spaces"

    id = Column(Integer, primary_key=True)

    # Unique type name (e.g., "amsua", "sst")
    name = Column(String, unique=True, nullable=False)

    # Optional future relationship
    ioda_structure_id = Column(Integer, nullable=True)

    def __repr__(self) -> str:
        return f"<ObsSpaceORM(id={self.id}, name='{self.name}')>"
