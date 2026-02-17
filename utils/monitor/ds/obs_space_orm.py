from sqlalchemy import Column, Integer, String, ForeignKey 
from sqlalchemy.orm import relationship 
from .db_base import Base


class ObsSpaceORM(Base):
    """
    Global ObsSpace type definition.
    """

    __tablename__ = "obs_spaces"

    id = Column(Integer, primary_key=True)

    # Unique type name (e.g., "amsua", "sst")
    name = Column(String, unique=True, nullable=False)

    # Link to the IODA blueprint
    # We add ForeignKey here so SQLAlchemy knows how to JOIN
    ioda_structure_id = Column(Integer, ForeignKey('ioda_structures.id'), nullable=True)

    # This allows you to do obs_space.ioda_structure to get the skeleton
    ioda_structure = relationship("IodaStructureORM", back_populates="obs_spaces")

    def __repr__(self) -> str:
        return f"<ObsSpaceORM(id={self.id}, name='{self.name}')>"
