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

    # Link to the NETCDF blueprint
    # We add ForeignKey here so SQLAlchemy knows how to JOIN
    netcdf_structure_id = Column(Integer, ForeignKey('netcdf_structures.id'), nullable=True)


    def __repr__(self) -> str:
        return f"<ObsSpaceORM(id={self.id}, name='{self.name}')>"


from .netcdf_structure_orm import NetcdfStructureORM
ObsSpaceORM.netcdf_structure = relationship("NetcdfStructureORM", back_populates="obs_spaces")
