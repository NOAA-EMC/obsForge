from sqlalchemy import (
    Column, 
    Integer, 
    String, 
    Boolean, 
    ForeignKey, 
    Table, 
    UniqueConstraint
)
from sqlalchemy.orm import relationship
# from sqlalchemy.dialects.postgresql import JSONB

from .db_base import Base


class NetcdfVariableDimensionORM(Base):
    """
    Association object linking a VARIABLE node to a DIMENSION node,
    preserving dimension order via dim_index.
    """

    __tablename__ = "netcdf_variable_dimensions"

    variable_node_id = Column(
        Integer,
        ForeignKey("netcdf_structure_nodes.id"),
        primary_key=True,
    )

    dimension_node_id = Column(
        Integer,
        ForeignKey("netcdf_structure_nodes.id"),
        primary_key=True,
    )

    dim_index = Column(
        Integer,
        nullable=False,
        primary_key=True,
    )

    # Relationship to dimension node
    dimension = relationship(
        "NetcdfNodeORM",
        foreign_keys=[dimension_node_id],
    )

    # Optional backref to variable (defined from variable side below)


class NetcdfStructureORM(Base):
    """
    Represents the invariant 'skeleton' of an NETCDF file.
    Files with the same variables, groups, and types share one ID.
    """
    __tablename__ = 'netcdf_structures'

    id = Column(Integer, primary_key=True)
    structure_hash = Column(String, unique=True, nullable=False, index=True)
    description = Column(String, nullable=True)

    # Relationship to nodes (variables/groups/dimensions)
    nodes = relationship("NetcdfNodeORM", back_populates="structure", cascade="all, delete-orphan")
    
    # Link back to your existing obs_spaces
    obs_spaces = relationship("ObsSpaceORM", back_populates="netcdf_structure")

    def __repr__(self) -> str:
        return (
            f"<NetcdfStructureORM:"
            f"id = {self.id}, "
            f"hash = {self.structure_hash}>"
        )

class NetcdfNodeORM(Base):

    __tablename__ = "netcdf_structure_nodes"

    id = Column(Integer, primary_key=True)
    structure_id = Column(Integer, ForeignKey("netcdf_structures.id"), nullable=False)
    full_path = Column(String, nullable=False)
    node_type = Column(String, nullable=False)
    dtype = Column(String, nullable=True)

    structure = relationship("NetcdfStructureORM", back_populates="nodes")

    # --------------------------------------------------
    # Ordered association objects (VARIABLE → DIMENSION)
    # --------------------------------------------------
    variable_dimensions = relationship(
        "NetcdfVariableDimensionORM",
        foreign_keys=[NetcdfVariableDimensionORM.variable_node_id],
        cascade="all, delete-orphan",
        order_by=NetcdfVariableDimensionORM.dim_index,
        backref="variable",
    )

    attributes = relationship(
        "NetcdfStructureAttributeORM",
        cascade="all, delete-orphan",
        backref="node",
    )

    __table_args__ = (
        UniqueConstraint(
            "structure_id",
            "full_path",
            "node_type",
            name="uq_structure_path_type"
        ),
    )


class NetcdfStructureAttributeORM(Base):
    """
    Blueprint: Defines that a node in this structure HAS an attribute with this name.
    Used for the structural hash.
    """
    __tablename__ = 'netcdf_structure_attributes'
    id = Column(Integer, primary_key=True)
    node_id = Column(Integer, ForeignKey('netcdf_structure_nodes.id'), nullable=False)
    attr_name = Column(String, nullable=False)
    
    __table_args__ = (UniqueConstraint('node_id', 'attr_name', name='_node_attr_name_uc'),)
