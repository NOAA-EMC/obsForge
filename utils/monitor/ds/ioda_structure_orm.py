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
from sqlalchemy.types import JSON

from .db_base import Base


# Association table to handle variable-to-dimension mapping and order
# This is part of the "invariant" structure.
ioda_variable_dimensions = Table(
    'ioda_variable_dimensions',
    Base.metadata,
    Column('variable_node_id', Integer, ForeignKey('ioda_structure_nodes.id'), primary_key=True),
    Column('dimension_node_id', Integer, ForeignKey('ioda_structure_nodes.id'), primary_key=True),
    Column('dim_index', Integer, primary_key=True), # 0 for first dim, 1 for second...
)

class IodaStructureORM(Base):
    """
    Represents the invariant 'skeleton' of an IODA file.
    Files with the same variables, groups, and types share one ID.
    """
    __tablename__ = 'ioda_structures'

    id = Column(Integer, primary_key=True)
    structure_hash = Column(String, unique=True, nullable=False, index=True)
    description = Column(String, nullable=True)

    # Relationship to nodes (variables/groups/dimensions)
    nodes = relationship("IodaNodeORM", back_populates="structure", cascade="all, delete-orphan")
    
    # Link back to your existing obs_spaces
    obs_spaces = relationship("ObsSpaceORM", back_populates="ioda_structure")

class IodaNodeORM(Base):
    """
    An entry in the IODA tree. Can be a Group, Variable, or Dimension.
    """
    __tablename__ = 'ioda_structure_nodes'

    id = Column(Integer, primary_key=True)
    structure_id = Column(Integer, ForeignKey('ioda_structures.id'), nullable=False)
    full_path = Column(String, nullable=False) # e.g., 'ObsValue/seaSurfaceTemperature'
    node_type = Column(String, nullable=False) # 'GROUP', 'VARIABLE', 'DIMENSION'
    dtype = Column(String, nullable=True)     # e.g., 'float32', 'int32'
    
    structure = relationship("IodaStructureORM", back_populates="nodes")

    # Self-referential-like many-to-many: Variables linked to their Dimensions
    dimensions = relationship(
        "IodaNodeORM",
        secondary=ioda_variable_dimensions,
        primaryjoin=id == ioda_variable_dimensions.c.variable_node_id,
        secondaryjoin=id == ioda_variable_dimensions.c.dimension_node_id,
        order_by=ioda_variable_dimensions.c.dim_index,
        backref="dimension_for_variables"
    )

    __table_args__ = (UniqueConstraint('structure_id', 'full_path', name='_structure_node_uc'),)


# class IodaAttributeORM(Base):
    # """
    # Handles metadata (attributes) for the structure itself or its nodes.
    # This stores invariant metadata like 'units' or 'standard_name'.
    # """
    # __tablename__ = 'ioda_attributes'
# 
    # id = Column(Integer, primary_key=True)
    # # Polymorphic: can point to the structure itself or a specific node
    # target_type = Column(String, nullable=False) # 'STRUCTURE' or 'NODE'
    # target_id = Column(Integer, nullable=False)
    # 
    # attr_key = Column(String, nullable=False)
    # attr_value = Column(JSON, nullable=False)
# 
    # __table_args__ = (UniqueConstraint('target_type', 'target_id', 'attr_key', name='_ioda_attr_uc'),)


class IodaStructureAttributeORM(Base):
    """
    Blueprint: Defines that a node in this structure HAS an attribute with this name.
    Used for the structural hash.
    """
    __tablename__ = 'ioda_structure_attributes'
    id = Column(Integer, primary_key=True)
    node_id = Column(Integer, ForeignKey('ioda_structure_nodes.id'), nullable=False)
    attr_name = Column(String, nullable=False)
    
    __table_args__ = (UniqueConstraint('node_id', 'attr_name', name='_node_attr_name_uc'),)

class IodaFileAttributeORM(Base):
    """
    Values: Stores the actual data for a specific file's attributes.
    """
    __tablename__ = 'ioda_file_attributes'
    id = Column(Integer, primary_key=True)
    file_id = Column(Integer, ForeignKey('files.id'), nullable=False)
    # Link back to the definition in the structure
    struct_attr_id = Column(Integer, ForeignKey('ioda_structure_attributes.id'), nullable=False)
    attr_value = Column(JSON, nullable=False)
