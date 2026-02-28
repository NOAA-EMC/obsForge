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
# from sqlalchemy.types import JSON

from .db_base import Base


# Association table to handle variable-to-dimension mapping and order
# This is part of the "invariant" structure.
netcdf_variable_dimensions = Table(
    'netcdf_variable_dimensions',
    Base.metadata,
    Column('variable_node_id', Integer, ForeignKey('netcdf_structure_nodes.id'), primary_key=True),
    Column('dimension_node_id', Integer, ForeignKey('netcdf_structure_nodes.id'), primary_key=True),
    Column('dim_index', Integer, primary_key=True), # 0 for first dim, 1 for second...
)

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

class NetcdfNodeORM(Base):
    """
    An entry in the NETCDF tree. Can be a Group, Variable, or Dimension.
    """
    __tablename__ = 'netcdf_structure_nodes'

    id = Column(Integer, primary_key=True)
    structure_id = Column(Integer, ForeignKey('netcdf_structures.id'), nullable=False)
    full_path = Column(String, nullable=False) # e.g., 'ObsValue/seaSurfaceTemperature'
    node_type = Column(String, nullable=False) # 'GROUP', 'VARIABLE', 'DIMENSION'
    dtype = Column(String, nullable=True)     # e.g., 'float32', 'int32'
    
    structure = relationship("NetcdfStructureORM", back_populates="nodes")

    # Self-referential-like many-to-many: Variables linked to their Dimensions
    dimensions = relationship(
        "NetcdfNodeORM",
        secondary=netcdf_variable_dimensions,
        primaryjoin=id == netcdf_variable_dimensions.c.variable_node_id,
        secondaryjoin=id == netcdf_variable_dimensions.c.dimension_node_id,
        order_by=netcdf_variable_dimensions.c.dim_index,
        backref="dimension_for_variables"
    )

    __table_args__ = (UniqueConstraint('structure_id', 'full_path', name='_structure_node_uc'),)



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

'''
class NetcdfFileAttributeORM(Base):
    """
    Values: Stores the actual data for a specific file's attributes.
    """
    __tablename__ = 'netcdf_file_attributes'
    id = Column(Integer, primary_key=True)
    file_id = Column(Integer, ForeignKey('files.id'), nullable=False)
    # Link back to the definition in the structure
    struct_attr_id = Column(Integer, ForeignKey('netcdf_structure_attributes.id'), nullable=False)
    attr_value = Column(JSON, nullable=False)
'''
