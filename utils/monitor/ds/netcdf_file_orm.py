from sqlalchemy import (
    Column,
    Integer,
    String,
    Float,
    ForeignKey,
    UniqueConstraint,
    Index,
)
from sqlalchemy.types import JSON
from .db_base import Base


class NetcdfFileDerivedAttributeORM(Base):
    """
    Numeric statistics computed from a specific variable
    within a specific NetCDF file.
    """

    __tablename__ = "netcdf_file_derived_attributes"

    id = Column(Integer, primary_key=True)

    # Anchor to physical file (FileORM)
    file_id = Column(
        Integer,
        ForeignKey("files.id"),
        nullable=False,
        index=True,
    )

    # REQUIRED link to specific variable node
    netcdf_node_id = Column(
        Integer,
        ForeignKey("netcdf_structure_nodes.id"),
        nullable=False,
        index=True,
    )

    # Derived attribute name (min, max, mean, std, valid_count, etc.)
    name = Column(String, nullable=False)

    # Computed numeric attribute value
    value = Column(Float, nullable=False, index=True)

    __table_args__ = (
        UniqueConstraint(
            "file_id",
            "netcdf_node_id",
            "name",
            name="_netcdf_file_node_name_uc",
        ),
        Index(
            "ix_netcdf_file_node_name_lookup",
            "name",
            "netcdf_node_id",
            "value",
        ),
    )


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
