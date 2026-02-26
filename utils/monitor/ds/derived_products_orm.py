from sqlalchemy import Column, Integer, String, Float, ForeignKey, UniqueConstraint
from sqlalchemy.orm import relationship
from .db_base import Base  # Assuming your Base is defined here




class DatasetFileDerivedAttributeORM(Base):
    """
    Stores numeric results (min, max, mean, count) for either 
    specific variables or the entire file.
    """
    __tablename__ = 'dataset_file_derived_attributes'
    
    id = Column(Integer, primary_key=True)
    
    # Anchor to the specific physical file entry
    obs_space_file_id = Column(Integer, 
                               ForeignKey('dataset_obs_space_files.id'), 
                               nullable=False, index=True)
    
    # Reference to a specific variable (e.g., 'ObsValue/seaSurfaceTemperature')
    # NULL = Global file-level attribute (e.g., 'lon__max')
    ioda_node_id = Column(Integer, 
                          ForeignKey('ioda_structure_nodes.id'), 
                          nullable=True, index=True)
    
    # The metric name (e.g., 'min', 'max', 'std', 'mean', 'valid_count')
    name = Column(String, nullable=False)
    
    # The actual numeric result
    value = Column(Float, nullable=False, index=True)

    __table_args__ = (
        UniqueConstraint('obs_space_file_id', 'ioda_node_id', 'name', 
                         name='_derived_attr_uc'),
    )


class DatasetFileDerivedFileORM(Base):
    """
    Stores references to diagnostic files (plots, logs) 
    generated from a specific source file.
    """
    __tablename__ = 'dataset_file_derived_files'
    
    id = Column(Integer, primary_key=True)
    
    obs_space_file_id = Column(Integer, 
                               ForeignKey('dataset_obs_space_files.id'), 
                               nullable=False, index=True)
    
    # Context (e.g., an SST histogram vs. a global coverage plot)
    ioda_node_id = Column(Integer, 
                          ForeignKey('ioda_structure_nodes.id'), 
                          nullable=True)
    
    # Product name (e.g., 'coverage_plot', 'reformatted_nc', 'log')
    name = Column(String, nullable=False)
    
    # Link to the central files table
    file_id = Column(Integer, ForeignKey('files.id'), nullable=False)

    __table_args__ = (
        UniqueConstraint('obs_space_file_id', 'ioda_node_id', 'name', 
                         name='_derived_file_uc'),
    )
