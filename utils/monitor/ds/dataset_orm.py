from sqlalchemy import (
    Column, Integer, String, Date, ForeignKey, UniqueConstraint
)
from sqlalchemy.orm import declarative_base, relationship

# Base class for all ORM models
from .db_base import Base  # SQLAlchemy declarative base



class DatasetORM(Base):
    __tablename__ = "datasets"

    id = Column(Integer, primary_key=True)
    name = Column(String, unique=True, nullable=False)
    root_dir = Column(String, nullable=False)

    # One-to-many: cycles and obs_spaces
    cycles = relationship("DatasetCycleORM", back_populates="dataset")
    obs_spaces = relationship("DatasetObsSpaceORM", back_populates="dataset")


class DatasetCycleORM(Base):
    __tablename__ = "dataset_cycles"

    id = Column(Integer, primary_key=True)
    dataset_id = Column(Integer, ForeignKey("datasets.id"), nullable=False)
    cycle_date = Column(Date, nullable=False)
    cycle_hour = Column(String, nullable=False)

    dataset = relationship("DatasetORM", back_populates="cycles")

    # One-to-many: obs_space_files
    obs_space_files = relationship(
        "DatasetObsSpaceFileORM", 
        back_populates="dataset_cycle"
    )


# class ObsSpaceORM(Base):
    # __tablename__ = "obs_spaces"
# 
    # id = Column(Integer, primary_key=True)
    # name = Column(String, unique=True, nullable=False)


class DatasetObsSpaceORM(Base):
    __tablename__ = "dataset_obs_spaces"

    id = Column(Integer, primary_key=True)
    dataset_id = Column(Integer, ForeignKey("datasets.id"), nullable=False)
    obs_space_id = Column(Integer, ForeignKey("obs_spaces.id"), nullable=False)

    dataset = relationship("DatasetORM", back_populates="obs_spaces")
    obs_space_files = relationship("DatasetObsSpaceFileORM", back_populates="dataset_obs_space")


# class FileORM(Base):
    # __tablename__ = "files"
# 
    # id = Column(Integer, primary_key=True)
    # path = Column(String, unique=True, nullable=False)
    # size = Column(Integer, nullable=False)
    # mtime = Column(Integer, nullable=False)  # store as timestamp for simplicity


class DatasetObsSpaceFileORM(Base):
    __tablename__ = "dataset_obs_space_files"

    id = Column(Integer, primary_key=True)

    dataset_obs_space_id = Column(Integer, ForeignKey("dataset_obs_spaces.id"), nullable=False)
    dataset_cycle_id = Column(Integer, ForeignKey("dataset_cycles.id"), nullable=False)
    file_id = Column(Integer, ForeignKey("files.id"), nullable=False)

    dataset_obs_space = relationship("DatasetObsSpaceORM", back_populates="obs_space_files")
    dataset_cycle = relationship("DatasetCycleORM", back_populates="obs_space_files")
    file = relationship("FileORM")

    __table_args__ = (
        UniqueConstraint(
            "dataset_obs_space_id",
            "dataset_cycle_id",
            "file_id",
            name="uq_dataset_obs_space_file"
        ),
    )

