from sqlalchemy import (
    Column, Integer, String, Date, ForeignKey, UniqueConstraint
)
from sqlalchemy.orm import relationship

# Base class for all ORM models
from .db_base import Base  # SQLAlchemy declarative base


class DatasetORM(Base):
    __tablename__ = "datasets"

    id = Column(Integer, primary_key=True)
    name = Column(String, unique=True, nullable=False)
    root_dir = Column(String, nullable=False)

    # One-to-many: cycles and fields
    cycles = relationship("DatasetCycleORM", back_populates="dataset")
    fields = relationship("DatasetFieldORM", back_populates="dataset")


class DatasetCycleORM(Base):
    __tablename__ = "dataset_cycles"

    id = Column(Integer, primary_key=True)
    dataset_id = Column(Integer, ForeignKey("datasets.id"), nullable=False)
    cycle_date = Column(Date, nullable=False)
    cycle_hour = Column(String, nullable=False)

    dataset = relationship("DatasetORM", back_populates="cycles")

    # One-to-many: dataset files
    dataset_files = relationship(
        "DatasetFileORM",
        back_populates="dataset_cycle"
    )

    __table_args__ = (
        UniqueConstraint(
            "dataset_id",
            "cycle_date",
            "cycle_hour",
            name="uq_dataset_cycle"
        ),
    )


class DatasetFieldORM(Base):
    __tablename__ = "dataset_fields"

    id = Column(Integer, primary_key=True)
    dataset_id = Column(Integer, ForeignKey("datasets.id"), nullable=False)
    obs_space_id = Column(Integer, ForeignKey("obs_spaces.id"), nullable=False)

    dataset = relationship("DatasetORM", back_populates="fields")
    dataset_files = relationship(
        "DatasetFileORM",
        back_populates="dataset_field"
    )

    __table_args__ = (
        UniqueConstraint(
            "dataset_id",
            "obs_space_id",
            name="uq_dataset_obs_space"
        ),
    )


class DatasetFileORM(Base):
    __tablename__ = "dataset_files"

    id = Column(Integer, primary_key=True)

    dataset_field_id = Column(
        Integer,
        ForeignKey("dataset_fields.id"),
        nullable=False
    )
    dataset_cycle_id = Column(
        Integer,
        ForeignKey("dataset_cycles.id"),
        nullable=False
    )
    file_id = Column(Integer, ForeignKey("files.id"), nullable=False)

    dataset_field = relationship(
        "DatasetFieldORM",
        back_populates="dataset_files"
    )
    dataset_cycle = relationship(
        "DatasetCycleORM",
        back_populates="dataset_files"
    )
    file = relationship("FileORM")

    __table_args__ = (
        UniqueConstraint(
            "dataset_field_id",
            "dataset_cycle_id",
            name="uq_dataset_cycle_field"
        )
    )
