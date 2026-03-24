import logging
from typing import Optional
import pandas as pd

from sqlalchemy import select, and_
from sqlalchemy.orm import Session

from .dataset_orm import (
    DatasetFieldORM,
    DatasetCycleORM,
    DatasetFileORM
)

from .netcdf_structure_orm import  NetcdfNodeORM
from .netcdf_file_orm import NetcdfFileDerivedAttributeORM

from .dataset_file import DatasetFile

logger = logging.getLogger(__name__)


class DatasetField:
    """
    Represents a variable of type ObsSpace within a Dataset.
    It has a list of DatasetFile objects (0 or 1 per cycle)
    A DatasetFile object is a link between field, cycle and file
    """
    def __init__(self, dataset: "Dataset", obs_space: "ObsSpace"):
        self.dataset = dataset
        self.obs_space = obs_space
        self.id = None  # set when persisted

        # the files are not persisted here, but in cycles
        self.files: List[DatasetFile] = []

    def __repr__(self) -> str:
        return (
            f"<Field id = {self.id}: "
            f"{self.dataset.name} "
            f"{self.obs_space},\n"
            f"{len(self.files)} files>"
        )

    def add_file(self, f: DatasetFile):
        self.files.append(f)

    @classmethod
    def from_orm(
        cls, 
        session: Session, 
        orm: DatasetFieldORM, 
        dataset: "Dataset", 
        n_files: Optional[int] = None
    ) -> "DatasetField":
        instance = cls.from_orm_self(orm, dataset)
        instance.from_orm_files(session, n=n_files)
        return instance

    @classmethod
    def from_orm_self(cls, orm: DatasetFieldORM, dataset: "Dataset") -> "DatasetField":
        if not orm: return None
        
        from .obs_space import ObsSpace
        obs_space_domain = ObsSpace.from_orm(orm.obs_space)

        instance = cls(dataset=dataset, obs_space=obs_space_domain)
        instance.id = orm.id
        return instance

    def from_orm_files(self, session: Session, n: Optional[int] = None) -> None:
        from .dataset_cycle import DatasetCycle
        stmt = (
            select(DatasetFileORM)
            .join(DatasetCycleORM)
            .where(DatasetFileORM.dataset_field_id == self.id)
            .order_by(DatasetCycleORM.cycle_date.desc(), DatasetCycleORM.cycle_hour.desc())
        )

        if n:
            stmt = stmt.limit(abs(n))

        file_orms = session.scalars(stmt).all()

        for f_orm in file_orms:
            cycle_domain = DatasetCycle._from_db_self(f_orm.dataset_cycle, self.dataset)
            
            ds_file = DatasetFile.from_orm(
                session=session,
                orm=f_orm,
                dataset_field=self,
                dataset_cycle=cycle_domain
            )
            self.add_file(ds_file)

    def to_orm(self) -> DatasetFieldORM:
        return DatasetFieldORM(
            id=self.id,
            dataset_id=self.dataset.id,
            obs_space_id=self.obs_space.id
        )

    def to_db(self, session: Session) -> "DatasetFieldORM":
        """
        Ensure this DatasetField exists in the DB. Returns the ORM object.
        Sets self.id.
        Safe against duplicates in session or DB.
        """

        # logger.info(f"to_db {self}")

        # Already persisted? Return ORM object
        if self.id is not None:
            existing = session.get(DatasetFieldORM, self.id)
            if existing:
                return existing

        # Persist underlying ObsSpace first
        self.obs_space.to_db(session)  # ensures self.obs_space.id is set

        # Flush pending inserts so session knows about existing rows
        session.flush()

        # Check DB + session for existing row
        existing = session.scalar(
            select(DatasetFieldORM).where(
                and_(
                    DatasetFieldORM.dataset_id == self.dataset.id,
                    DatasetFieldORM.obs_space_id == self.obs_space.id
                )
            )
        )

        if existing:
            # Set self.id to avoid duplicate inserts later
            self.id = existing.id
            return existing

        # No existing row → safe to create ORM
        orm = self.to_orm()
        session.add(orm)
        session.flush()  # assign database-generated ID
        self.id = orm.id

        # logger.info(f"done .... to_db {self}")

        return orm

    def get_variable_derived_data(self, session: Session, variable_path: str, metrics: list | None = None):
        """
        Fetch historical derived attributes for this variable 
        and return a Pandas DataFrame
        indexed by timestamp, ready for plotting.

        Parameters
        ----------
        session : Session
            SQLAlchemy session.
        variable_path : str
            Full NetCDF variable path (e.g. "/group/variable").
        metrics : list[str] | None
            Metrics to fetch (e.g. ["mean","std_dev"]).
            If None, all available metrics are returned.

        Returns
        -------
        pandas.DataFrame
            Indexed by timestamp with metric columns.
        """

        # Build WHERE conditions
        conditions = [
            DatasetFileORM.dataset_field_id == self.id,
            NetcdfNodeORM.full_path == variable_path,
        ]

        # Only filter metrics if a list is provided
        if metrics is not None:
            conditions.append(NetcdfFileDerivedAttributeORM.name.in_(metrics))

        stmt = (
            select(
                DatasetCycleORM.cycle_date,
                DatasetCycleORM.cycle_hour,
                NetcdfFileDerivedAttributeORM.name,
                NetcdfFileDerivedAttributeORM.value
            )
            .join(DatasetFileORM, DatasetFileORM.dataset_cycle_id == DatasetCycleORM.id)
            .join(NetcdfFileDerivedAttributeORM, NetcdfFileDerivedAttributeORM.file_id == DatasetFileORM.file_id)
            .join(NetcdfNodeORM, NetcdfNodeORM.id == NetcdfFileDerivedAttributeORM.netcdf_node_id)
            .where(*conditions)
            .order_by(DatasetCycleORM.cycle_date, DatasetCycleORM.cycle_hour)
        )
        # print(stmt.compile(compile_kwargs={"literal_binds": True}))

        results = session.execute(stmt).all()

        if not results:
            return pd.DataFrame()

        # Convert to DataFrame
        df = pd.DataFrame(results, columns=["date", "hour", "metric", "value"])

        # Create timestamp column
        df["ts"] = pd.to_datetime(
            df["date"].astype(str) + " " + df["hour"].astype(str),
            format="%Y-%m-%d %H"
        )

        # Pivot to wide format for plotting
        return df.pivot(index="ts", columns="metric", values="value")
