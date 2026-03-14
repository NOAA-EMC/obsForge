import logging
from typing import Optional

from sqlalchemy import select, and_
from sqlalchemy.orm import Session

from .dataset_orm import DatasetFieldORM
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


'''
def get_history_dataframe(self, session: Session, variable_path: str, metrics: list = None):
    """
    Fetches the historical stats for this specific field and 
    returns a Pandas DataFrame ready for plotting.
    """
    import pandas as pd
    from .dataset_orm import DatasetFileORM, DatasetCycleORM
    from .netcdf_file_orm import NetcdfFileDerivedAttributeORM, NetcdfNodeORM

    if metrics is None:
        metrics = ["mean", "std_dev"]

    # One targeted query for this field's specific variable
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
        .where(
            DatasetFileORM.dataset_field_id == self.id,
            NetcdfNodeORM.full_path == variable_path,
            NetcdfFileDerivedAttributeORM.name.in_(metrics)
        )
        .order_by(DatasetCycleORM.cycle_date, DatasetCycleORM.cycle_hour)
    )

    results = session.execute(stmt).all()
    
    if not results:
        return pd.DataFrame()

    # Convert to DataFrame
    df = pd.DataFrame(results, columns=['date', 'hour', 'metric', 'value'])
    
    # Create a proper datetime index for gap-aware plotting
    df['ts'] = pd.to_datetime(df['date'].astype(str) + ' ' + df['hour'], format='%Y-%m-%d %H')
    
    # Pivot so columns are 'mean', 'std_dev', etc.
    return df.pivot(index='ts', columns='metric', values='value')



for field in dataset.fields:
    # 1. Get the data for the specific variable (e.g., Temperature)
    hist_df = field.get_history_dataframe(session, "/ObsValue/temperature")
    
    if hist_df.empty:
        continue

    # 2. Plotting (The "Mean + Band" look)
    import matplotlib.pyplot as plt
    
    plt.figure(figsize=(10, 5))
    
    # The 'asfreq' call handles the gaps by inserting NaNs where cycles are missing
    plot_df = hist_df.asfreq('6H') 
    
    plt.plot(plot_df.index, plot_df['mean'], label='Mean', color='blue')
    
    # The Standard Deviation Band
    plt.fill_between(
        plot_df.index, 
        plot_df['mean'] - plot_df['std_dev'], 
        plot_df['mean'] + plot_df['std_dev'], 
        color='blue', alpha=0.2, label='1$\sigma$ Band'
    )

    plt.title(f"History for {field.obs_space.name}")
    plt.savefig(f"plots/{field.obs_space.name}_history.png")
    plt.close()
'''
