# import logging
# from typing import Optional
import pandas as pd

from sqlalchemy import select, and_
from sqlalchemy.orm import Session

from ds.dataset_orm import (
    FieldORM,
    CycleORM,
    DatasetFileORM
)

from ds.netcdf_structure_orm import  NetcdfNodeORM
from ds.netcdf_file_orm import NetcdfFileDerivedAttributeORM

# from ds.dataset_file import DatasetFile
from ds.field import Field


class FieldDataService:
    def __init__(self, session):
        self.session = session

    def get_variable_derived_data(self, field: Field, variable_path: str, metrics: list | None = None):
        """
        Fetch historical derived attributes for this variable 
        and return a Pandas DataFrame
        indexed by timestamp, ready for plotting.

        Parameters
        ----------
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
            DatasetFileORM.dataset_field_id == field.id,
            NetcdfNodeORM.full_path == variable_path,
        ]

        # Only filter metrics if a list is provided
        if metrics is not None:
            conditions.append(NetcdfFileDerivedAttributeORM.name.in_(metrics))

        stmt = (
            select(
                CycleORM.cycle_date,
                CycleORM.cycle_hour,
                NetcdfFileDerivedAttributeORM.name,
                NetcdfFileDerivedAttributeORM.value
            )
            .join(DatasetFileORM, DatasetFileORM.dataset_cycle_id == CycleORM.id)
            .join(NetcdfFileDerivedAttributeORM, NetcdfFileDerivedAttributeORM.file_id == DatasetFileORM.file_id)
            .join(NetcdfNodeORM, NetcdfNodeORM.id == NetcdfFileDerivedAttributeORM.netcdf_node_id)
            .where(*conditions)
            .order_by(CycleORM.cycle_date, CycleORM.cycle_hour)
        )
        # print(stmt.compile(compile_kwargs={"literal_binds": True}))

        results = self.session.execute(stmt).all()

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
