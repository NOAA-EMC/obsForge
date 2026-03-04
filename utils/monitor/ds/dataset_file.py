import logging
from typing import Optional, Dict

from sqlalchemy import select, and_
from sqlalchemy.orm import Session

from .dataset_orm import DatasetFileORM

from .netcdf_file import NetcdfFile
from .file import File

logger = logging.getLogger(__name__)


class DatasetFile:
    def __init__(
        self,
        file: "File",
        dataset_field: "DatasetField",
        dataset_cycle: "DatasetCycle",
        id: Optional[int] = None
    ):
        self.dataset_field = dataset_field
        self.dataset_cycle = dataset_cycle
        self.file = file
        self.id = id

        self.netcdf_file: Optional[NetcdfFile] = None
        try:
            structure = None
            if self.dataset_field.obs_space:
                structure = self.dataset_field.obs_space.netcdf_structure

            self.netcdf_file = NetcdfFile(
                file=self.file,
                structure=structure,
            )
        except Exception as e:
            logger.error(
                f"Failed to initialize NetcdfFile for {self.file.path}: {e}"
            )
            self.netcdf_file = None

    def __repr__(self) -> str:
        return (
            f"<DatasetFile(id={self.id}, "
            "\n"
            # f"obs_space={self.dataset_field.obs_space.name}, "
            f"{self.dataset_field}, "
            "\n"
            # f"obs_space={self.dataset_field.obs_space}, "
            # f"cycle={self.dataset_cycle.cycle_date} {self.dataset_cycle.cycle_hour}, "
            f"{self.dataset_cycle}, "
            "\n"
            f"file={self.file.path})>"
        )

    def compute_attributes(self) -> None:
        """
        Read attributes and compute derived attributes in memory.
        """
        # logger.info(f"compute_attributes for {self.file.path}")

        if not self.netcdf_file:
            logger.error(f"Cannot open file {self.file.path} (NetcdfFile not initialized)")
            return

        try:
            self.netcdf_file.read_attributes()
            self.netcdf_file.compute_derived_attributes()
        except Exception as e:
            logger.error(f"compute_attributes failed for {self.file.path}: {e}")


    def to_orm(self) -> "DatasetFileORM":
        return DatasetFileORM(
            id=self.id,
            dataset_field_id=self.dataset_field.id,
            dataset_cycle_id=self.dataset_cycle.id,
            file_id=self.file.id
        )


    def to_db(self, session: Session) -> "DatasetFileORM":
        """
        Ensure this DatasetFile exists in the DB. Returns the ORM object.
        Sets self.id.
        """
        # Already persisted?
        if self.id is not None:
            existing = session.get(DatasetFileORM, self.id)
            if existing:
                return existing

        # logger.info(f"to_db {self}")

        # Persist underlying DatasetField
        if self.dataset_field.id is None:
            self.dataset_field.to_db(session)

        # Persist underlying DatasetCycle
        if self.dataset_cycle.id is None:
            self.dataset_cycle.to_db(session)

        # Persist the physical file
        if self.file.id is None:
            self.file.to_db(session)

        # Persist NetCDF file, structure, attributes, derived attributes
        if self.netcdf_file:
            try:
                self.netcdf_file.to_db(session)
            except Exception as e:
                logger.error(
                    f"Failed to persist NetCDF data for {self.file.path}: {e}"
                )

        # Ensure session sees all IDs
        session.flush()

        # Check if a row already exists
        existing = session.scalar(
            select(DatasetFileORM).where(
                and_(
                    DatasetFileORM.dataset_field_id == self.dataset_field.id,
                    DatasetFileORM.dataset_cycle_id == self.dataset_cycle.id,
                    DatasetFileORM.file_id == self.file.id
                )
            )
        )

        if existing:
            self.id = existing.id
            return existing

        # Create ORM row
        orm = DatasetFileORM(
            dataset_field_id=self.dataset_field.id,
            dataset_cycle_id=self.dataset_cycle.id,
            file_id=self.file.id
        )
        session.add(orm)
        session.flush()
        self.id = orm.id

        # logger.info(f"done .... to_db {self}")
        return orm

    def plot_variable(self, plot_path):
        from processing.plotting.plot_generator import PlotGenerator

        nc_file = self.netcdf_file
        variables = nc_file.structure.list_variables("/ObsValue")
        logger.info(f"Plotting {variables} in {self}")

        variable_name = variables[0]
        if not variable_name:
            return

        variable_units = "units"

        # values = nc_file.get_variable(f"/ObsValue/{variable_name}")
        values = nc_file.get_variable(f"{variable_name}")
        lons   = nc_file.get_variable("/MetaData/longitude")
        lats   = nc_file.get_variable("/MetaData/latitude")

        if values is None or lons is None or lats is None:
            logger.warning(f"Missing data: plot not generated for {df.file.path}")
            return

        # plot_output_dir needs to go
        plot_output_dir = ""
        plotter = PlotGenerator(plot_output_dir)
        plotter.generate_surface_map(
            plot_path,
            "dataset name",
            "obs_space name",
            lats,
            lons,
            values,
            variable_name,      # data["var_name"],
            variable_units      # data["units"]
        )   

        # if not os.path.exists(plot_path):
            # logger.error(f"Expected plot not created: data = {data_file_path}, plot ={plot_path}")
