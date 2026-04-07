import logging
logger = logging.getLogger(__name__)

from typing import Optional

from sqlalchemy import select, and_
from ds.dataset_orm import (
    FieldORM, DatasetORM, CycleORM, DatasetFileORM
)

from ds.dataset import Dataset
from ds.dataset_cycle import DatasetCycle
from ds.dataset_field import DatasetField

from ds.dataset_file import DatasetFile
from ds.dataset_cycle import DatasetCycle


class DatasetRepository:
    def __init__(self, session):
        self.session = session

    def save_dataset(self, dataset: Dataset):
        existing = self.session.scalar(
            select(DatasetORM).where(DatasetORM.name == dataset.name)
        )

        if existing:
            dataset.id = existing.id
            return existing

        orm = DatasetORM(
            name=dataset.name,
            root_dir=dataset.root_dir
        )

        self.session.add(orm)
        self.session.flush()

        dataset.id = orm.id
        return orm

    def save_cycle(self, cycle):
        # logger.info(f"save_cycle: {cycle}")

        # persist cycle itself
        if cycle.id is None:
            existing = self.session.scalar(
                select(CycleORM).where(
                    and_(
                        CycleORM.dataset_id == cycle.dataset.id,
                        CycleORM.cycle_date == cycle.cycle_date,
                        CycleORM.cycle_hour == cycle.cycle_hour,
                    )
                )
            )

            if existing:
                cycle.id = existing.id
            else:
                orm = CycleORM(
                    dataset_id=cycle.dataset.id,
                    cycle_date=cycle.cycle_date,
                    cycle_hour=cycle.cycle_hour,
                )
                self.session.add(orm)
                self.session.flush()
                cycle.id = orm.id

        self.save_cycle_files(cycle)

        return cycle

    def load_cycle_files(self, cycle):
        if not cycle.dataset.dataset_fields:
            logger.error("Dataset fields must be loaded before loading cycle files")
            # raise ValueError("Dataset fields must be loaded before loading cycle files")
            return

        stmt = (
            select(DatasetFileORM)
            .where(DatasetFileORM.dataset_cycle_id == cycle.id)
        )
        file_orms = self.session.scalars(stmt).all()

        for f_orm in file_orms:
            field_domain = cycle.dataset.find_field_by_id(
                f_orm.dataset_field_id
            )

            if not field_domain:
                continue

            ds_file = DatasetFile.from_orm(
                session=self.session,
                orm=f_orm,
                dataset_field=field_domain,
                dataset_cycle=cycle
            )

            field_domain.add_file(ds_file)
            cycle.add_file(ds_file)

    def load_cycle(self, dataset, cycle_date, cycle_hour):
        if not dataset.dataset_fields:
            self.load_fields(dataset)

        # 1. Check if already in memory
        existing = next(
            (c for c in dataset.dataset_cycles
             if c.cycle_date == cycle_date and c.cycle_hour == cycle_hour),
            None
        )
        if existing:
            return existing

        # 2. Query DB
        stmt = select(CycleORM).where(
            and_(
                CycleORM.dataset_id == dataset.id,
                CycleORM.cycle_date == cycle_date,
                CycleORM.cycle_hour == cycle_hour
            )
        )
        c_orm = self.session.scalar(stmt)

        if not c_orm:
            return None

        # 3. Build domain object
        cycle = DatasetCycle.from_orm(c_orm, dataset)

        # 4. Load files
        self.load_cycle_files(cycle)

        # 5. Register in dataset
        dataset.dataset_cycles.append(cycle)
        dataset.dataset_cycles.sort()

        return cycle

    def get_all_datasets(self):
        stmt = select(DatasetORM).order_by(DatasetORM.name)
        return [
            Dataset.from_db_self(orm)
            for orm in self.session.scalars(stmt).all()
        ]

    def get_dataset_by_id(self, dataset_id):
        orm = self.session.get(DatasetORM, dataset_id)
        return Dataset.from_db_self(orm) if orm else None

    def save_cycle_files(self, cycle):
        for f in cycle.files:
            self.save_dataset_file(f)

    def save_field(self, field):
        field.obs_space.to_db(self.session)

        self.session.flush()

        existing = self.session.scalar(
            select(FieldORM).where(
                and_(
                    FieldORM.dataset_id == field.dataset.id,
                    FieldORM.obs_space_id == field.obs_space.id
                )
            )
        )

        if existing:
            field.id = existing.id
            return existing

        orm = FieldORM(
            dataset_id=field.dataset.id,
            obs_space_id=field.obs_space.id
        )

        self.session.add(orm)
        self.session.flush()

        field.id = orm.id
        return orm

    def load_field(self, dataset, field_id):
        orm = self.session.get(FieldORM, field_id)
        if not orm:
            return None
        return DatasetField.from_orm(orm, dataset)
        # return DatasetField.from_db_self(orm, dataset)

    def load_fields(self, dataset):
        stmt = select(FieldORM).where(FieldORM.dataset_id == dataset.id)
        field_orms = self.session.scalars(stmt).all()

        dataset.dataset_fields = [
            # DatasetField.from_db_self(f_orm, dataset)
            DatasetField.from_orm(f_orm, dataset)
            for f_orm in field_orms
        ]

    def load_field_files(self, field, n: Optional[int] = None):
        stmt = (
            select(DatasetFileORM)
            .join(CycleORM)
            .where(DatasetFileORM.dataset_field_id == field.id)
            .order_by(CycleORM.cycle_date.desc(), CycleORM.cycle_hour.desc())
        )

        if n:
            stmt = stmt.limit(abs(n))

        file_orms = self.session.scalars(stmt).all()

        for f_orm in file_orms:
            cycle_domain = DatasetCycle._from_db_self(
                f_orm.dataset_cycle,
                field.dataset
            )

            ds_file = DatasetFile.from_orm(
                session=self.session,
                orm=f_orm,
                dataset_field=field,
                dataset_cycle=cycle_domain
            )

            field.add_file(ds_file)

    def save_dataset_file(self, ds_file):
        self.save_field(ds_file.dataset_field)
        # self.save_cycle(ds_file.dataset_cycle)
        self.save_file(ds_file.file)

        if ds_file.netcdf_file:
            ds_file.netcdf_file.to_db(self.session)

        existing = self.session.scalar(
            select(DatasetFileORM).where(
                and_(
                    DatasetFileORM.dataset_field_id == ds_file.dataset_field.id,
                    DatasetFileORM.dataset_cycle_id == ds_file.dataset_cycle.id,
                    DatasetFileORM.file_id == ds_file.file.id
                )
            )
        )

        if existing:
            ds_file.id = existing.id
            return existing

        orm = DatasetFileORM(
            dataset_field_id=ds_file.dataset_field.id,
            dataset_cycle_id=ds_file.dataset_cycle.id,
            file_id=ds_file.file.id
        )

        self.session.add(orm)
        self.session.flush()
        ds_file.id = orm.id

        return orm

    def save_file(self, file):
        return file.to_db(self.session)

    '''
    def save_netcdf_file(self, nc_file):
        nc_file.structure.to_db(self.session)
        nc_file.file.to_db(self.session)

        self.session.flush()

        self._save_netcdf_attributes(nc_file)
        self._save_netcdf_derived(nc_file)
    '''


####################################################

    # def save_dataset_file(self, ds_file):
        # self.save_field(ds_file.dataset_field)
        # self.save_cycle(ds_file.dataset_cycle)
        # self.save_file(ds_file.file)
# 
        # if ds_file.netcdf_file:
            # ds_file.netcdf_file.to_db(self.session)
# 
        # return ds_file.to_db(self.session)

    def old_save_cycle(self, cycle):
        # Already persisted?
        if cycle.id is not None:
            existing = self.session.get(CycleORM, cycle.id)
            if existing:
                return existing

        # Check for existing in DB
        existing = self.session.scalar(
            select(CycleORM).where(
                and_(
                    CycleORM.dataset_id == cycle.dataset.id,
                    CycleORM.cycle_date == cycle.cycle_date,
                    CycleORM.cycle_hour == cycle.cycle_hour,
                )
            )
        )

        if existing:
            cycle.id = existing.id
            return existing

        # Create new
        orm = CycleORM(
            dataset_id=cycle.dataset.id,
            cycle_date=cycle.cycle_date,
            cycle_hour=cycle.cycle_hour,
        )

        self.session.add(orm)
        self.session.flush()

        cycle.id = orm.id
        return orm
