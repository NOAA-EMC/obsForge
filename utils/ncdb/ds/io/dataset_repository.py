from sqlalchemy import select, and_
from ds.dataset_orm import (
    FieldORM, DatasetORM, CycleORM, DatasetFileORM
)

from ds.dataset import Dataset
from ds.dataset_cycle import DatasetCycle
from ds.dataset_field import DatasetField

from ds.dataset_file import DatasetFile


class DatasetRepository:
    def __init__(self, session):
        self.session = session

    def load_fields(self, dataset):
        stmt = select(FieldORM).where(FieldORM.dataset_id == dataset.id)
        field_orms = self.session.scalars(stmt).all()

        dataset.dataset_fields = [
            DatasetField.from_db_self(f_orm, dataset)
            for f_orm in field_orms
        ]


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
        cycle = DatasetCycle._from_db_self(c_orm, dataset)

        # 4. Load files
        self.load_cycle_files(cycle)

        # 5. Register in dataset
        dataset.dataset_cycles.append(cycle)
        dataset.dataset_cycles.sort()

        return cycle
