import logging
from typing import Optional

from .dataset_orm import DatasetFileORM
from sqlalchemy import select


logger = logging.getLogger(__name__)


class DatasetFile:
    def __init__(
        self,
        dataset_field: "DatasetField",
        dataset_cycle: "DatasetCycle",
        file: "File",
        id: Optional[int] = None
    ):
        self.id = id
        self.dataset_field = dataset_field
        self.dataset_cycle = dataset_cycle
        self.file = file

    def __repr__(self):
        return (
            f"<DatasetFile(id={self.id}, "
            f"obs_space={self.dataset_field.obs_space.name}, "
            f"cycle={self.dataset_cycle.cycle_date} {self.dataset_cycle.cycle_hour}, "
            f"file={self.file.path})>"
        )

    def to_orm(self) -> "DatasetFileORM":
        """Convert to ORM object for persistence."""
        return DatasetFileORM(
            id=self.id,
            dataset_field_id=self.dataset_field.id,
            dataset_cycle_id=self.dataset_cycle.id,
            file_id=self.file.id
        )

    def to_db(self, session: "Session") -> None:
        """Persist this DatasetFile entry."""
        # Ensure File is persisted
        if self.file.id is None:
            self.file.to_db(session)

        # Ensure DatasetField is persisted
        if self.dataset_field.id is None:
            self.dataset_field.to_db(session)

        # Ensure DatasetCycle is persisted
        if self.dataset_cycle.id is None:
            self.dataset_cycle.to_db(session)

        # Check if the entry already exists
        exists = session.scalar(
            select(DatasetFileORM).where(
                (DatasetFileORM.dataset_field_id == self.dataset_field.id) &
                (DatasetFileORM.dataset_cycle_id == self.dataset_cycle.id) &
                (DatasetFileORM.file_id == self.file.id)
            )
        )
        if exists:
            self.id = exists.id
            return

        # Persist
        orm_obj = self.to_orm()
        session.add(orm_obj)
        session.flush()
        self.id = orm_obj.id


'''
class DatasetFile:

    def compute_attributes(self):
        import numpy as np
        # Open your ioda handle here (h5py, etc.)
        results = []
        
        # Pseudo-logic: Iterate through nodes in the file's structure
        for node in self.structure.nodes:
            data = self.read_variable(node.group_name, node.name)
            
            # Mask fill values and NaNs
            mask = (data < 1e15) & (~np.isnan(data))
            valid = data[mask]
            nobs = len(valid)
            
            stats = {'nobs': float(nobs), 'nfill': float(len(data) - nobs)}
            
            if nobs > 0:
                stats.update({
                    'min':  float(np.min(valid)),
                    'max':  float(np.max(valid)),
                    'mean': float(np.mean(valid)),
                    'std':  float(np.std(valid)),
                    'med':  float(np.median(valid))
                })
            
            results.append({'node_id': node.id, 'metrics': stats})
        return results

    def to_db(self, session, results):
        from sqlalchemy.dialects.sqlite import insert
        
        for item in results:
            node_id = item['node_id']
            for name, val in item['metrics'].items():
                stmt = insert(DatasetFileDerivedAttributeORM).values(
                    obs_space_file_id=self.id,
                    ioda_node_id=node_id,
                    name=name,
                    value=val
                ).on_conflict_do_update(
                    index_elements=['obs_space_file_id', 'ioda_node_id', 'name'],
                    set_={'value': val}
                )
                session.execute(stmt)
'''
