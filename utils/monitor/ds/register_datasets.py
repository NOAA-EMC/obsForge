import os
import logging
from typing import List

from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from .db_base import Base
from .obs_space_orm import ObsSpaceORM
from .dataset_orm import DatasetORM, DatasetCycleORM, DatasetFieldORM

from .dataset import Dataset
from .dataset_cycle import DatasetCycle

from pathlib import Path
from .file import File
from .file_orm import FileORM

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


def discover_datasets(data_root: str) -> List[Dataset]:
    """
    Scan data_root and return Dataset domain objects
    """
    if not os.path.exists(data_root):
        raise FileNotFoundError(f"Data root not found: {data_root}")

    dataset_names = set()

    for entry in os.listdir(data_root):
        full_path = os.path.join(data_root, entry)

        if not os.path.isdir(full_path):
            continue

        if "." in entry:
            prefix = entry.split(".")[0]
            dataset_names.add(prefix)

    return [
        Dataset(name=name, root_dir=data_root)
        for name in sorted(dataset_names)
    ]


def read_datasets(datasets: List[Dataset]):
    for ds in datasets:
        cycle_dir = ds.find_first_valid_cycle_dir(ds.root_dir)
        cycle = DatasetCycle.from_directory(ds, cycle_dir)

        logger.info(f"Constructed: {cycle} from {cycle_dir}")

        ds.add_cycle(cycle)

        logger.info(f"Read dataset: {ds.name} (ID={ds.id})")


def persist_datasets(datasets: List[Dataset], db_path: str) -> List[Dataset]:
    engine = create_engine(f"sqlite:///{db_path}")
    Base.metadata.create_all(engine)
    session = Session(engine)

    for ds in datasets:
        ds.to_db(session)
        logger.info(f"Persisted dataset: {ds.name} (ID={ds.id})")

    session.commit()
    session.close()

    return datasets


def register_datasets(data_root: str, db_path: str) -> List[Dataset]:
    datasets = discover_datasets(data_root)
    read_datasets(datasets)
    return persist_datasets(datasets, db_path)
    # return datasets

##########################################################################

# def create_db_session(db_path: str):
    # # Create engine & session
    # engine = create_engine(f"sqlite:///{db_path}")
    # Base.metadata.create_all(engine)
    # session = Session(engine)
    # return session

def old_register_datasets(data_root: str, db_path: str) -> List[Dataset]:
    """
    Scan a data root and register all datasets found.

    Parameters
    ----------
    data_root : str
        Root directory containing dataset cycle directories, e.g.,
        gfs.20260204, gdas.20260201, gcdas.20260201, etc.
    db_path : str
        Path to the SQLite database.

    Returns
    -------
    List[Dataset]
        List of Dataset domain objects.
    """
    if not os.path.exists(data_root):
        raise FileNotFoundError(f"Data root not found: {data_root}")

    all_entries = os.listdir(data_root)
    dataset_names = set()

    # Top-level directories are of the form: gfs.20260204
    for entry in all_entries:
        full_path = os.path.join(data_root, entry)
        if not os.path.isdir(full_path):
            continue
        # Extract dataset prefix before the first dot
        if "." in entry:
            prefix = entry.split(".")[0]
            dataset_names.add(prefix)

    # Create engine & session
    engine = create_engine(f"sqlite:///{db_path}")
    # engine = create_engine(f"sqlite:///{db_path}", echo=True)
    Base.metadata.create_all(engine)
    session = Session(engine)

    datasets = []
    for name in sorted(dataset_names):
        ds = Dataset(name=name, root_dir=data_root)

        cycle_dir = ds.find_first_valid_cycle_dir(data_root)
        cycle = DatasetCycle.from_directory(ds, cycle_dir)
        logger.info(f"Constructed: {cycle} from {cycle_dir}")

        ds.add_cycle(cycle)

        logger.info(f"done...... ")
        import sys
        sys.exit(1)

        '''
        ds.register_cycles()
        ds.register_obs_spaces()
        ds.register_files()

        ds.print_obs_space_files_report()

        ds.sync_ioda_structures(session)
        '''

        ds.to_db(session)

        logger.info(f"Registered dataset: {name} (ID={ds.id})")
        datasets.append(ds)

    session.commit()

    return datasets



def old_register_directory_files(session, directory: str, recursive=False):
    """
    Scan directory for .nc files and register them in DB.
    """

    directory = os.path.realpath(directory)

    if not os.path.isdir(directory):
        raise ValueError(f"{directory} is not a valid directory")

    registered = []

    path_obj = Path(directory)

    files_iter = (
        path_obj.rglob("*.nc")
        if recursive
        else path_obj.glob("*.nc")
    )

    for path in files_iter:
        # logger.info(f"path: {path}")
        if not path.is_file():
            continue

        file_obj = File.from_filesystem(str(path))
        # print("-------from filesystem--------------")
        # print(file_obj)
        # print("---------------------")
        file_obj.get_or_create(session)

        registered.append(file_obj)

    return registered
