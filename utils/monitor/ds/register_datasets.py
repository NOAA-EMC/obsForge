import os
import logging
from typing import List

from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from .db_base import Base
from .obs_space_orm import ObsSpaceORM
from .dataset_orm import DatasetORM, DatasetCycleORM, DatasetObsSpaceORM

from .dataset import Dataset

from pathlib import Path
from .file import File
from .file_orm import FileORM

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


# def create_db_session(db_path: str):
    # # Create engine & session
    # engine = create_engine(f"sqlite:///{db_path}")
    # Base.metadata.create_all(engine)
    # session = Session(engine)
    # return session


def register_datasets(data_root: str, db_path: str) -> List[Dataset]:
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
        # ds = Dataset(db_path=db_path, name=name, root_dir=data_root)
        ds = Dataset(name=name, root_dir=data_root)
        ds.register_cycles()
        ds.register_obs_spaces()
        ds.register_files()
        ds.to_db(session)

        logger.info(f"Registered dataset: {name} (ID={ds.id})")
        datasets.append(ds)

    session.commit()

    return datasets



def register_directory_files(session, directory: str, recursive=False):
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
