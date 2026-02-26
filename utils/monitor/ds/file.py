import os
from datetime import datetime
from typing import Optional

from sqlalchemy import select
from .file_orm import FileORM


class File:
    def __init__(
        self,
        path: str,
        size: int,
        mtime: datetime,
        id: Optional[int] = None,
    ):
        # enforce absolute canonical path
        self.path = os.path.realpath(path)

        if not os.path.isabs(self.path):
            raise ValueError("File path must be absolute.")

        self.id = id
        self.size = size
        self.mtime = mtime

    @classmethod
    def from_path(cls, path: str) -> "File":
        """
        Build a File domain object directly from filesystem.
        """
        real_path = os.path.realpath(path)

        if not os.path.isabs(real_path):
            raise ValueError("File path must be absolute.")

        stat = os.stat(real_path)

        return cls(
            path=real_path,
            size=stat.st_size,
            mtime=datetime.fromtimestamp(stat.st_mtime),
        )

    def to_orm(self) -> FileORM:
        return FileORM(
            id=self.id,
            path=self.path,
            size=self.size,
            mtime=self.mtime,
        )

    def __repr__(self):
        return (
            f"<File(id={self.id}, "
            f"path='{self.path}', "
            f"size={self.size}, "
            f"mtime={self.mtime})>"
        )


    def to_db(self, session) -> "File":
    # def get_or_create(self, session) -> "File":
        """
        Ensure this file exists in DB.
        If exists → load id and update metadata if needed.
        If not → insert and assign id.
        """

        existing = session.scalar(
            select(FileORM).where(FileORM.path == self.path)
        )

        if existing:
            # Update metadata if file changed
            if (
                existing.size != self.size
                or existing.mtime != self.mtime
            ):
                existing.size = self.size
                existing.mtime = self.mtime

            self.id = existing.id
            return self

        # Not found → create
        orm_obj = self.to_orm()
        session.add(orm_obj)
        session.flush()  # assigns PK

        self.id = orm_obj.id
        return self
