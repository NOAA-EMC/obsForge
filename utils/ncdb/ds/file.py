import os
from datetime import datetime
from typing import Optional

from sqlalchemy import select
from sqlalchemy.orm import Session

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

    @classmethod
    def from_orm(cls, orm: FileORM) -> "File":
        """Reconstructs the basic File domain object from the database."""
        return cls(
            path=orm.path,
            size=orm.size,
            mtime=orm.mtime,
            id=orm.id
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


    def to_db(self, session: Session) -> "FileORM":
        """
        Ensure this File exists in DB.
        - If it exists: update size/mtime if changed, set self.id, return ORM.
        - If not: insert, assign PK, set self.id, return ORM.
        Idempotent and safe against multiple calls.
        """

        # Check if file already exists by path
        existing = session.scalar(
            select(FileORM).where(FileORM.path == self.path)
        )

        if existing:
            # Update metadata if changed
            if existing.size != self.size or existing.mtime != self.mtime:
                existing.size = self.size
                existing.mtime = self.mtime

            self.id = existing.id
            return existing  # return ORM

        # Not found → create new
        orm_obj = self.to_orm()
        session.add(orm_obj)
        session.flush()  # assign PK
        self.id = orm_obj.id

        return orm_obj
