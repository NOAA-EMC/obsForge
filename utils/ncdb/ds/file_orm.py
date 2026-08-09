from sqlalchemy import (
    Column, Integer, String, BigInteger, DateTime, UniqueConstraint
)

from .db_base import Base


class FileORM(Base):
    __tablename__ = "files"

    id = Column(Integer, primary_key=True)

    # Absolute canonical path (must be unique)
    path = Column(String, nullable=False, unique=True, index=True)

    # File size in bytes
    size = Column(BigInteger, nullable=False)

    # Last modification time
    mtime = Column(DateTime, nullable=False)

    # __table_args__ = (
        # UniqueConstraint("path", name="uq_files_path"),
    # )

    def __repr__(self):
        return (
            f"<FileORM(id={self.id}, "
            f"path='{self.path}', "
            f"size={self.size}, "
            f"mtime={self.mtime})>"
        )
