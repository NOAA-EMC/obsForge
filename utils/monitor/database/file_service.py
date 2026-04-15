import os
import sqlite3
import logging
from typing import Optional, Iterable, Tuple

from database.db_service import DBDataService

logger = logging.getLogger(__name__)


class FileService(DBDataService):
    """
    Owns the `files` table.

    Responsibility:
      - Persist immutable-ish filesystem facts about files
      - No knowledge of obs_spaces, cycles, or processing state
      - No content inspection
    """

    TABLE_NAME = "files"

    def __init__(self, db_path: str):
        super().__init__(db_path)
        self._ensure_schema()

    # ------------------------------------------------------------------
    # Schema
    # ------------------------------------------------------------------

    def _ensure_schema(self):
        """
        Create the files table if it does not exist.

        This service is the sole owner of this table.
        """
        sql = f"""
        CREATE TABLE IF NOT EXISTS {self.TABLE_NAME} (
            id     INTEGER PRIMARY KEY,
            path   TEXT NOT NULL UNIQUE,
            size   INTEGER NOT NULL,
            mtime  INTEGER NOT NULL
        )
        """
        self.execute(sql)
        self.commit()

    # ------------------------------------------------------------------
    # Write operations
    # ------------------------------------------------------------------

    def upsert_file(
        self,
        path: str,
        size: int,
        mtime: int,
    ) -> int:
        """
        Insert or update a file's filesystem information.

        Returns
        -------
        int
            Row ID of the file.
        """
        sql = f"""
        INSERT INTO {self.TABLE_NAME} (path, size, mtime)
        VALUES (?, ?, ?)
        ON CONFLICT(path) DO UPDATE SET
            size  = excluded.size,
            mtime = excluded.mtime
        """
        row_id = self.execute(sql, (path, size, mtime))
        self.commit()
        return row_id

    def upsert_from_stat(self, path: str) -> Optional[int]:
        """
        Convenience method: stat a file and persist its filesystem info.

        Returns None if file does not exist or is inaccessible.
        """
        try:
            st = os.stat(path)
        except OSError as e:
            logger.debug(f"Cannot stat file {path}: {e}")
            return None

        return self.upsert_file(
            path=path,
            size=st.st_size,
            mtime=int(st.st_mtime),
        )

    # ------------------------------------------------------------------
    # Read operations
    # ------------------------------------------------------------------

    def get_by_path(self, path: str) -> Optional[sqlite3.Row]:
        """Fetch a single file record by path."""
        sql = f"""
        SELECT id, path, size, mtime
        FROM {self.TABLE_NAME}
        WHERE path = ?
        """
        return self.fetch_one(sql, (path,))

    def list_all(self) -> Iterable[sqlite3.Row]:
        """Return all known files."""
        sql = f"""
        SELECT id, path, size, mtime
        FROM {self.TABLE_NAME}
        ORDER BY path
        """
        return self.fetch_all(sql)

    # ------------------------------------------------------------------
    # Utility
    # ------------------------------------------------------------------

    def delete_missing_files(self, existing_paths: Iterable[str]) -> int:
        """
        Remove records for files that no longer exist in the filesystem.

        This is optional, but useful when rescanning a tree.
        """
        placeholders = ",".join("?" for _ in existing_paths)
        sql = f"""
        DELETE FROM {self.TABLE_NAME}
        WHERE path NOT IN ({placeholders})
        """
        count = self.execute(sql, tuple(existing_paths))
        self.commit()
        return count

