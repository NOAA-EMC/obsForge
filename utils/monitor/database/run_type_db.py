import logging
from typing import List, Optional, Dict, Any, Tuple

from .db_service import DBDataService

logger = logging.getLogger(__name__)


class RunTypeService(DBDataService):
    """
    Data access for the run_types table.

    Columns:
        id    INTEGER PRIMARY KEY
        name  TEXT UNIQUE
    """

    TABLE_NAME = "run_types"

    def __init__(self, db_path: str):
        super().__init__(db_path)
        self._create_table()

    def _create_table(self):
        """Ensure the run_types table exists."""
        sql = f"""
        CREATE TABLE IF NOT EXISTS {self.TABLE_NAME} (
            id   INTEGER PRIMARY KEY,
            name TEXT UNIQUE NOT NULL
        )
        """
        self.execute(sql)
        self.commit()
        logger.debug(f"{self.TABLE_NAME} table ensured.")

    # -----------------------------
    # CRUD operations
    # -----------------------------

    def add(self, name: str) -> int:
        """Add a new run type. Returns the inserted row ID."""
        sql = f"INSERT OR IGNORE INTO {self.TABLE_NAME} (name) VALUES (?)"
        run_type_id = self.execute(sql, (name,))
        self.commit()
        return run_type_id

    def get_by_name(self, name: str) -> Optional[Dict[str, Any]]:
        """Fetch a run type by name."""
        sql = f"SELECT * FROM {self.TABLE_NAME} WHERE name = ?"
        row = self.fetch_one(sql, (name,))
        return dict(row) if row else None

    def get_by_id(self, run_type_id: int) -> Optional[Dict[str, Any]]:
        """Fetch a run type by ID."""
        sql = f"SELECT * FROM {self.TABLE_NAME} WHERE id = ?"
        row = self.fetch_one(sql, (run_type_id,))
        return dict(row) if row else None

    def list_all(self) -> List[Dict[str, Any]]:
        """Return all run types."""
        sql = f"SELECT * FROM {self.TABLE_NAME} ORDER BY id"
        rows = self.fetch_all(sql)
        return [dict(r) for r in rows]

    def delete_by_name(self, name: str) -> int:
        """Delete a run type by name. Returns number of rows deleted."""
        sql = f"DELETE FROM {self.TABLE_NAME} WHERE name = ?"
        count = self.execute(sql, (name,))
        self.commit()
        return count

    def delete_by_id(self, run_type_id: int) -> int:
        """Delete a run type by ID. Returns number of rows deleted."""
        sql = f"DELETE FROM {self.TABLE_NAME} WHERE id = ?"
        count = self.execute(sql, (run_type_id,))
        self.commit()
        return count

