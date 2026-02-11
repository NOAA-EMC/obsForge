import json
import hashlib
import logging
import sqlite3
from typing import Optional

from database.db_service import DBDataService
from processing.ioda_reader import IodaStructure, IodaNumpyEncoder

logger = logging.getLogger(__name__)


class IodaStructureService(DBDataService):
    """
    Owns the `ioda_structure` table.

    Responsibility:
      - Persist IODA structure schemas
      - Deduplicate identical structures
      - Return IodaStructure domain objects
    """

    TABLE_NAME = "ioda_structure"

    def __init__(self, db_path: str):
        super().__init__(db_path)
        self._ensure_schema()

    # ------------------------------------------------------------------
    # Schema
    # ------------------------------------------------------------------

    def _ensure_schema(self):
        sql = f"""
        CREATE TABLE IF NOT EXISTS {self.TABLE_NAME} (
            id     INTEGER PRIMARY KEY,
            schema TEXT NOT NULL,
            hash   TEXT NOT NULL UNIQUE
        )
        """
        self.execute(sql)
        self.commit()

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _canonical_json(self, structure: IodaStructure) -> str:
        """
        Produce a canonical JSON representation suitable for hashing.
        """
        return json.dumps(
            structure.as_dict(),
            sort_keys=True,
            separators=(",", ":"),
            cls=IodaNumpyEncoder,
        )

    def _compute_hash(self, canonical_json: str) -> str:
        return hashlib.sha256(canonical_json.encode("utf-8")).hexdigest()

    # ------------------------------------------------------------------
    # Write operations
    # ------------------------------------------------------------------

    def get_or_create(self, structure: IodaStructure) -> int:
        """
        Store the structure if it does not already exist.

        Returns
        -------
        int
            ioda_structure.id
        """
        canonical = self._canonical_json(structure)
        h = self._compute_hash(canonical)

        # Try to fetch existing
        row = self.fetch_one(
            f"SELECT id FROM {self.TABLE_NAME} WHERE hash = ?",
            (h,),
        )
        if row:
            return row["id"]

        # Insert new
        row_id = self.execute(
            f"""
            INSERT INTO {self.TABLE_NAME} (schema, hash)
            VALUES (?, ?)
            """,
            (canonical, h),
        )
        self.commit()
        return row_id

    # ------------------------------------------------------------------
    # Read operations
    # ------------------------------------------------------------------

    def get_by_id(self, structure_id: int) -> Optional[IodaStructure]:
        sql = f"""
        SELECT schema
        FROM {self.TABLE_NAME}
        WHERE id = ?
        """
        row = self.fetch_one(sql, (structure_id,))
        if not row:
            return None

        return IodaStructure.from_db(row["schema"])

    def get_by_hash(self, hash_value: str) -> Optional[IodaStructure]:
        sql = f"""
        SELECT schema
        FROM {self.TABLE_NAME}
        WHERE hash = ?
        """
        row = self.fetch_one(sql, (hash_value,))
        if not row:
            return None

        return IodaStructure.from_db(row["schema"])

