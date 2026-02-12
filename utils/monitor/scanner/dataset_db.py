import logging
from typing import List, Optional, Tuple

from database.db_service import DBDataService
# from .db_service import DBDataService


class DatasetService(DBDataService):
    """
    Persistence layer for datasets and their cycles.

    Owns:
        - datasets
        - dataset_cycles

    Responsible for:
        - Table creation
        - Idempotent inserts
        - Cycle queries
    """

    # ------------------------------------------------------------------
    # Initialization
    # ------------------------------------------------------------------

    def __init__(self, db_path: str):
        super().__init__(db_path)
        self._create_tables()

    # ------------------------------------------------------------------
    # Schema Definition (Owned by this Service)
    # ------------------------------------------------------------------

    def _create_tables(self) -> None:
        """
        Create tables owned by this service.
        """

        # Table: datasets
        self.execute(
            """
            CREATE TABLE IF NOT EXISTS datasets (
                id     INTEGER PRIMARY KEY,
                name   TEXT NOT NULL UNIQUE
            )
            """
        )

        # Table: dataset_cycles
        self.execute(
            """
            CREATE TABLE IF NOT EXISTS dataset_cycles (
                id            INTEGER PRIMARY KEY,
                dataset_id   INTEGER NOT NULL,
                cycle_date    TEXT NOT NULL,
                cycle_hour    TEXT NOT NULL,
                UNIQUE(dataset_id, cycle_date, cycle_hour),
                FOREIGN KEY(dataset_id)
                    REFERENCES datasets(id)
            )
            """
        )

        # Optional but recommended index for performance
        self.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_dataset_cycles_rt
            ON dataset_cycles(dataset_id)
            """
        )

        self.commit()

    # ------------------------------------------------------------------
    # Dataset Operations
    # ------------------------------------------------------------------

    def ensure_dataset(self, name: str) -> int:
        """
        Ensure a dataset exists.
        Returns its ID.
        """

        row = self.fetch_one(
            "SELECT id FROM datasets WHERE name = ?",
            (name,)
        )

        if row:
            return row["id"]

        new_id = self.execute(
            "INSERT INTO datasets (name) VALUES (?)",
            (name,)
        )

        self.commit()

        logging.info(f"Inserted dataset: {name}")

        return new_id

    # ------------------------------------------------------------------
    # Cycle Operations
    # ------------------------------------------------------------------

    def ensure_cycle(
        self,
        dataset_id: int,
        cycle_date: str,
        cycle_hour: str
    ) -> None:
        """
        Ensure a cycle exists for a dataset.

        Relies on UNIQUE constraint for idempotency.
        """

        self.execute(
            """
            INSERT OR IGNORE INTO dataset_cycles
                (dataset_id, cycle_date, cycle_hour)
            VALUES (?, ?, ?)
            """,
            (dataset_id, cycle_date, cycle_hour)
        )

        self.commit()

    # ------------------------------------------------------------------
    # Query Methods
    # ------------------------------------------------------------------

    def get_dataset_id(self, name: str) -> Optional[int]:
        row = self.fetch_one(
            "SELECT id FROM datasets WHERE name = ?",
            (name,)
        )
        return row["id"] if row else None

    def list_datasets(self) -> List[str]:
        rows = self.fetch_all(
            "SELECT name FROM datasets ORDER BY name"
        )
        return [r["name"] for r in rows]

    def list_cycles(
        self,
        dataset_id: int
    ) -> List[Tuple[str, str]]:
        rows = self.fetch_all(
            """
            SELECT cycle_date, cycle_hour
            FROM dataset_cycles
            WHERE dataset_id = ?
            ORDER BY cycle_date, cycle_hour
            """,
            (dataset_id,)
        )

        return [(r["cycle_date"], r["cycle_hour"]) for r in rows]

    def get_latest_cycle(
        self,
        dataset_id: int
    ) -> Optional[Tuple[str, str]]:
        row = self.fetch_one(
            """
            SELECT cycle_date, cycle_hour
            FROM dataset_cycles
            WHERE dataset_id = ?
            ORDER BY cycle_date DESC, cycle_hour DESC
            LIMIT 1
            """,
            (dataset_id,)
        )

        if not row:
            return None

        return (row["cycle_date"], row["cycle_hour"])

    def get_first_cycle(
        self,
        dataset_id: int
    ) -> Optional[Tuple[str, str]]:
        row = self.fetch_one(
            """
            SELECT cycle_date, cycle_hour
            FROM dataset_cycles
            WHERE dataset_id = ?
            ORDER BY cycle_date ASC, cycle_hour ASC
            LIMIT 1
            """,
            (dataset_id,)
        )

        if not row:
            return None

        return (row["cycle_date"], row["cycle_hour"])

