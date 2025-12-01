import os
import sqlite3
import hashlib
from datetime import datetime


class MonitorDB:
    """
    Database layer for storing task metadata, task runs, obs spaces,
    obs space collections, and detailed run stats.
    """

    # ------------------------------------------------------------------
    # Initialization
    # ------------------------------------------------------------------
    def __init__(self, db_path):
        self.db_path = db_path
        self.conn = sqlite3.connect(self.db_path)
        self.conn.row_factory = sqlite3.Row
        self._initialize_schema()

    # ------------------------------------------------------------------
    def _initialize_schema(self):
        """Create all DB tables and indexes if not present."""
        cur = self.conn.cursor()

        # ------------------------
        # Table: tasks
        # ------------------------
        cur.execute("""
            CREATE TABLE IF NOT EXISTS tasks (
                id INTEGER PRIMARY KEY,
                name TEXT UNIQUE NOT NULL,
                description TEXT
            );
        """)

        # ------------------------
        # Table: task_runs
        # ------------------------
        cur.execute("""
            CREATE TABLE IF NOT EXISTS task_runs (
                id INTEGER PRIMARY KEY,
                task_id INTEGER NOT NULL,
                date TEXT NOT NULL,
                cycle INTEGER NOT NULL,
                run_type TEXT,
                start_time TEXT,
                end_time TEXT,
                runtime_sec REAL,
                notes TEXT,
                FOREIGN KEY(task_id) REFERENCES tasks(id)
            );
        """)

        # ------------------------
        # Table: obs_spaces
        # ------------------------
        cur.execute("""
            CREATE TABLE IF NOT EXISTS obs_spaces (
                id INTEGER PRIMARY KEY,
                name TEXT UNIQUE NOT NULL,
                category TEXT,
                description TEXT
            );
        """)

        # ------------------------
        # obs_space_collections
        # ------------------------
        cur.execute("""
            CREATE TABLE IF NOT EXISTS obs_space_collections (
                id INTEGER PRIMARY KEY,
                name TEXT UNIQUE NOT NULL,
                hash TEXT UNIQUE NOT NULL,
                description TEXT
            );
        """)

        # ------------------------
        # members of a collection
        # ------------------------
        cur.execute("""
            CREATE TABLE IF NOT EXISTS obs_space_collection_members (
                collection_id INTEGER,
                obs_space_id INTEGER,
                FOREIGN KEY(collection_id) REFERENCES obs_space_collections(id),
                FOREIGN KEY(obs_space_id) REFERENCES obs_spaces(id),
                PRIMARY KEY(collection_id, obs_space_id)
            );
        """)

        # ------------------------
        # task_run_details
        # ------------------------
        cur.execute("""
            CREATE TABLE IF NOT EXISTS task_run_details (
                id INTEGER PRIMARY KEY,
                task_run_id INTEGER NOT NULL,
                obs_space_id INTEGER NOT NULL,
                obs_count INTEGER,
                runtime_sec REAL,
                FOREIGN KEY(task_run_id) REFERENCES task_runs(id),
                FOREIGN KEY(obs_space_id) REFERENCES obs_spaces(id)
            );
        """)

        # ----------------------------------------------------------------------
        # Indexes for performance
        # ----------------------------------------------------------------------
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_task_runs_task_cycle_date
            ON task_runs(task_id, cycle, date);
        """)

        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_trd_run
            ON task_run_details(task_run_id);
        """)

        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_trd_space
            ON task_run_details(obs_space_id);
        """)

        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_collections_hash
            ON obs_space_collections(hash);
        """)

        self.conn.commit()

    # ======================================================================
    # Reset entire DB
    # ======================================================================
    def reset(self):
        """Drop all tables and recreate them."""
        cur = self.conn.cursor()

        cur.execute("DROP TABLE IF EXISTS task_run_details;")
        cur.execute("DROP TABLE IF EXISTS obs_space_collection_members;")
        cur.execute("DROP TABLE IF EXISTS obs_space_collections;")
        cur.execute("DROP TABLE IF EXISTS obs_spaces;")
        cur.execute("DROP TABLE IF EXISTS task_runs;")
        cur.execute("DROP TABLE IF EXISTS tasks;")

        self.conn.commit()
        self._initialize_schema()

    # ======================================================================
    # Task helpers
    # ======================================================================
    def get_task_id(self, task_name):
        """Return the ID of a task, creating it if needed."""
        cur = self.conn.cursor()

        row = cur.execute(
            "SELECT id FROM tasks WHERE name = ?;", (task_name,)
        ).fetchone()

        if row:
            return row["id"]

        cur.execute(
            "INSERT INTO tasks(name) VALUES (?);",
            (task_name,)
        )
        self.conn.commit()
        return cur.lastrowid

    # ======================================================================
    # Logging task runs
    # ======================================================================
    def log_task_run(self, task_id, date, cycle, run_type,
                     start_time, end_time, runtime_sec, notes):
        cur = self.conn.cursor()

        cur.execute("""
            INSERT INTO task_runs(
                task_id, date, cycle, run_type,
                start_time, end_time, runtime_sec, notes
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?);
        """, (task_id, date, cycle, run_type,
              start_time, end_time, runtime_sec, notes))

        self.conn.commit()
        return cur.lastrowid

    # ======================================================================
    # Obs-space helpers
    # ======================================================================
    def ensure_obs_space(self, name, category=None, description=None):
        """Create obs-space entry if needed."""
        cur = self.conn.cursor()

        row = cur.execute(
            "SELECT id FROM obs_spaces WHERE name = ?;",
            (name,)
        ).fetchone()

        if row:
            return row["id"]

        cur.execute(
            """
            INSERT INTO obs_spaces(name, category, description)
            VALUES (?, ?, ?);
            """,
            (name, category, description)
        )

        self.conn.commit()
        return cur.lastrowid

    # ======================================================================
    # Obs-space collections
    # ======================================================================
    @staticmethod
    def _collection_hash(collection_set):
        """Stable hash for a set of obs space names."""
        joined = ",".join(sorted(collection_set))
        return hashlib.sha1(joined.encode("utf-8")).hexdigest()

    def ensure_space_collection(self, category_name, obs_space_names):
        """
        Ensure a unique obs-space collection exists for this exact set.
        Returns the collection ID.
        """
        obs_space_names = list(obs_space_names)
        col_hash = self._collection_hash(obs_space_names)

        cur = self.conn.cursor()

        row = cur.execute(
            "SELECT id FROM obs_space_collections WHERE hash = ?;",
            (col_hash,)
        ).fetchone()

        if row:
            return row["id"]

        # Create new collection
        collection_name = f"{category_name}:{col_hash[:10]}"

        cur.execute("""
            INSERT INTO obs_space_collections(name, hash, description)
            VALUES (?, ?, ?);
        """, (collection_name, col_hash, f"Auto-created collection for {category_name}"))

        collection_id = cur.lastrowid

        # Add members
        for name in obs_space_names:
            obs_id = self.ensure_obs_space(name, category=category_name)
            cur.execute("""
                INSERT INTO obs_space_collection_members(collection_id, obs_space_id)
                VALUES (?, ?);
            """, (collection_id, obs_id))

        self.conn.commit()
        return collection_id

    # ======================================================================
    # Logging run details
    # ======================================================================
    def log_task_run_detail(self, task_run_id, obs_space_id, obs_count, runtime_sec):
        cur = self.conn.cursor()
        cur.execute("""
            INSERT INTO task_run_details(
                task_run_id, obs_space_id, obs_count, runtime_sec
            ) VALUES (?, ?, ?, ?);
        """, (task_run_id, obs_space_id, obs_count, runtime_sec))

        self.conn.commit()
        return cur.lastrowid

    # ======================================================================
    # Fetching Helpers
    # ======================================================================
    def fetch_task_time_series(self, task_name):
        """Return all runtime_sec values for a task ordered by date+cycle."""
        cur = self.conn.cursor()
        return cur.execute("""
            SELECT tr.date, tr.cycle, tr.runtime_sec
            FROM task_runs tr
            JOIN tasks t ON t.id = tr.task_id
            WHERE t.name = ?
            ORDER BY tr.date, tr.cycle;
        """, (task_name,)).fetchall()

    def fetch_obs_count_time_series(self, obs_space_name):
        """Time series for a single obs-space."""
        cur = self.conn.cursor()
        return cur.execute("""
            SELECT tr.date, tr.cycle, d.obs_count
            FROM task_run_details d
            JOIN obs_spaces s ON s.id = d.obs_space_id
            JOIN task_runs tr ON tr.id = d.task_run_id
            WHERE s.name = ?
            ORDER BY tr.date, tr.cycle;
        """, (obs_space_name,)).fetchall()

    def fetch_obs_count_for_collection(self, collection_name):
        """Sum of obs across a collection."""
        cur = self.conn.cursor()
        return cur.execute("""
            SELECT tr.date, tr.cycle, SUM(d.obs_count) AS total_obs
            FROM task_run_details d
            JOIN task_runs tr ON tr.id = d.task_run_id
            JOIN obs_space_collection_members m ON m.obs_space_id = d.obs_space_id
            JOIN obs_space_collections c ON c.id = m.collection_id
            WHERE c.name = ?
            GROUP BY tr.date, tr.cycle
            ORDER BY tr.date, tr.cycle;
        """, (collection_name,)).fetchall()

    def fetch_all_runs(self):
        cur = self.conn.cursor()
        return cur.execute("""
            SELECT tr.id, t.name AS task, tr.date, tr.cycle,
                   tr.run_type, tr.runtime_sec
            FROM task_runs tr
            JOIN tasks t ON t.id = tr.task_id
            ORDER BY tr.date, tr.cycle;
        """).fetchall()

    def fetch_run_details(self, task_run_id):
        cur = self.conn.cursor()
        return cur.execute("""
            SELECT s.name AS obs_space, d.obs_count, d.runtime_sec
            FROM task_run_details d
            JOIN obs_spaces s ON s.id = d.obs_space_id
            WHERE d.task_run_id = ?;
        """, (task_run_id,)).fetchall()
