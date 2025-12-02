'''
Categories

These are logical groupings of obs spaces, often based on the type of observation.

Examples: 'sst', 'icec', 'adt', 'sss'.

Typically, the workflow generates data in directories that correspond to categories.

Categories are persistent concepts across cycles; the set of obs spaces in a category may vary per cycle.

Collections

Collections are concrete sets of obs spaces that exist at a given moment (or cycle).

A collection may be a subset of a category or span multiple categories.

Collections are useful if you want to record the actual set of files/obs spaces processed in a run.

A collection is essentially the snapshot of obs spaces for a workflow run.

It can have a hash or unique ID to detect if an identical set has been processed before.
'''

import sqlite3
from typing import List, Tuple

class MonitorDB:
    def __init__(self, db_path: str):
        self.conn = sqlite3.connect(db_path)
        self._initialize_schema()

    def _initialize_schema(self):
        cur = self.conn.cursor()

        # --- Categories ---
        cur.execute("""
        CREATE TABLE IF NOT EXISTS obs_space_categories (
            id INTEGER PRIMARY KEY,
            name TEXT UNIQUE NOT NULL,
            description TEXT
        );
        """)

        # --- Obs spaces ---
        cur.execute("""
        CREATE TABLE IF NOT EXISTS obs_spaces (
            id INTEGER PRIMARY KEY,
            name TEXT UNIQUE NOT NULL,
            category_id INTEGER NOT NULL,
            description TEXT,
            FOREIGN KEY(category_id) REFERENCES obs_space_categories(id)
        );
        """)

        # --- Collections ---
        cur.execute("""
        CREATE TABLE IF NOT EXISTS obs_space_collections (
            id INTEGER PRIMARY KEY,
            hash TEXT UNIQUE NOT NULL,
            description TEXT
        );
        """)

        cur.execute("""
        CREATE TABLE IF NOT EXISTS obs_space_collection_members (
            collection_id INTEGER,
            obs_space_id INTEGER,
            PRIMARY KEY(collection_id, obs_space_id),
            FOREIGN KEY(collection_id) REFERENCES obs_space_collections(id),
            FOREIGN KEY(obs_space_id) REFERENCES obs_spaces(id)
        );
        """)

        # --- Tasks ---
        cur.execute("""
        CREATE TABLE IF NOT EXISTS tasks (
            id INTEGER PRIMARY KEY,
            name TEXT UNIQUE NOT NULL,
            description TEXT
        );
        """)

        # --- Task runs ---
        cur.execute("""
        CREATE TABLE IF NOT EXISTS task_runs (
            id INTEGER PRIMARY KEY,
            task_id INTEGER NOT NULL,
            date TEXT NOT NULL,
            cycle INTEGER NOT NULL,
            run_type TEXT,
            logfile TEXT,
            start_time TEXT,
            end_time TEXT,
            runtime_sec REAL,
            notes TEXT,
            FOREIGN KEY(task_id) REFERENCES tasks(id)
        );
        """)

        # --- Task run details ---
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

        # --- Indexes ---
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

    # --- Helper methods for inserting/fetching ---

    def reset_database(self):
        cur = self.conn.cursor()
        tables = [
            "task_run_details",
            "task_runs",
            "tasks",
            "obs_space_collection_members",
            "obs_space_collections",
            "obs_spaces",
            "obs_space_categories"
        ]
        for t in tables:
            cur.execute(f"DROP TABLE IF EXISTS {t}")
        self.conn.commit()
        self._initialize_schema()

    def fetch_task_time_series(self, task_name: str) -> List[Tuple]:
        cur = self.conn.cursor()
        cur.execute("""
            SELECT tr.date, tr.cycle, tr.runtime_sec
            FROM task_runs tr
            JOIN tasks t ON t.id = tr.task_id
            WHERE t.name = ?
            ORDER BY tr.date, tr.cycle
        """, (task_name,))
        return cur.fetchall()

    def get_or_create_task(self, name: str) -> int:
        cur = self.conn.cursor()
        cur.execute("SELECT id FROM tasks WHERE name = ?", (name,))
        row = cur.fetchone()
        if row:
            return row[0]

        cur.execute(
            "INSERT INTO tasks (name, description) VALUES (?, ?)",
            (name, None)
        )
        self.conn.commit()
        return cur.lastrowid


    def get_or_create_category(self, name: str) -> int:
        cur = self.conn.cursor()
        cur.execute("SELECT id FROM obs_space_categories WHERE name = ?", (name,))
        row = cur.fetchone()
        if row:
            return row[0]

        cur.execute(
            "INSERT INTO obs_space_categories (name, description) VALUES (?, ?)",
            (name, None)
        )
        self.conn.commit()
        return cur.lastrowid


    def get_or_create_obs_space(self, obs_space_name: str, category_id: int) -> int:
        cur = self.conn.cursor()
        cur.execute("SELECT id FROM obs_spaces WHERE name = ?", (obs_space_name,))
        row = cur.fetchone()
        if row:
            return row[0]

        cur.execute(
            """
            INSERT INTO obs_spaces (name, category_id, description)
            VALUES (?, ?, ?)
            """,
            (obs_space_name, category_id, None)
        )
        self.conn.commit()
        return cur.lastrowid


    def log_task_run(
        self, task_id: int, date: str, cycle: int,
        run_type: str, start_time: str, end_time: str,
        runtime_sec: float, logfile: str, notes: str = None
    ) -> int:

        cur = self.conn.cursor()
        cur.execute(
            """
            INSERT INTO task_runs
            (task_id, date, cycle, run_type, logfile, start_time, end_time, runtime_sec, notes)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (task_id, date, cycle, run_type, logfile, start_time, end_time, runtime_sec, notes)
        )
        self.conn.commit()
        return cur.lastrowid


    def log_task_run_detail(
        self, task_run_id: int, obs_space_id: int, obs_count: int, runtime_sec: float
    ):
        cur = self.conn.cursor()
        cur.execute(
            """
            INSERT INTO task_run_details
            (task_run_id, obs_space_id, obs_count, runtime_sec)
            VALUES (?, ?, ?, ?)
            """,
            (task_run_id, obs_space_id, obs_count, runtime_sec)
        )
        self.conn.commit()


    def fetch_obs_count_time_series(self, obs_space_name: str) -> List[Tuple]:
        cur = self.conn.cursor()
        cur.execute("""
            SELECT tr.date, tr.cycle, d.obs_count
            FROM task_run_details d
            JOIN obs_spaces s ON s.id = d.obs_space_id
            JOIN task_runs tr ON tr.id = d.task_run_id
            WHERE s.name = ?
            ORDER BY tr.date, tr.cycle
        """, (obs_space_name,))
        return cur.fetchall()

    def fetch_obs_count_for_category(self, category_name: str) -> List[Tuple]:
        cur = self.conn.cursor()
        cur.execute("""
            SELECT tr.date, tr.cycle, SUM(d.obs_count) AS total_obs
            FROM task_run_details d
            JOIN task_runs tr ON tr.id = d.task_run_id
            JOIN obs_spaces s ON s.id = d.obs_space_id
            JOIN obs_space_categories c ON c.id = s.category_id
            WHERE c.name = ?
            GROUP BY tr.date, tr.cycle
            ORDER BY tr.date, tr.cycle
        """, (category_name,))
        return cur.fetchall()

    def fetch_all_runs(self) -> List[Tuple]:
        cur = self.conn.cursor()
        return cur.execute("""
            SELECT tr.id, t.name AS task, tr.date, tr.cycle, tr.run_type, tr.runtime_sec
            FROM task_runs tr
            JOIN tasks t ON t.id = tr.task_id
            ORDER BY tr.date, tr.cycle
        """).fetchall()

    def fetch_run_details(self, task_run_id: int) -> List[Tuple]:
        cur = self.conn.cursor()
        return cur.execute("""
            SELECT s.name AS obs_space, d.obs_count, d.runtime_sec
            FROM task_run_details d
            JOIN obs_spaces s ON s.id = d.obs_space_id
            WHERE d.task_run_id = ?
        """, (task_run_id,)).fetchall()

