import os
import sqlite3
from typing import List, Tuple, Optional


class MonitorDB:
    """
    SQLite-backed DB for storing monitored task runs and obs-space data.
    """

    def __init__(self, db_path: str):
        self.db_path = db_path

        # Ensure DB directory exists
        parent = os.path.dirname(db_path)
        if parent and not os.path.exists(parent):
            raise FileNotFoundError(f"Directory does not exist: {parent}")

        # Create file if missing
        self.conn = sqlite3.connect(db_path)
        self.conn.execute("PRAGMA foreign_keys = ON;")

        self._initialize_schema()

    # -----------------------------------------------------
    def _initialize_schema(self):
        """Create tables and indexes if missing."""
        cur = self.conn.cursor()

        cur.execute("""
        CREATE TABLE IF NOT EXISTS obs_space_categories (
            id INTEGER PRIMARY KEY,
            name TEXT UNIQUE NOT NULL,
            description TEXT
        );
        """)

        cur.execute("""
        CREATE TABLE IF NOT EXISTS obs_spaces (
            id INTEGER PRIMARY KEY,
            name TEXT UNIQUE NOT NULL,
            category_id INTEGER NOT NULL,
            description TEXT,
            FOREIGN KEY(category_id) REFERENCES obs_space_categories(id)
        );
        """)

        cur.execute("""
        CREATE TABLE IF NOT EXISTS tasks (
            id INTEGER PRIMARY KEY,
            name TEXT UNIQUE NOT NULL,
            description TEXT
        );
        """)

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

        cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_task_runs_task_cycle_date
        ON task_runs(task_id, cycle, date);
        """)

        cur.execute("""CREATE INDEX IF NOT EXISTS idx_trd_run ON task_run_details(task_run_id);""")
        cur.execute("""CREATE INDEX IF NOT EXISTS idx_trd_space ON task_run_details(obs_space_id);""")

        self.conn.commit()

    # -----------------------------------------------------
    # Helper methods
    def get_or_create_task(self, name: str) -> int:
        cur = self.conn.cursor()
        cur.execute("SELECT id FROM tasks WHERE name=?", (name,))
        row = cur.fetchone()
        if row:
            return row[0]

        cur.execute("INSERT INTO tasks(name) VALUES(?)", (name,))
        self.conn.commit()
        return cur.lastrowid

    def get_or_create_category(self, name: str) -> int:
        cur = self.conn.cursor()
        cur.execute("SELECT id FROM obs_space_categories WHERE name=?", (name,))
        row = cur.fetchone()
        if row:
            return row[0]

        cur.execute("INSERT INTO obs_space_categories(name) VALUES(?)", (name,))
        self.conn.commit()
        return cur.lastrowid

    def get_or_create_obs_space(self, name: str, category_id: int) -> int:
        cur = self.conn.cursor()
        cur.execute("SELECT id FROM obs_spaces WHERE name=?", (name,))
        row = cur.fetchone()
        if row:
            return row[0]

        cur.execute(
            "INSERT INTO obs_spaces(name, category_id) VALUES(?,?)",
            (name, category_id)
        )
        self.conn.commit()
        return cur.lastrowid

    # -----------------------------------------------------
    # Logging inserts
    def log_task_run(self, task_id: int, date: str, cycle: int, run_type: str,
                     logfile: str, start_time: str, end_time: str,
                     runtime_sec: float, notes: Optional[str] = None) -> int:
        cur = self.conn.cursor()
        cur.execute("""
            INSERT INTO task_runs(task_id, date, cycle, run_type, logfile,
                                  start_time, end_time, runtime_sec, notes)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (task_id, date, cycle, run_type, logfile,
              start_time, end_time, runtime_sec, notes))
        self.conn.commit()
        return cur.lastrowid

    def log_task_run_detail(self, task_run_id: int, obs_space_id: int,
                            obs_count: int, runtime_sec: float):
        cur = self.conn.cursor()
        cur.execute("""
            INSERT INTO task_run_details(task_run_id, obs_space_id,
                                         obs_count, runtime_sec)
            VALUES (?, ?, ?, ?)
        """, (task_run_id, obs_space_id, obs_count, runtime_sec))
        self.conn.commit()

