import sqlite3
import logging

logger = logging.getLogger("MonitorSchema")

class MonitorSchema:
    """
    Defines the database structure for the Monitoring System.
    Focuses on Workflow History, File Inventory, and Data Quality.
    """

    def __init__(self, conn: sqlite3.Connection):
        self.conn = conn
        self._init_schema()

    def _init_schema(self):
        """Executes DDL statements to create tables."""
        cur = self.conn.cursor()
        
        # ======================================================================
        # 1. WORKFLOW ENTITIES
        # ======================================================================
        cur.execute("""
            CREATE TABLE IF NOT EXISTS tasks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT UNIQUE NOT NULL,
                description TEXT
            )
        """)
        
        cur.execute("""
            CREATE TABLE IF NOT EXISTS task_runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                task_id INTEGER NOT NULL,
                run_type TEXT,       -- e.g., 'gdas', 'gfs'
                date TEXT,           -- '20251230'
                cycle INTEGER,       -- 0, 6, 12, 18
                job_id TEXT,         -- Batch Scheduler ID
                status TEXT,         -- 'SUCCEEDED', 'FAILED'
                exit_code INTEGER,
                attempt INTEGER,
                host TEXT, 
                logfile TEXT, 
                start_time TEXT, 
                end_time TEXT, 
                runtime_sec REAL,
                FOREIGN KEY(task_id) REFERENCES tasks(id)
            )
        """)

        # ======================================================================
        # 2. DATA ORGANIZATION & DEFINITIONS
        # ======================================================================
        cur.execute("CREATE TABLE IF NOT EXISTS obs_space_categories (id INTEGER PRIMARY KEY, name TEXT UNIQUE)")

        cur.execute("""
            CREATE TABLE IF NOT EXISTS obs_spaces (
                id INTEGER PRIMARY KEY, 
                category_id INTEGER, 
                name TEXT UNIQUE NOT NULL,
                FOREIGN KEY(category_id) REFERENCES obs_space_categories(id)
            )
        """)

        # VARIABLES: Enhanced for QC Checks
        cur.execute("""
            CREATE TABLE IF NOT EXISTS variables (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT UNIQUE NOT NULL,       -- e.g. 'waterTemperature'
                
                -- Metadata
                data_type TEXT,
                dimensionality INTEGER,
                
                -- QC Parameters (Added for Inspector)
                units TEXT,                      -- 'kelvin', 'psu'
                valid_min REAL,                  -- Absolute Physical Floor
                valid_max REAL,                  -- Absolute Physical Ceiling
                min_std_dev REAL DEFAULT 0.0     -- 'Frozen' sensor check
            )
        """)

        # ======================================================================
        # 3. INVENTORY & HISTORY
        # ======================================================================
        cur.execute("""
            CREATE TABLE IF NOT EXISTS file_inventory (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                task_run_id INTEGER,
                obs_space_id INTEGER,
                file_path TEXT UNIQUE NOT NULL,
                integrity_status TEXT,           -- 'OK', 'CORRUPT', 'WARNING'
                obs_count INTEGER,
                file_size_bytes INTEGER,
                file_modified_time INTEGER,      -- For Change Detection
                error_message TEXT,
                properties TEXT,
                FOREIGN KEY(task_run_id) REFERENCES task_runs(id),
                FOREIGN KEY(obs_space_id) REFERENCES obs_spaces(id)
            )
        """)

        # ======================================================================
        # 4. STATISTICS & CONTENT
        # ======================================================================
        cur.execute("""
            CREATE TABLE IF NOT EXISTS obs_space_content (
                obs_space_id INTEGER, 
                variable_id INTEGER, 
                group_name TEXT, 
                PRIMARY KEY(obs_space_id, variable_id, group_name)
            )
        """)
        
        cur.execute("""
            CREATE TABLE IF NOT EXISTS file_variable_statistics (
                id INTEGER PRIMARY KEY, 
                file_id INTEGER, 
                variable_id INTEGER, 
                min_val REAL, max_val REAL, mean_val REAL, std_dev REAL, 
                FOREIGN KEY(file_id) REFERENCES file_inventory(id) ON DELETE CASCADE
            )
        """)
        
        cur.execute("""
            CREATE TABLE IF NOT EXISTS file_domains (
                id INTEGER PRIMARY KEY, 
                file_id INTEGER UNIQUE, 
                start_time INTEGER, end_time INTEGER, 
                min_lat REAL, max_lat REAL, min_lon REAL, max_lon REAL, 
                FOREIGN KEY(file_id) REFERENCES file_inventory(id) ON DELETE CASCADE
            )
        """)
        
        self.conn.commit()
