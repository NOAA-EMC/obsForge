import sqlite3
import os
import logging
from typing import Optional, Any, Dict, List

logger = logging.getLogger("MonitorDB")

class MonitorDB:
    """
    WRITE-ONLY Database Access Object.
    
    Responsibilities:
    1. Define Database Schema (Entities, Knowledge Base, Operations, Deep Metrics).
    2. Persist Entities (Tasks, Categories, Obs Spaces).
    3. Persist Knowledge (Variables, Schema Definitions).
    4. Persist Operations (Task Runs, File Inventory).
    5. Persist Deep Metrics (Lineage, Domains, Statistics).
    
    Design Note:
    - This class handles WRITES only.
    - 'Units' and 'Attributes' tables have been removed per requirements.
    - 'Role' logic is inferred at read-time, not stored.
    """
    
    def __init__(self, db_path: str):
        self.db_path = db_path
        self._ensure_path()
        # Enable foreign keys constraint enforcement
        self.conn = sqlite3.connect(db_path) 
        self.conn.execute("PRAGMA foreign_keys = ON;")
        self._init_schema()

    def _ensure_path(self):
        """Creates the database directory if it doesn't exist."""
        parent = os.path.dirname(self.db_path)
        if parent and not os.path.exists(parent):
            try:
                os.makedirs(parent)
            except OSError:
                pass

    def commit(self):
        """Manually commit the current transaction."""
        self.conn.commit()

    def _execute_log(self, sql, params=()):
        """Internal helper to execute SQL statements."""
        return self.conn.execute(sql, params)

    # ------------------------------------------------------------------
    # 1. SCHEMA DEFINITION
    # ------------------------------------------------------------------
    def _init_schema(self):
        """Creates all necessary tables if they do not exist."""
        
        # --- A. KNOWLEDGE BASE (Entities) ---
        
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS obs_space_categories (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT UNIQUE NOT NULL
            )
        """)
        
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS obs_spaces (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                category_id INTEGER,
                FOREIGN KEY(category_id) REFERENCES obs_space_categories(id),
                UNIQUE(name, category_id)
            )
        """)

        # --- B. NORMALIZED VARIABLE REGISTRY (The "Dictionary") ---
        
        # 1. Global Dictionary of all known variables
        # Removed 'units' column.
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS variables (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT UNIQUE NOT NULL,  -- e.g. 'waterTemperature'
                data_type TEXT,             -- e.g. 'float32'
                dimensionality INTEGER      -- e.g. 3 (Rank)
            )
        """)

        # 2. Mapping: Which Obs Space contains which Variable?
        # Removed 'var_role' (inferred from group_name)
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS obs_space_content (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                obs_space_id INTEGER NOT NULL,
                variable_id INTEGER NOT NULL,
                
                group_name TEXT NOT NULL,   -- e.g. 'ObsValue', 'MetaData'
                
                updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY(obs_space_id) REFERENCES obs_spaces(id) ON DELETE CASCADE,
                FOREIGN KEY(variable_id) REFERENCES variables(id),
                UNIQUE(obs_space_id, variable_id, group_name)
            )
        """)

        # Note: obs_space_global_attributes has been DROPPED.

        # --- C. OPERATIONAL TABLES (Tracking) ---

        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS tasks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT UNIQUE NOT NULL
            )
        """)
        
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS task_runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                task_id INTEGER NOT NULL,
                date TEXT NOT NULL,
                cycle INTEGER NOT NULL,
                run_type TEXT,
                
                job_id TEXT,
                status TEXT,
                exit_code INTEGER,
                attempt INTEGER,
                host TEXT,
                logfile TEXT,
                start_time TEXT,
                end_time TEXT,
                runtime_sec REAL,
                
                updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY(task_id) REFERENCES tasks(id),
                UNIQUE(task_id, date, cycle, run_type)
            )
        """)

        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS file_inventory (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                task_run_id INTEGER NOT NULL,
                obs_space_id INTEGER,
                
                file_path TEXT NOT NULL,
                file_type TEXT,
                integrity_status TEXT,
                file_size_bytes INTEGER,
                file_modified_time INTEGER,  -- For Incremental Scanning
                
                obs_count INTEGER,
                error_message TEXT,
                metadata TEXT,               -- Legacy JSON (Backup)
                
                FOREIGN KEY(task_run_id) REFERENCES task_runs(id) ON DELETE CASCADE,
                FOREIGN KEY(obs_space_id) REFERENCES obs_spaces(id),
                UNIQUE(task_run_id, file_path)
            )
        """)

        # --- D. DEEP INVENTORY (Content & Lineage) ---
        
        # 1. Lineage: Tracks input source files
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS file_source_inputs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                file_id INTEGER NOT NULL,
                source_file_path TEXT NOT NULL,
                FOREIGN KEY(file_id) REFERENCES file_inventory(id) ON DELETE CASCADE
            )
        """)
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_file_source_inputs_file_id ON file_source_inputs(file_id)")

        # 2. Domain: 4D Spatiotemporal Bounds
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS file_data_domain (
                file_id INTEGER PRIMARY KEY,
                start_time INTEGER,
                end_time INTEGER,
                min_lat REAL, max_lat REAL,
                min_lon REAL, max_lon REAL,
                min_depth REAL, max_depth REAL,
                FOREIGN KEY(file_id) REFERENCES file_inventory(id) ON DELETE CASCADE
            )
        """)

        # 3. Statistics: Detailed Content Profile
        # Note: Links to 'variables' table via ID. Removed 'units'.
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS file_variable_statistics (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                file_id INTEGER NOT NULL,
                variable_id INTEGER,         -- Link to Global Definition
                group_name TEXT,             -- e.g. ObsValue vs ObsError
                
                min_val REAL,
                max_val REAL,
                mean_val REAL,
                std_dev REAL,
                
                FOREIGN KEY(file_id) REFERENCES file_inventory(id) ON DELETE CASCADE,
                FOREIGN KEY(variable_id) REFERENCES variables(id)
            )
        """)
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_file_var_stats_file_id ON file_variable_statistics(file_id)")

    # ------------------------------------------------------------------
    # 2. SCHEMA REGISTRATION (The "Instant Learner")
    # ------------------------------------------------------------------

    def register_file_schema(self, obs_space_id: int, schema_dict: Dict):
        """
        Takes the schema found in a file and ensures it is registered in the DB.
        Filters out structural noise and the 'root' group.
        """
        # Variables to ignore (Structural Dimensions / Attributes)
        structural_vars = {
            'Location', 'Channel', 'nlocs', 'nchans', 'nvars', 
            'cycle', 'mission', 'scan_position', 'satId', 'record_number',
            'seqno'
        }
        
        for path, meta in schema_dict.items():
            if '/' in path:
                group, var_name = path.rsplit('/', 1)
            else:
                group, var_name = 'root', path
            
            # Skip structural noise and root group variables
            if var_name in structural_vars or group == 'root':
                continue

            # 1. Upsert Global Variable Definition (No Units)
            # If variable exists, we update dimensionality/type to match latest observation
            sql_upsert_var = """
                INSERT INTO variables (name, data_type, dimensionality)
                VALUES (?, ?, ?)
                ON CONFLICT(name) DO UPDATE SET 
                    data_type = COALESCE(excluded.data_type, data_type),
                    dimensionality = COALESCE(excluded.dimensionality, dimensionality)
            """
            self._execute_log(sql_upsert_var, (var_name, meta.get('type'), meta.get('ndim', 1)))
            
            # Get the ID (needed for linking)
            var_id = self._execute_log("SELECT id FROM variables WHERE name=?", (var_name,)).fetchone()[0]
            
            # 2. Link to Obs Space (If not already linked)
            sql_link_content = """
                INSERT OR IGNORE INTO obs_space_content (obs_space_id, variable_id, group_name)
                VALUES (?, ?, ?)
            """
            self._execute_log(sql_link_content, (obs_space_id, var_id, group))

    # ------------------------------------------------------------------
    # 3. ENTITY MANAGEMENT
    # ------------------------------------------------------------------

    def get_or_create_task(self, name: str) -> int:
        self._execute_log("INSERT OR IGNORE INTO tasks(name) VALUES(?)", (name,))
        cur = self._execute_log("SELECT id FROM tasks WHERE name=?", (name,))
        return cur.fetchone()[0]

    def get_or_create_category(self, name: str) -> int:
        self._execute_log("INSERT OR IGNORE INTO obs_space_categories(name) VALUES(?)", (name,))
        cur = self._execute_log("SELECT id FROM obs_space_categories WHERE name=?", (name,))
        return cur.fetchone()[0]

    def get_or_create_obs_space(self, name: str, category_id: int) -> int:
        self._execute_log(
            "INSERT OR IGNORE INTO obs_spaces(name, category_id) VALUES(?, ?)", 
            (name, category_id)
        )
        cur = self._execute_log(
            "SELECT id FROM obs_spaces WHERE name=? AND category_id=?", 
            (name, category_id)
        )
        return cur.fetchone()[0]

    # ------------------------------------------------------------------
    # 4. OPERATIONAL LOGGING (Scanner)
    # ------------------------------------------------------------------

    def log_task_run(self, task_id, date, cycle, run_type, 
                     job_id=None, status=None, exit_code=None, attempt=None, host=None,
                     logfile=None, start_time=None, end_time=None, runtime_sec=None):
        """Records a task execution event."""
        action = "updated"
        
        check_sql = """
            SELECT id FROM task_runs 
            WHERE task_id=? AND date=? AND cycle=? AND run_type=?
        """
        cursor = self._execute_log(check_sql, (task_id, date, cycle, run_type))
        row = cursor.fetchone()

        if row:
            tr_id = row[0]
            update_sql = """
                UPDATE task_runs SET 
                    job_id=?, status=?, exit_code=?, attempt=?, host=?,
                    logfile=?, start_time=?, end_time=?, runtime_sec=?, 
                    updated_at=CURRENT_TIMESTAMP
                WHERE id=?
            """
            self._execute_log(update_sql, (job_id, status, exit_code, attempt, host, logfile, start_time, end_time, runtime_sec, tr_id))
        else:
            action = "inserted"
            insert_sql = """
                INSERT INTO task_runs (
                    task_id, date, cycle, run_type, 
                    job_id, status, exit_code, attempt, host,
                    logfile, start_time, end_time, runtime_sec
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
            cur = self._execute_log(insert_sql, (task_id, date, cycle, run_type, job_id, status, exit_code, attempt, host, logfile, start_time, end_time, runtime_sec))
            tr_id = cur.lastrowid

        return tr_id, action

    def log_file_inventory(self, task_run_id: int, obs_space_id: Optional[int], 
                           path: str, integrity: str, size: int, 
                           mtime: int, 
                           obs_count: int = 0, error_msg: str = None, 
                           metadata_json: str = None, properties: dict = None):
        """
        Records file existence and extracts basic lineage from properties.
        """
        file_type = path.split('.')[-1] if '.' in path else 'unknown'
        
        # 1. Insert Header
        sql = """
            INSERT INTO file_inventory (
                task_run_id, obs_space_id, file_path, file_type, 
                integrity_status, file_size_bytes, file_modified_time,
                obs_count, error_message, metadata
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(task_run_id, file_path) DO UPDATE SET
                integrity_status = excluded.integrity_status,
                file_size_bytes  = excluded.file_size_bytes,
                file_modified_time = excluded.file_modified_time,
                obs_count        = excluded.obs_count,
                error_message    = excluded.error_message,
                metadata         = excluded.metadata
        """
        self._execute_log(sql, (
            task_run_id, obs_space_id, path, file_type, 
            integrity, size, mtime, 
            obs_count, error_msg, metadata_json
        ))
        
        # Get File ID
        cur = self._execute_log("SELECT id FROM file_inventory WHERE task_run_id=? AND file_path=?", (task_run_id, path))
        file_id = cur.fetchone()[0]

        # 2. Extract Lineage (obs_source_files)
        if properties and 'obs_source_files' in properties:
            # Clear old lineage for this file (Full Replace)
            self._execute_log("DELETE FROM file_source_inputs WHERE file_id=?", (file_id,))
            
            raw_sources = str(properties['obs_source_files'])
            # Basic CSV parsing (handles simple "file1, file2")
            sources = [s.strip().strip('"').strip("'") for s in raw_sources.split(',')]
            
            for src in sources:
                if src:
                    self._execute_log(
                        "INSERT INTO file_source_inputs(file_id, source_file_path) VALUES(?, ?)", 
                        (file_id, src)
                    )
        
        return file_id

    # ------------------------------------------------------------------
    # 5. METRIC LOGGING (Deep Inventory)
    # ------------------------------------------------------------------

    def log_file_domain(self, file_id: int, start: int, end: int, 
                        min_lat: float, max_lat: float, 
                        min_lon: float, max_lon: float, 
                        min_depth: float = None, max_depth: float = None):
        """Records the 4D spatiotemporal bounding box."""
        sql = """
            INSERT OR REPLACE INTO file_data_domain 
            (file_id, start_time, end_time, min_lat, max_lat, min_lon, max_lon, min_depth, max_depth)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        self._execute_log(sql, (file_id, start, end, min_lat, max_lat, min_lon, max_lon, min_depth, max_depth))

    def log_variable_statistics(self, file_id: int, stats_list: List[Dict]):
        """
        Records stats for multiple variables.
        stats_list = [{'name': 'ObsValue/sst', 'min': 280.0, ...}, ...]
        No Units stored.
        """
        self._execute_log("DELETE FROM file_variable_statistics WHERE file_id=?", (file_id,))
        
        sql = """
            INSERT INTO file_variable_statistics 
            (file_id, variable_id, group_name, min_val, max_val, mean_val, std_dev)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """
        
        for s in stats_list:
            if '/' in s['name']:
                group, var_name = s['name'].rsplit('/', 1)
            else:
                group, var_name = 'root', s['name']
            
            # Lookup Variable ID from Global Dictionary
            cur = self._execute_log("SELECT id FROM variables WHERE name=?", (var_name,))
            row = cur.fetchone()
            
            if row:
                var_id = row[0]
                self._execute_log(sql, (
                    file_id, var_id, group, 
                    s.get('min'), s.get('max'), s.get('mean'), s.get('std')
                ))

    def update_file_status(self, file_id: int, status: str, error_msg: Optional[str]):
        """Called by Validator to flag files."""
        self._execute_log(
            "UPDATE file_inventory SET integrity_status=?, error_message=? WHERE id=?", 
            (status, error_msg, file_id)
        )
