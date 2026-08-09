import os
import json
import logging
import sqlite3

from processing.ioda_reader.obs_space_ioda_structure import ObsSpaceIodaStructure


from .schema import MonitorSchema
from .db_service import DBDataService

logger = logging.getLogger("MonitorDB")


class ObsSpaceDBService(DBDataService):
    """
    Data service for obs_spaces table.
    """

    def get_or_create_obs_space(self, name, category_id, ioda_structure=None):
        """
        Registers an Obs Space and optionally attaches its immutable IODA structure.

        Returns:
            obs_space_id (int) or None
        """

        # 1. Insert basic obs_space record (idempotent)
        self.execute(
            "INSERT OR IGNORE INTO obs_spaces (name, category_id) VALUES (?, ?)",
            (name, category_id),
        )

        # 2. Attach IODA structure if provided (schema-compatible)
        if ioda_structure is not None:
            try:
                self.execute(
                    """
                    UPDATE obs_spaces
                    SET ioda_spec = ?
                    WHERE name = ?
                      AND ioda_spec IS NULL
                    """,
                    (
                        json.dumps(ioda_structure),
                        name,
                    ),
                )
            except sqlite3.OperationalError as e:
                if "no such column: ioda_spec" in str(e):
                    logger.warning(
                        "Column 'ioda_spec' missing. Skipping structure for obs_space=%s",
                        name,
                    )
                else:
                    raise

        # 3. Fetch and return obs_space id
        row = self.fetch_one(
            "SELECT id FROM obs_spaces WHERE name = ?",
            (name,),
        )

        return row["id"] if row else None


    def get_ioda_structure(self, obs_space_name):
        """
        Retrieves the immutable IODA structure for a specific observation space.

        Returns:
            ObsSpaceIodaStructure instance or None
        """
        try:
            row = self.fetch_one(
                "SELECT ioda_spec FROM obs_spaces WHERE name = ?",
                (obs_space_name,),
            )

            if row and row["ioda_spec"]:
                # Hydrate object from stored JSON
                return ObsSpaceIodaStructure.from_db(row["ioda_spec"])

            logger.warning(
                "No IODA structure found in DB for obs_space=%s",
                obs_space_name,
            )
            return None

        except Exception as e:
            logger.error(
                "Failed to retrieve IODA structure for obs_space=%s: %s",
                obs_space_name,
                e,
            )
            return None

--------------------------------------------------------


    def ooget_or_create_obs_space(self, name, cat_id, ioda_structure=None):
        """
        Registers an Obs Space and optionally attaches its immutable IODA structure.
        """
        # 1. Standard Insert/Ignore for the basic record
        self.conn.execute(
            "INSERT OR IGNORE INTO obs_spaces (name, category_id) VALUES (?, ?)",
            (name, cat_id)
        )

        # 2. If a structure is provided, attempt to update the column.
        # We wrap this in a try/except to stay compatible with the old schema.
        if ioda_structure is not None:
            try:
                self.conn.execute(
                    "UPDATE obs_spaces SET ioda_spec = ? WHERE name = ? AND ioda_spec IS NULL",
                    (ioda_structure.to_db(), name)
                )
            except sqlite3.OperationalError as e:
                if "no such column: ioda_spec" in str(e):
                    logger.warning(f"Column 'ioda_spec' missing. Skipping structure for {name}.")
                else:
                    raise e

        # 3. Retrieve and return the ID as before
        res = self.conn.execute(
            "SELECT id FROM obs_spaces WHERE name=?",
            (name,)
        ).fetchone()
        
        return res[0] if res else None


    #derived
    def oldget_or_create_obs_space(self, name: str, category_id: int) -> int:
        row = self.fetch_one("SELECT id, category_id FROM obs_spaces WHERE name = ?", (name,))
            
        if row:
            if row['category_id'] != category_id:
                logger.warning(f"Correcting Category for {name}: Old={row['category_id']} New={category_id}")
                self.execute("UPDATE obs_spaces SET category_id=? WHERE id=?", (category_id, row['id']))
            return row['id']
                        
        return self.execute(
            "INSERT INTO obs_spaces (name, category_id) VALUES (?, ?)", 
            (name, category_id)
        )   


    def old_get_or_create_obs_space(self, name, cat_id):
        """Registers an Obs Space."""
        self.conn.execute(
            "INSERT OR IGNORE INTO obs_spaces (name, category_id) VALUES (?, ?)",
            (name, cat_id)
        )
        return self.conn.execute(
            "SELECT id FROM obs_spaces WHERE name=?",
            (name,)
        ).fetchone()[0]





def save_ioda_structure(conn, obs_space_name, file_path):
    struct = ObsSpaceIodaStructure()
    struct.read_from_file(file_path)
    
    cur = conn.cursor()
    cur.execute(
        "UPDATE obs_spaces SET ioda_spec = ? WHERE name = ?",
        (struct.to_db(), obs_space_name)
    )
    conn.commit()



def get_ioda_structure(conn, obs_space_name):
    cur = conn.cursor()
    cur.execute("SELECT ioda_spec FROM obs_spaces WHERE name = ?", (obs_space_name,))
    row = cur.fetchone()
    
    if row and row[0]:
        return ObsSpaceIodaStructure().from_db(row[0])
    return None

def get_ioda_structure(conn, obs_space_name):
    """
    Retrieves the immutable IODA structure for a specific observation space.
    Returns an ObsSpaceIodaStructure object or None if not found.
    """
    try:
        cur = conn.cursor()
        cur.execute("SELECT ioda_spec FROM obs_spaces WHERE name = ?", (obs_space_name,))
        row = cur.fetchone()
        
        if row and row[0]:
            # Hydrate the object from the JSON string stored in the DB
            return ObsSpaceIodaStructure().from_db(row[0])
            
        logger.warning(f"No IODA structure found in DB for obs_space: {obs_space_name}")
        return None
        
    except Exception as e:
        logger.error(f"Failed to retrieve IODA structure for {obs_space_name}: {e}")
        return None


def ensure_ioda_structure(conn, obs_space_name, file_path):
    """
    Reads structure from a file and saves to DB ONLY if it doesn't exist yet.
    """
    existing = get_ioda_structure(conn, obs_space_name)
    if existing:
        return existing # Already defined, skip heavy file I/O

    # Not in DB, so let's extract it from the physical file
    struct = ObsSpaceIodaStructure()
    struct.read_from_file(file_path)
    
    # Save back to DB
    cur = conn.cursor()
    cur.execute(
        "UPDATE obs_spaces SET ioda_spec = ? WHERE name = ?",
        (struct.to_db(), obs_space_name)
    )
    conn.commit()
    logger.info(f"Successfully initialized IODA structure for {obs_space_name}")
    return struct

def get_ioda_spec_by_name(self, name):
        """Retrieves the IODA structure object for a given obs space name."""
        res = self.conn.execute(
            "SELECT ioda_spec FROM obs_spaces WHERE name=?", (name,)
        ).fetchone()
        
        if res and res[0]:
            from .ioda_struct import ObsSpaceIodaStructure # Assuming your class name
            return ObsSpaceIodaStructure().from_db(res[0])
        return None


