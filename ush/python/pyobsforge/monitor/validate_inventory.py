#!/usr/bin/env python3
import argparse
import logging
import json

from pyobsforge.monitor.database.db_reader import DBReader
from pyobsforge.monitor.database.monitor_db import MonitorDB

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s", datefmt="%H:%M:%S")
logger = logging.getLogger("ValidateInventory")

class MonitorValidator:
    def __init__(self, db_path):
        self.reader = DBReader(db_path)
        self.writer = MonitorDB(db_path)

    def validate_all(self):
        logger.info("Step 3: Validating Inventory against Knowledge Base...")
        
        # 1. Load Truth via Reader
        knowledge = self.reader.get_knowledge_base()
        logger.info(f"Loaded definitions for {len(knowledge)} Obs Spaces.")

        # 2. Get Candidates via Reader
        # (Files that are physically valid and have metadata to check)
        candidates = self.reader.get_validation_candidates()
        
        updates = 0
        checked = 0
        
        for row in candidates:
            file_id = row['id']
            sid = row['obs_space_id']
            current_status = row['integrity_status']
            
            # Skip if we have no truth for this space yet
            if sid not in knowledge:
                continue
                
            checked += 1
            
            try:
                file_meta = json.loads(row['metadata'])
            except:
                continue

            # 3. Logic: Check Compliance
            is_compliant = True
            violation_msg = None
            
            expected_props = knowledge[sid]
            
            for key, expected_val in expected_props.items():
                if key in file_meta:
                    actual_val = str(file_meta[key])
                    if actual_val != expected_val:
                        is_compliant = False
                        violation_msg = f"Metadata Mismatch: {key} expected '{expected_val}', got '{actual_val}'"
                        break
            
            # 4. State Transitions
            new_status = current_status
            new_msg = row['error_message']

            if is_compliant:
                # Restore to OK if it was marked BAD_META but is now compliant (Backtracking)
                if current_status == "BAD_META":
                    new_status = "OK"
                    new_msg = None 
            else:
                # Mark as BAD
                if current_status == "OK":
                    new_status = "BAD_META"
                    new_msg = violation_msg
                elif current_status == "BAD_META" and new_msg != violation_msg:
                    new_msg = violation_msg

            # 5. Apply Update via Writer
            if new_status != current_status or new_msg != row['error_message']:
                self.writer.update_file_status(file_id, new_status, new_msg)
                updates += 1

        logger.info(f"Validation Complete. Checked {checked} files. Status changed for {updates} files.")

def main():
    parser = argparse.ArgumentParser(description="Step 3: Validate Inventory")
    parser.add_argument("--db", required=True, help="Path to SQLite DB file")
    
    args = parser.parse_args()
    
    validator = MonitorValidator(args.db)
    validator.validate_all()

if __name__ == "__main__":
    main()
