#!/usr/bin/env python3
import argparse
import logging
import json
from collections import defaultdict, Counter

from pyobsforge.monitor.database.db_reader import DBReader
from pyobsforge.monitor.database.monitor_db import MonitorDB

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%H:%M:%S")
logger = logging.getLogger("LearnProperties")

class ObsLearner:
    def __init__(self, db_path):
        self.reader = DBReader(db_path)
        self.writer = MonitorDB(db_path)

    def learn_truth(self, consensus_threshold=0.8, min_samples=3):
        logger.info("Starting Knowledge Consolidation (The Judge)...")
        spaces = self.reader.get_all_obs_spaces_map()
        updates = 0
        
        for space in spaces:
            s_id = space['id']
            s_name = space['name']
            
            # 1. Fetch History
            history = self.reader.get_metadata_history(s_id)
            total_samples = len(history)
            if total_samples < min_samples: continue

            # 2. Vote
            votes = defaultdict(Counter)
            for raw_json in history:
                try:
                    meta = json.loads(raw_json)
                    for key, val in meta.items():
                        if key in ['variables', 'platform', 'sensor', 'source']:
                            votes[key][val] += 1
                except: continue

            # 3. Decide Truth
            for prop_key, counts in votes.items():
                top_val, top_count = counts.most_common(1)[0]
                confidence = top_count / total_samples
                
                if confidence >= consensus_threshold:
                    current_val = self.reader.get_current_property(s_id, prop_key)
                    if current_val != top_val:
                        logger.info(f"  [LEARNED] {s_name}: {prop_key} = '{top_val}' (Conf: {confidence:.2f})")
                        self.writer.set_obs_space_property(s_id, prop_key, top_val)
                        updates += 1
                else:
                    logger.warning(f"  [CONFLICT] {s_name}: {prop_key} unstable. Top: '{top_val}' ({confidence:.2f})")

        logger.info(f"Learning Complete. Updated {updates} properties.")

def main():
    parser = argparse.ArgumentParser(description="Step 2: Learn Truth from History")
    parser.add_argument("--db", required=True)
    parser.add_argument("--threshold", type=float, default=0.8)
    parser.add_argument("--min-samples", type=int, default=3)
    args = parser.parse_args()
    
    learner = ObsLearner(args.db)
    learner.learn_truth(args.threshold, args.min_samples)

if __name__ == "__main__":
    main()
