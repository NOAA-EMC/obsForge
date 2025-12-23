#!/usr/bin/env python3
import argparse
import sys
import os
import traceback

# --- ARCHITECTURE IMPORTS ---
# Assumes PYTHONPATH includes the directory containing 'pyobsforge'
try:
    from pyobsforge.monitor.reporting.website_generator import WebsiteGenerator
except ImportError as e:
    print("\n[ERROR] Could not import project modules.")
    print(f"Details: {e}")
    print("\nPlease ensure PYTHONPATH is set correctly.")
    print("Example: export PYTHONPATH=$PYTHONPATH:/path/to/ush/python\n")
    sys.exit(1)

def main():
    parser = argparse.ArgumentParser(
        description="Generate ObsForge Static Website",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    
    parser.add_argument(
        "--db", 
        required=True, 
        help="Path to the SQLite database file (e.g., emcda.db)"
    )
    
    parser.add_argument(
        "--out", 
        required=True, 
        help="Output directory where the website will be generated"
    )
    
    args = parser.parse_args()
    
    # 1. Validate Input
    if not os.path.exists(args.db):
        print(f"Error: Database file not found at {args.db}")
        sys.exit(1)

    try:
        # 2. Run Generator
        print(f"Reading Database: {args.db} ...")
        generator = WebsiteGenerator(args.db, args.out)
        
        generator.generate()
        
        print(f"\n[SUCCESS] Website generated at:")
        print(f"  {os.path.abspath(os.path.join(args.out, 'index.html'))}")
        
    except Exception as e:
        print(f"\n[FATAL ERROR] Failed to generate site: {e}")
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()
