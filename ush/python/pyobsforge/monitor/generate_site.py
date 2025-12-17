#!/usr/bin/env python3
import argparse
import sys
import os

# Ensure the package path is available if running from the script directory
# (Optional safety measure, though standard python path usually handles it)
sys.path.append(os.path.join(os.path.dirname(__file__), '../../..'))

from pyobsforge.monitor.reporting.site_generator import SiteGenerator

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
    
    # Validation
    if not os.path.exists(args.db):
        print(f"Error: Database file not found at {args.db}")
        sys.exit(1)

    try:
        # Initialize and Run the Generator
        generator = SiteGenerator(args.db, args.out)
        generator.build()
        
        print(f"\nSuccess! Website generated at:")
        print(f"  {os.path.abspath(os.path.join(args.out, 'index.html'))}")
        
    except Exception as e:
        print(f"\n[FATAL ERROR] Failed to generate site: {e}")
        # Print stack trace for debugging
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()
