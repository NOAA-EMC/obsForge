#!/usr/bin/env python3
import argparse
import sys
import os

# Ensure imports work
sys.path.append(os.path.join(os.path.dirname(__file__), '../../..'))

from pyobsforge.monitor.scanner.discovery_scanner import DiscoveryScanner

def main():
    parser = argparse.ArgumentParser(description="Test the New Discovery Scanner")
    parser.add_argument("--data-root", required=True, help="Root directory containing logs/ and data")
    parser.add_argument("--date", required=True, help="YYYYMMDD")
    parser.add_argument("--cycle", required=True, help="HH (e.g. 00, 06)")
    
    args = parser.parse_args()
    
    if not os.path.isdir(args.data_root):
        print(f"Error: Data root {args.data_root} does not exist.")
        sys.exit(1)

    scanner = DiscoveryScanner(args.data_root)
    
    # Run the scan (Prints output to console)
    scanner.scan_cycle(args.date, args.cycle)

if __name__ == "__main__":
    main()
