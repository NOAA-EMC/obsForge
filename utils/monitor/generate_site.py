#!/usr/bin/env python3
import argparse
import os
import sys
import traceback

from website.website_generator import WebsiteGenerator


def main():
    """
    Main entry point for generating the ObsForge static website.
    """
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

    parser.add_argument(
        "--data-root",
        required=True,
        help="The root of the data directory tree"
    )

    parser.add_argument(
        "--data-products-root",
        required=True,
        help="The root of the website data directory"
    )

    args = parser.parse_args()

    # 1. Validate Input
    if not os.path.exists(args.db):
        print(f"Error: Database file not found at {args.db}")
        sys.exit(1)

    try:
        # 2. Run Generator
        print(f"Reading Database: {args.db} ...")
        generator = WebsiteGenerator(
            db_path=args.db, 
            data_root=args.data_root,
            data_products_root=args.data_products_root,
            output_dir=args.out, 
        )

        generator.generate()

        out_path = os.path.abspath(os.path.join(args.out, 'index.html'))
        print(f"\n[SUCCESS] Website generated at:")
        print(f"  {out_path}")

    except Exception as e:
        print(f"\n[FATAL ERROR] Failed to generate site: {e}")
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
