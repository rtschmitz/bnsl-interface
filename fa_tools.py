#!/usr/bin/env python3
import argparse
import os
import sys
import runpy
from pathlib import Path

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--sync-registry", action="store_true")
    parser.add_argument("--prefetch-headshots", action="store_true")
    args = parser.parse_args()

    # Build argv exactly like the old CLI
    argv = ["fa_app.py"]
    if args.sync_registry:
        argv.append("--sync-registry")
    if args.prefetch_headshots:
        argv.append("--prefetch-headshots")

    # Run free_agency.py as if it were executed directly
    script_path = Path(__file__).resolve().with_name("fa_app.py")
    if not script_path.exists():
        raise FileNotFoundError(f"Couldn't find {script_path}")

    old_argv = sys.argv[:]
    try:
        sys.argv = argv
        runpy.run_path(str(script_path), run_name="__main__")
    finally:
        sys.argv = old_argv

if __name__ == "__main__":
    main()
