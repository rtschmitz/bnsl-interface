from __future__ import annotations

"""Centralized paths for BNSL persistent/runtime data.

Set BNSL_DATA_DIR=/data on Render to place mutable state on the mounted
persistent disk.  For local development you can either leave it unset, which
keeps the old repo-relative behavior, or set BNSL_DATA_DIR=.bnsl_data to mimic
Render without touching production data.

Optional overrides:
  BNSL_DB_DIR          directory for SQLite DBs
  BNSL_INPUT_DIR       directory for admin/input files
  BNSL_GENERATED_DIR   directory for generated reference DBs
  BNSL_CACHE_DIR       directory for generated caches, e.g. player images
"""

import os
from pathlib import Path

APP_DIR = Path(__file__).resolve().parent

DATA_DIR = Path(os.environ.get("BNSL_DATA_DIR", str(APP_DIR))).resolve()
DB_DIR = Path(os.environ.get("BNSL_DB_DIR", str(DATA_DIR))).resolve()
INPUT_DIR = Path(os.environ.get("BNSL_INPUT_DIR", str(DATA_DIR / "inputs"))).resolve()
GENERATED_DIR = Path(os.environ.get("BNSL_GENERATED_DIR", str(DATA_DIR / "generated"))).resolve()
CACHE_DIR = Path(os.environ.get("BNSL_CACHE_DIR", str(DATA_DIR / "cache"))).resolve()

# DB/generated/cache directories are always safe to create.  INPUT_DIR is also
# created so initial Render setup can copy input files into a known location.
for _directory in (DATA_DIR, DB_DIR, INPUT_DIR, GENERATED_DIR, CACHE_DIR):
    _directory.mkdir(parents=True, exist_ok=True)


def db_path(filename: str) -> Path:
    """Return a persistent SQLite path."""
    DB_DIR.mkdir(parents=True, exist_ok=True)
    return DB_DIR / filename


def generated_path(filename: str) -> Path:
    """Return a persistent generated-artifact path."""
    GENERATED_DIR.mkdir(parents=True, exist_ok=True)
    return GENERATED_DIR / filename


def input_path(filename: str) -> Path:
    """Return an input path, preferring persistent/admin input over repo copy.

    This lets Render use /data/inputs while local development can still run
    from checked-in or manually placed repo-local files.
    """
    persistent = INPUT_DIR / filename
    if persistent.exists():
        return persistent
    return APP_DIR / filename


def cache_path(*parts: str) -> Path:
    """Return a cache path under the persistent cache directory."""
    path = CACHE_DIR.joinpath(*parts)
    path.parent.mkdir(parents=True, exist_ok=True)
    return path
