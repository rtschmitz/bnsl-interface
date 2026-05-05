#!/usr/bin/env python3
"""
Flask Free Agency App — BNSL FA Framework
----------------------------------------
Single-file Flask app implementing:
- Team/email login (same pattern as your draft app)
- Free agent list (CSV import, plus demo iconic MLB names if missing)
- Fixed-contract bidding (1–6 years + optional club option year, fixed AAV)
- Minimum AAV rules by (years + option)
- Hometown multiplier (1.05x / 1.10x) applied to bid VALUE for hometown team bids
- Outbid rule: new bid must be >= 10% higher bid value than current leader
- 48-hour timer resets on each new bid; auto-sign on expiry
- Watchlist (bid directly from watchlist)
- Bid history page (summary of all bids)
- Modal bid UI with live preview (no chained browser prompts)

Quickstart
==========
pip install flask
python free_agency.py
Visit: http://127.0.0.1:5000

CSV (optional)
==============
If free_agents.csv exists beside this script, it will be imported on first run (when DB is empty).
Schema (case-insensitive headers accepted):
- name
- position
- last_team
- hometown_team
- hometown_seasons   (0,1,2 -> 0=no bonus, 1=5%, 2=10%)
- seed_qo            (0/1) whether to seed with qualifying offer bid at start

Notes
=====
- AAV inputs/outputs are handled in $M (millions). Example: 0.75 == $750k.
"""
from __future__ import annotations
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
import csv
import math
import os
import sqlite3
import unicodedata

import argparse
import re
import time
import requests
from flask import send_file


from flask import Flask, request, jsonify, session, redirect, url_for, render_template_string, abort


from pathlib import Path
import os
import logging
from flask import Blueprint, current_app, has_app_context

APP_DIR = Path(__file__).resolve().parent

# ---------- DB (module default; app factory can override via app.config["DB_PATH"]) ----------
env_db = os.environ.get("DB_PATH")
if env_db:
    DB_PATH = Path(env_db)
else:
    DB_PATH = APP_DIR / "fa.db"

# Ensure parent dir exists for the module-default DB_PATH.
DB_PATH.parent.mkdir(parents=True, exist_ok=True)

def get_db_path() -> Path:
    """
    Safe both inside and outside Flask app context.
    Uses app.config["FA_DB_PATH"] if available, else module default DB_PATH.
    """
    if has_app_context():
        cfg = current_app.config.get("FA_DB_PATH")
        if cfg:
            return Path(cfg)
    return DB_PATH

# ---------- Files / dirs ----------
FREE_AGENTS_CSV = APP_DIR / "free_agents.csv"
PLAYER_REGISTRY_CSV = APP_DIR / "player_registry.csv"  # your new registry file

# Roster CSV is now the source of truth for the FA tab.
# Override with:
#   export BNSL_ROSTER_CSV=/path/to/rostered_2025service_BNSL_arb_updated.csv
ROSTER_CSV = Path(os.environ.get("BNSL_ROSTER_CSV", str(APP_DIR / "rostered_2025service_BNSL_arb_updated.csv")))

# Live roster DB is the source of truth for free agency eligibility.
# The OOTP export is only used to backfill missing unsigned players into roster.db.
OOTP_FA_ROSTER = Path(os.environ.get(
    "BNSL_OOTP_FA_ROSTER",
    str(APP_DIR / "bnsl_ootp27_fixed_rosters_oldids_optionsupdated.txt"),
))
HOMETOWN_DISCOUNTS_DB = Path(os.environ.get(
    "BNSL_HOMETOWN_DISCOUNTS_DB",
    str(APP_DIR / "hometown_discounts.db"),
))
ROSTER_DB_SYNC_TTL_SECONDS = int(os.environ.get("BNSL_ROSTER_DB_SYNC_TTL_SECONDS", "15"))
CURRENT_SEASON = 2025
CURRENT_FA_CLASS = str(CURRENT_SEASON + 1)

# To avoid reparsing a large roster on every request, sync if the file mtime changed
# or after this TTL. Set to 0 if you want every request to check the roster file.
ROSTER_SYNC_TTL_SECONDS = int(os.environ.get("BNSL_ROSTER_SYNC_TTL_SECONDS", "15"))

# Status values that still count as rostered even if active/expanded flags are false.
ROSTERED_STATUSES = {"ACTIVE", "RESERVE"}


HEADSHOT_DIR = APP_DIR / "static" / "player_images"
HEADSHOT_DIR.mkdir(parents=True, exist_ok=True)

# Map franchise abbreviations -> your full MLB_TEAMS strings.
ABBR_TO_TEAM = {
    "ARI":"Arizona Diamondbacks","ATL":"Atlanta Braves","BAL":"Baltimore Orioles","BOS":"Boston Red Sox",
    "CHC":"Chicago Cubs","CHW":"Chicago White Sox","CIN":"Cincinnati Reds","CLE":"Cleveland Guardians",
    "COL":"Colorado Rockies","DET":"Detroit Tigers","HOU":"Houston Astros","KCR":"Kansas City Royals",
    "LAA":"Los Angeles Angels","LAD":"Los Angeles Dodgers","MIA":"Miami Marlins","MIL":"Milwaukee Brewers",
    "MIN":"Minnesota Twins","NYM":"New York Mets","NYY":"New York Yankees","OAK":"Oakland Athletics",
    "PHI":"Philadelphia Phillies","PIT":"Pittsburgh Pirates","SDP":"San Diego Padres","SFG":"San Francisco Giants",
    "SEA":"Seattle Mariners","STL":"St. Louis Cardinals","TBR":"Tampa Bay Rays","TEX":"Texas Rangers",
    "TOR":"Toronto Blue Jays","WSN":"Washington Nationals",
}

# Common roster/export aliases that differ from the display-team dictionary above.
ABBR_TO_TEAM.update({
    "KC": "Kansas City Royals",
    "KCR": "Kansas City Royals",
    "SD": "San Diego Padres",
    "SDP": "San Diego Padres",
    "SF": "San Francisco Giants",
    "SFG": "San Francisco Giants",
    "TB": "Tampa Bay Rays",
    "TBR": "Tampa Bay Rays",
    "WAS": "Washington Nationals",
    "WSH": "Washington Nationals",
    "WSN": "Washington Nationals",
})

# Roster tab stores franchise as abbreviations, while FA tab uses full team names.
TEAM_TO_ABBR = {full: abbr for abbr, full in ABBR_TO_TEAM.items() if len(abbr) <= 3}
TEAM_TO_ABBR.update({
    "Kansas City Royals": "KC",
    "San Diego Padres": "SD",
    "San Francisco Giants": "SF",
    "Tampa Bay Rays": "TB",
    "Washington Nationals": "WAS",
    "Oakland Athletics": "OAK",
    "Athletics": "OAK",
})

OOTP_POSITION_MAP = {
    1: "P", 2: "C", 3: "1B", 4: "2B", 5: "3B", 6: "SS",
    7: "LF", 8: "CF", 9: "RF", 10: "DH", 11: "P", 12: "P", 13: "P",
}
OOTP_BT_MAP = {1: "R", 2: "L", 3: "S"}


# ---------- Blueprint ----------
fa_bp = Blueprint("fa", __name__)

@fa_bp.before_app_request
def _ensure_fa_db_dir_exists():
    # If DB_PATH was overridden in app config, ensure its parent exists too.
    p = get_db_path()
    p.parent.mkdir(parents=True, exist_ok=True)

logging.basicConfig(level=logging.INFO)

MLB_TEAMS = [
    "Arizona Diamondbacks","Atlanta Braves","Baltimore Orioles","Boston Red Sox",
    "Chicago Cubs","Chicago White Sox","Cincinnati Reds","Cleveland Guardians",
    "Colorado Rockies","Detroit Tigers","Houston Astros","Kansas City Royals",
    "Los Angeles Angels","Los Angeles Dodgers","Miami Marlins","Milwaukee Brewers",
    "Minnesota Twins","New York Mets","New York Yankees","Oakland Athletics",
    "Philadelphia Phillies","Pittsburgh Pirates","San Diego Padres","San Francisco Giants",
    "Seattle Mariners","St. Louis Cardinals","Tampa Bay Rays","Texas Rangers",
    "Toronto Blue Jays","Washington Nationals",
]

# Use the same mapping style as your draft app.
TEAM_EMAILS = {
    "Toronto Blue Jays": "daniele.defeo@gmail.com",
    "New York Yankees": "dmsund66@gmail.com",
    "Boston Red Sox": "chris_lawrence@sbcglobal.net",
    "Tampa Bay Rays": "smith.mark.louis@gmail.com",
    "Baltimore Orioles": "bsweis@ptd.net",

    "Detroit Tigers": "manconley@gmail.com",
    "Kansas City Royals": "jim@timhafer.com",
    "Minnesota Twins": "jonathan.adelman@gmail.com",
    "Chicago White Sox": "bglover6@gmail.com",
    "Cleveland Guardians": "bonfanti20@gmail.com",

    "Los Angeles Angels": "dsucoff@gmail.com",
    "Seattle Mariners": "daniel_a_fisher@yahoo.com",
    "Oakland Athletics": "bspropp@hotmail.com",
    "Houston Astros": "golk624@protonmail.com",
    "Texas Rangers": "Brianorr@live.com",

    "Washington Nationals": "smsetnor@gmail.com",
    "New York Mets": "kerkhoffc@gmail.com",
    "Philadelphia Phillies": "jdcarney26@gmail.com",
    "Atlanta Braves": "stevegaston@yahoo.com",
    "Miami Marlins": "schmitz@ucsb.edu",

    "St. Louis Cardinals": "parkbench@mac.com",
    "Chicago Cubs": "bryanhartman@gmail.com",
    "Pittsburgh Pirates": "jseiner24@gmail.com",
    "Milwaukee Brewers": "tsurratt@hiaspire.com",
    "Cincinnati Reds": "jpmile@yahoo.com",

    "Los Angeles Dodgers": "jr92@comcast.net",
    "Colorado Rockies": "GypsySon@gmail.com",
    "Arizona Diamondbacks": "mhr4240@gmail.com",
    "San Francisco Giants": "jasonmallet@gmail.com",
    "San Diego Padres": "mattaca77@gmail.com",
}

# ---------- Helpers ----------
def ensure_column(conn: sqlite3.Connection, table: str, col: str, coldef: str):
    """
    Add a column if it doesn't already exist.
    coldef example: "mlbam_id INTEGER"
    """
    cur = conn.cursor()
    cur.execute(f"PRAGMA table_info({table})")
    cols = {r[1] for r in cur.fetchall()}  # (cid, name, type, notnull, dflt, pk)
    if col not in cols:
        cur.execute(f"ALTER TABLE {table} ADD COLUMN {coldef}")
        conn.commit()


def utcnow() -> datetime:
    return datetime.now(timezone.utc)

def iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat(timespec="seconds")

def parse_iso(s: str) -> datetime:
    return datetime.fromisoformat(s)

def emails_equal(a: str | None, b: str | None) -> bool:
    if not a or not b:
        return False
    return a.strip().lower() == b.strip().lower()

def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(str(get_db_path()))
    conn.row_factory = sqlite3.Row

    def _unaccent(s):
        if s is None:
            return ""
        return "".join(ch for ch in unicodedata.normalize("NFKD", str(s)) if not unicodedata.combining(ch))
    conn.create_function("unaccent", 1, _unaccent)
    return conn

def _require_authed_team() -> str:
    team = session.get("authed_team")
    if not team:
        abort(401, "Not logged in")
    return team

def min_aav_millions(years: int, has_option: bool) -> float:
    """
    Minimum salary rules (AAV), in $M.
    Map by total years where option adds one potential year.
    """
    total = min(6, years + (2 if has_option else 0))
    mins = {
        1: 0.75,
        2: 1.25,
        3: 2.50,
        4: 5.00,
        5: 7.00,
        6: 10.00,
    }
    return float(mins.get(total, 999999.0))

def hometown_multiplier(hometown_seasons: int) -> float:
    if hometown_seasons >= 2:
        return 1.10
    if hometown_seasons >= 1:
        return 1.05
    return 1.00

def compute_bid_value_1yr_equiv(aav_m: float, years: int, has_option: bool, hm_mult: float) -> float:
    """
    Placeholder bid value model (units: $M "1-year equivalent").

    Constraints you wanted:
    - More AAV => higher value
    - More guaranteed years => higher value, diminishing returns
    - Option year => lower value than same guaranteed deal (so 2y @ 5 > 2y+opt @ 5)
    - 1y @ 10M > 2y @ 5M/yr
    """
    years = max(1, min(6, int(years)))
    aav_m = max(0.0, float(aav_m))

    # Diminishing returns on guaranteed years:
    # 1y: factor=1.0, 2y: ~1.65, 3y: ~2.03, 6y: ~2.68
    y_factor = 1.0 + 0.65 * math.log2(years)

    val = aav_m * y_factor

    # Option reduces certainty for player -> penalize bid value
    if has_option:
        val *= 0.90

    # Hometown multiplier applies to bid value (your "discount" wording)
    val *= float(hm_mult)

    # Round-ish for stable UI
    return float(val)

def fmt_money_m(x: float) -> str:
    return f"${x:.2f}M"

def clamp_int(x: Any, lo: int, hi: int, default: int) -> int:
    try:
        v = int(x)
        return max(lo, min(hi, v))
    except Exception:
        return default

def clamp_float(x: Any, lo: float, hi: float, default: float) -> float:
    try:
        v = float(x)
        if math.isnan(v) or math.isinf(v):
            return default
        return max(lo, min(hi, v))
    except Exception:
        return default


# ---------- DB init / import ----------
def init_db():
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS free_agents (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            position TEXT,
            last_team TEXT,
            hometown_team TEXT,
            hometown_seasons INTEGER NOT NULL DEFAULT 0,
            seed_qo INTEGER NOT NULL DEFAULT 0,

            signed_team TEXT,
            signed_at TEXT
        )
    """)

    # --- add registry columns to free_agents if missing ---
    ensure_column(conn, "free_agents", "mlbam_id", "mlbam_id INTEGER")
    ensure_column(conn, "free_agents", "fangraphs_id", "fangraphs_id INTEGER")
    ensure_column(conn, "free_agents", "fg_url", "fg_url TEXT")
    ensure_column(conn, "free_agents", "franchise_abbr", "franchise_abbr TEXT")
    ensure_column(conn, "free_agents", "htd", "htd INTEGER")  # mirror of HTD (0/1/2)

    # --- roster-sync columns: free_agents is now populated from the roster tab/CSV ---
    ensure_column(conn, "free_agents", "roster_player_id", "roster_player_id INTEGER")
    ensure_column(conn, "free_agents", "roster_source", "roster_source TEXT")
    ensure_column(conn, "free_agents", "is_roster_unrostered", "is_roster_unrostered INTEGER NOT NULL DEFAULT 0")
    ensure_column(conn, "free_agents", "last_seen_roster_sync", "last_seen_roster_sync TEXT")
    ensure_column(conn, "free_agents", "removed_from_roster_sync", "removed_from_roster_sync TEXT")
    cur.execute("""
        CREATE TABLE IF NOT EXISTS player_meta (
            mlbam_id INTEGER PRIMARY KEY,
            full_name TEXT,
            bats TEXT,
            throws TEXT,
            birth_date TEXT,
            height TEXT,
            weight INTEGER,
            headshot_local TEXT,
            updated_at TEXT
        )
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS free_agents_mlbam_idx ON free_agents(mlbam_id)")

    cur.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS free_agents_roster_player_id_uq
        ON free_agents(roster_player_id)
        WHERE roster_player_id IS NOT NULL
    """)
    cur.execute("""
        CREATE INDEX IF NOT EXISTS free_agents_roster_unrostered_idx
        ON free_agents(is_roster_unrostered)
    """)


    cur.execute("""
        CREATE TABLE IF NOT EXISTS bids (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            player_id INTEGER NOT NULL,
            team TEXT NOT NULL,

            years INTEGER NOT NULL,
            has_option INTEGER NOT NULL DEFAULT 0,
            aav_m REAL NOT NULL,

            bid_value_m REAL NOT NULL,            -- "1-year equivalent" bid value
            hometown_mult REAL NOT NULL DEFAULT 1.0,

            created_at TEXT NOT NULL,
            expires_at TEXT,                      -- only meaningful for current winning bid
            status TEXT NOT NULL DEFAULT 'OUTBID'  -- OUTBID, ACTIVE, SIGNED
        )
    """)

    cur.execute("""
        CREATE INDEX IF NOT EXISTS bids_player_created_idx
        ON bids(player_id, created_at)
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS watchlist (
            team TEXT NOT NULL,
            player_id INTEGER NOT NULL,
            created_at TEXT NOT NULL,
            UNIQUE(team, player_id) ON CONFLICT IGNORE
        )
    """)

    conn.commit()
    conn.close()

def db_is_empty() -> bool:
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM free_agents")
    n = int(cur.fetchone()[0] or 0)
    conn.close()
    return n == 0

def _clean_int(x: str, default: int = 0) -> int:
    try:
        return int(str(x).strip())
    except Exception:
        return default

def _clean_text(x: str) -> str:
    return (x or "").strip()

def import_player_registry_csv(path: Path):
    """
    Expected headers (case-insensitive):
      player,FG_URL,position,fangraphs_id,mlbam_id,franchise_abbr,HTD
    """
    if not path.exists():
        return

    conn = get_conn()
    cur = conn.cursor()

    with path.open(newline="", encoding="utf-8") as f:
        r = csv.DictReader(f)
        for row in r:
            # tolerate header case differences
            name = _clean_text(row.get("player") or row.get("Player") or row.get("name") or row.get("Name"))
            if not name:
                continue

            fg_url = _clean_text(row.get("FG_URL") or row.get("fg_url") or row.get("Fg_Url") or row.get("url") or row.get("URL"))
            pos = _clean_text(row.get("position") or row.get("Position"))
            fg_id = _clean_int(row.get("fangraphs_id") or row.get("FanGraphs_ID") or row.get("FG_ID") or 0, 0)
            mlbam_id = _clean_int(row.get("mlbam_id") or row.get("MLBAM_ID") or 0, 0)
            abbr = _clean_text(row.get("franchise_abbr") or row.get("Franchise_Abbr") or "").upper()
            htd = clamp_int(row.get("HTD") or row.get("htd") or 0, 0, 2, 0)

            # Map hometown franchise abbr -> full team name
            hometown_team = ABBR_TO_TEAM.get(abbr, "")

            # Your existing hometown logic uses hometown_seasons (0/1/2) -> 0/5/10%
            hometown_seasons = htd

            # Prefer to upsert by mlbam_id if present, else by fangraphs_id
            existing_id = None
            if mlbam_id > 0:
                cur.execute("SELECT id FROM free_agents WHERE mlbam_id=?", (mlbam_id,))
                x = cur.fetchone()
                existing_id = int(x["id"]) if x else None

            if existing_id is None and fg_id > 0:
                cur.execute("SELECT id FROM free_agents WHERE fangraphs_id=?", (fg_id,))
                x = cur.fetchone()
                existing_id = int(x["id"]) if x else None

            if existing_id is None:
                # 11 columns => 11 placeholders => 11 values
                cur.execute("""
                    INSERT INTO free_agents(
                      name, position, last_team, hometown_team, hometown_seasons, seed_qo,
                      mlbam_id, fangraphs_id, fg_url, franchise_abbr, htd
                    )
                    VALUES(?,?,?,?,?,?,?,?,?,?,?)
                """, (
                    name,
                    pos,
                    "",                      # last_team
                    hometown_team,
                    hometown_seasons,
                    0,                       # seed_qo
                    (mlbam_id if mlbam_id > 0 else None),
                    (fg_id if fg_id > 0 else None),
                    (fg_url or None),
                    (abbr or None),
                    htd,
                ))
            else:
                cur.execute("""
                    UPDATE free_agents
                    SET name=?,
                        position=?,
                        hometown_team=?,
                        hometown_seasons=?,
                        mlbam_id=COALESCE(?, mlbam_id),
                        fangraphs_id=COALESCE(?, fangraphs_id),
                        fg_url=COALESCE(?, fg_url),
                        franchise_abbr=COALESCE(?, franchise_abbr),
                        htd=COALESCE(?, htd)
                    WHERE id=?
                """, (
                    name,
                    pos,
                    hometown_team,
                    hometown_seasons,
                    (mlbam_id if mlbam_id > 0 else None),
                    (fg_id if fg_id > 0 else None),
                    (fg_url or None),
                    (abbr or None),
                    htd,
                    existing_id
                ))

    conn.commit()
    conn.close()



def mlb_headshot_url(mlbam_id: int) -> str:
    # Widely used pattern (works for most modern players)
    return (
        "https://img.mlbstatic.com/mlb-photos/image/upload/"
        "w_213,d_people:generic:headshot:silo:current.png,q_auto:best,f_auto/"
        f"v1/people/{mlbam_id}/headshot/67/current"
    )

def legacy_headshot_url(mlbam_id: int) -> str:
    # Older endpoint that redirects to current location
    return f"https://securea.mlb.com/mlb/images/players/head_shot/{mlbam_id}.jpg"

def ensure_headshot_cached(mlbam_id: int) -> str | None:
    """
    Returns local filesystem path string if cached/created, else None.
    """
    if mlbam_id <= 0:
        return None

    out = HEADSHOT_DIR / f"{mlbam_id}.png"
    if out.exists() and out.stat().st_size > 1024:
        return str(out)

    sess = requests.Session()
    headers = {"User-Agent": "Mozilla/5.0"}

    # Try primary URL
    for url in (mlb_headshot_url(mlbam_id), legacy_headshot_url(mlbam_id)):
        try:
            r = sess.get(url, headers=headers, timeout=20, allow_redirects=True)
            if r.status_code == 200 and r.content and len(r.content) > 1024:
                # If it's jpg from legacy, still save as .png filename is fine for browsers if content-type differs,
                # but to be clean you can keep .jpg; we keep .png just for uniformity.
                out.write_bytes(r.content)
                return str(out)
        except Exception:
            pass

    return None


def generate_sample_free_agents_csv(path: Path):
    if path.exists():
        return
    # Fun demo list: iconic historical MLB names
    sample = [
        ["name","position","last_team","hometown_team","hometown_seasons","seed_qo"],
        ["Babe Ruth","OF","New York Yankees","New York Yankees","2","1"],
        ["Jackie Robinson","2B","Brooklyn Dodgers","Los Angeles Dodgers","2","1"],
        ["Willie Mays","OF","San Francisco Giants","San Francisco Giants","2","1"],
        ["Hank Aaron","OF","Atlanta Braves","Atlanta Braves","2","1"],
        ["Ted Williams","OF","Boston Red Sox","Boston Red Sox","2","1"],
        ["Sandy Koufax","SP","Los Angeles Dodgers","Los Angeles Dodgers","2","1"],
        ["Nolan Ryan","SP","Texas Rangers","Texas Rangers","2","0"],
        ["Cal Ripken Jr.","SS","Baltimore Orioles","Baltimore Orioles","2","0"],
        ["Ken Griffey Jr.","OF","Seattle Mariners","Seattle Mariners","2","0"],
        ["Mariano Rivera","RP","New York Yankees","New York Yankees","2","0"],
        ["Albert Pujols","1B","St. Louis Cardinals","St. Louis Cardinals","2","0"],
        ["Ichiro Suzuki","OF","Seattle Mariners","Seattle Mariners","2","0"],
        ["Pedro Martinez","SP","Boston Red Sox","Boston Red Sox","2","0"],
        ["Roberto Clemente","OF","Pittsburgh Pirates","Pittsburgh Pirates","2","0"],
        ["Greg Maddux","SP","Atlanta Braves","Atlanta Braves","1","0"],
        ["Tony Gwynn","OF","San Diego Padres","San Diego Padres","2","0"],
        ["Barry Bonds","OF","San Francisco Giants","San Francisco Giants","2","0"],
    ]
    with path.open("w", newline="", encoding="utf-8") as f:
        csv.writer(f).writerows(sample)

def import_free_agents_csv(path: Path):
    if not path.exists():
        return
    conn = get_conn()
    cur = conn.cursor()
    with path.open(newline="", encoding="utf-8") as f:
        r = csv.DictReader(f)
        for row in r:
            name = (row.get("name") or row.get("Name") or "").strip()
            if not name:
                continue
            pos = (row.get("position") or row.get("Position") or "").strip()
            last_team = (row.get("last_team") or row.get("Last Team") or "").strip()
            hometown_team = (row.get("hometown_team") or row.get("Hometown Team") or "").strip()
            hs = clamp_int(row.get("hometown_seasons") or row.get("Hometown Seasons") or 0, 0, 2, 0)
            seed_qo = clamp_int(row.get("seed_qo") or row.get("Seed QO") or 0, 0, 1, 0)
            cur.execute("""
                INSERT INTO free_agents(name, position, last_team, hometown_team, hometown_seasons, seed_qo)
                VALUES(?,?,?,?,?,?)
            """, (name, pos, last_team, hometown_team, hs, seed_qo))
    conn.commit()
    conn.close()


# ---------- Roster-source sync for Free Agency tab ----------
_last_roster_sync_signature: tuple[float | None, float | None] | None = None
_last_roster_sync_checked_at: float = 0.0


def get_roster_db_path() -> Path:
    if has_app_context():
        cfg = current_app.config.get("ROSTER_DB_PATH")
        if cfg:
            return Path(cfg)
    return APP_DIR / "roster.db"


def get_ootp_fa_roster_path() -> Path:
    if has_app_context():
        cfg = current_app.config.get("OOTP_FA_ROSTER_PATH")
        if cfg:
            return Path(cfg)
    return OOTP_FA_ROSTER


def get_hometown_discounts_db_path() -> Path:
    if has_app_context():
        cfg = current_app.config.get("HOMETOWN_DISCOUNTS_DB_PATH")
        if cfg:
            return Path(cfg)
    return HOMETOWN_DISCOUNTS_DB


def get_roster_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(str(get_roster_db_path()))
    conn.row_factory = sqlite3.Row
    return conn


def ensure_roster_column(conn: sqlite3.Connection, col: str, coldef: str) -> None:
    cur = conn.cursor()
    cur.execute("PRAGMA table_info(roster_players)")
    cols = {r[1] for r in cur.fetchall()}
    if col not in cols:
        cur.execute(f"ALTER TABLE roster_players ADD COLUMN {coldef}")
        conn.commit()


def ensure_roster_fa_import_columns(conn: sqlite3.Connection) -> None:
    ensure_roster_column(conn, "bbref_id", "bbref_id TEXT")
    ensure_roster_column(conn, "bbrefminors_id", "bbrefminors_id TEXT")
    ensure_roster_column(conn, "ootp_id", "ootp_id INTEGER")
    ensure_roster_column(conn, "ootp_fa_imported_at", "ootp_fa_imported_at TEXT")
    ensure_roster_column(conn, "ootp_fa_last_seen", "ootp_fa_last_seen TEXT")


def _row_get_ci(row: Dict[str, str], *names: str) -> str:
    """Case-insensitive CSV row getter."""
    if not row:
        return ""
    lower_map = {str(k).strip().lower(): v for k, v in row.items()}
    for name in names:
        v = lower_map.get(str(name).strip().lower())
        if v is not None:
            return str(v).strip()
    return ""


def _parse_positive_int(value: Any) -> int | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        val = int(float(text))
        return val if val > 0 else None
    except Exception:
        return None


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(float(str(value or "").strip()))
    except Exception:
        return default


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(str(value or "").strip())
    except Exception:
        return default


def _truthy_text(value: Any) -> bool:
    return str(value or "").strip().upper() in {"TRUE", "T", "YES", "Y", "1"}


def roster_code_to_team(value: Any) -> str:
    code = str(value or "").strip().upper()
    if not code:
        return ""

    # Accept "Arizona Diamondbacks (ARI)" too.
    m = re.search(r"\(([A-Z]{2,3})\)", code)
    if m:
        code = m.group(1)

    code = re.sub(r"[^A-Z0-9]", "", code)
    return ABBR_TO_TEAM.get(code, code)


def _norm_token(value: Any) -> str:
    text = "".join(
        ch for ch in unicodedata.normalize("NFKD", str(value or ""))
        if not unicodedata.combining(ch)
    ).lower()
    return re.sub(r"[^a-z0-9]", "", text)


def _split_name(name: str) -> tuple[str, str]:
    bits = [b for b in re.split(r"\s+", (name or "").strip()) if b]
    if not bits:
        return "", ""
    if len(bits) == 1:
        return "", bits[0]
    return bits[0], bits[-1]


def _dob_from_ootp(day: Any, month: Any, year: Any) -> str:
    y = _safe_int(year, 0)
    m = _safe_int(month, 0)
    d = _safe_int(day, 0)
    if y <= 0 or not (1 <= m <= 12) or not (1 <= d <= 31):
        return ""
    return f"{y:04d}-{m:02d}-{d:02d}"


def _service_years_from_ootp(value: Any) -> float:
    # OOTP exports ML service as days. 172 days is the usual MLB service-year divisor.
    days = _safe_float(value, 0.0)
    return round(days / 172.0, 3) if days > 0 else 0.0


def _player_match_keys(
    first_name: Any,
    last_name: Any,
    dob: Any,
    team: Any,
    bbref_id: Any,
    bbrefminors_id: Any,
    ootp_id: Any,
) -> list[str]:
    """
    Ordered exactly like the requested cross-file identity fallback:
      1. bbrefidLASTNAME
      2. bbrefminorsidLASTNAME
      3. ootpidLASTNAME
      4. firstnameLASTNAMEteam
      5. firstnameLASTNAMEDOB
      6. LASTNAMEDOB
      7. firstnameLASTNAME
    """
    first = _norm_token(first_name)
    last = _norm_token(last_name)
    birth = _norm_token(dob)
    tm = _norm_token(team)
    bbref = _norm_token(bbref_id)
    bbrefm = _norm_token(bbrefminors_id)
    oid = _norm_token(ootp_id)

    if not last:
        return []

    keys: list[str] = []
    if bbref:
        keys.append(f"bbref:{bbref}:{last}")
    if bbrefm:
        keys.append(f"bbrefminors:{bbrefm}:{last}")
    if oid:
        keys.append(f"ootp:{oid}:{last}")
    if first and tm:
        keys.append(f"first_last_team:{first}:{last}:{tm}")
    if first and birth:
        keys.append(f"first_last_dob:{first}:{last}:{birth}")
    if birth:
        keys.append(f"last_dob:{last}:{birth}")
    if first:
        keys.append(f"first_last:{first}:{last}")
    return keys


def _roster_row_match_keys(row: sqlite3.Row) -> list[str]:
    first = row["first_name"] if "first_name" in row.keys() else ""
    last = row["last_name"] if "last_name" in row.keys() else ""
    if not first or not last:
        f2, l2 = _split_name(row["name"] if "name" in row.keys() else "")
        first = first or f2
        last = last or l2
    bbref = row["bbref_id"] if "bbref_id" in row.keys() else ""
    bbrefm = row["bbrefminors_id"] if "bbrefminors_id" in row.keys() else ""
    ootp_id = row["ootp_id"] if "ootp_id" in row.keys() else (row["id"] if "id" in row.keys() else "")
    return _player_match_keys(
        first,
        last,
        row["date_of_birth"] if "date_of_birth" in row.keys() else "",
        row["franchise"] if "franchise" in row.keys() else "",
        bbref,
        bbrefm,
        ootp_id,
    )


def _htd_row_value(row: sqlite3.Row | None, key: str, default: Any = "") -> Any:
    if row is None:
        return default
    return row[key] if key in row.keys() else default


def _htd_candidate_keys(fa_row: sqlite3.Row, roster_row: sqlite3.Row | None) -> list[str]:
    """Build match keys for a free_agents row against hometown_discounts.db."""
    name = str(_htd_row_value(roster_row, "name") or fa_row["name"] or "").strip()
    first = str(_htd_row_value(roster_row, "first_name") or "").strip()
    last = str(_htd_row_value(roster_row, "last_name") or "").strip()
    if not first or not last:
        f2, l2 = _split_name(name)
        first = first or f2
        last = last or l2

    first_n = _norm_token(first)
    last_n = _norm_token(last)
    if not last_n:
        return []

    keys: list[str] = []

    def add(key: str) -> None:
        if key and key not in keys:
            keys.append(key)

    # Exact/cross aliases for bbref-ish IDs.  The BNSL/OOTP files sometimes put
    # the same string under bbref_id, bbrefminors_id, or OOTP pID labels.
    for raw in (
        _htd_row_value(roster_row, "bbref_id"),
        _htd_row_value(roster_row, "bbrefminors_id"),
    ):
        val = _norm_token(raw)
        if val:
            add(f"bbref:{val}:{last_n}")
            add(f"bbrefminors:{val}:{last_n}")

    for raw in (
        _htd_row_value(roster_row, "ootp_id"),
        _htd_row_value(roster_row, "bbrefminors_id"),
    ):
        val = _norm_token(raw)
        if val:
            add(f"ootp:{val}:{last_n}")

    # Team-based key is a fallback.  It helps if the row still has last_team or
    # franchise populated, but current FAs may have those cleared by roster import.
    team_candidates = []
    if "last_team" in fa_row.keys():
        team_candidates.append(fa_row["last_team"])
    team_candidates.append(_htd_row_value(roster_row, "franchise"))
    for team_value in team_candidates:
        team_text = roster_code_to_team(team_value)
        team_norm = _norm_token(team_text)
        raw_norm = _norm_token(team_value)
        if first_n and team_norm:
            add(f"first_last_team:{first_n}:{last_n}:{team_norm}")
        if first_n and raw_norm:
            add(f"first_last_team:{first_n}:{last_n}:{raw_norm}")

    if first_n:
        add(f"first_last:{first_n}:{last_n}")

    return keys


def _load_hometown_discount_key_map(path: Path) -> dict[str, dict[str, Any]]:
    if not path.exists():
        return {}
    conn = sqlite3.connect(str(path))
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    cur.execute("""
        SELECT k.key, k.rank, k.method,
               d.id AS discount_id, d.full_name, d.team_abbr, d.team_name,
               d.hometown_seasons, d.multiplier, d.stats_player_id
        FROM hometown_discount_keys k
        JOIN hometown_discounts d ON d.id = k.discount_id
    """)
    rows = {str(r["key"]): dict(r) for r in cur.fetchall()}
    conn.close()
    return rows


def apply_hometown_discounts_to_free_agents(clear_missing: bool = True) -> tuple[int, int, int]:
    """
    Apply precomputed HTD claims from hometown_discounts.db to fa.db/free_agents.

    This is deliberately cheap: it reads the compressed key table, not the large
    batting/pitching logs.  The bid code already uses hometown_team and
    hometown_seasons to apply the 1.05x/1.10x value multiplier.

    Returns: (checked_unsigned_fas, matched, cleared)
    """
    htd_path = get_hometown_discounts_db_path()
    key_map = _load_hometown_discount_key_map(htd_path)
    if not key_map:
        return (0, 0, 0)

    conn = get_conn()
    cur = conn.cursor()
    ensure_column(conn, "free_agents", "franchise_abbr", "franchise_abbr TEXT")
    ensure_column(conn, "free_agents", "htd", "htd INTEGER")

    cur.execute("""
        SELECT *
        FROM free_agents
        WHERE COALESCE(is_roster_unrostered, 1)=1
          AND (signed_team IS NULL OR signed_team='')
    """)
    fa_rows = cur.fetchall()

    roster_rows: dict[int, sqlite3.Row | None] = {}
    roster_conn = None
    roster_path = get_roster_db_path()
    if roster_path.exists():
        roster_conn = get_roster_conn()

    checked = len(fa_rows)
    matched = 0
    cleared = 0

    for fa_row in fa_rows:
        roster_row = None
        roster_id = _safe_int(fa_row["roster_player_id"] if "roster_player_id" in fa_row.keys() else 0, 0)
        if roster_id and roster_conn is not None:
            if roster_id not in roster_rows:
                roster_rows[roster_id] = roster_conn.execute(
                    "SELECT * FROM roster_players WHERE id=?",
                    (roster_id,),
                ).fetchone()
            roster_row = roster_rows[roster_id]

        match = None
        for key in _htd_candidate_keys(fa_row, roster_row):
            if key in key_map:
                match = key_map[key]
                break

        if match:
            matched += 1
            cur.execute("""
                UPDATE free_agents
                SET hometown_team=?, hometown_seasons=?, franchise_abbr=?, htd=?
                WHERE id=?
            """, (
                match["team_name"],
                int(match["hometown_seasons"] or 0),
                match["team_abbr"],
                int(match["hometown_seasons"] or 0),
                int(fa_row["id"]),
            ))
        elif clear_missing:
            cleared += 1
            cur.execute("""
                UPDATE free_agents
                SET hometown_team='', hometown_seasons=0, franchise_abbr=NULL, htd=0
                WHERE id=?
            """, (int(fa_row["id"]),))

    if roster_conn is not None:
        roster_conn.close()
    conn.commit()
    conn.close()

    logging.info(
        "HTD sync complete from %s: %s checked, %s matched, %s cleared",
        htd_path, checked, matched, cleared,
    )
    return (checked, matched, cleared)


def _ootp_get(headers: list[str], values: list[str], name: str) -> str:
    target = name.strip().lower()
    for i, h in enumerate(headers):
        if h.strip().lower() == target and i < len(values):
            return str(values[i]).strip()
    return ""


def _iter_ootp_export_rows(path: Path):
    """Yield (headers, values) rows from the OOTP text export."""
    headers: list[str] = []
    with Path(path).open(newline="", encoding="utf-8-sig") as f:
        for raw in f:
            line = raw.strip()
            if not line:
                continue
            if line.startswith("//"):
                comment = line[2:].strip()
                if comment.lower().startswith("id,") and "lastname" in comment.lower():
                    headers = [h.strip() for h in next(csv.reader([comment], skipinitialspace=True))]
                continue
            if not headers:
                continue
            values = [v.strip() for v in next(csv.reader([line], skipinitialspace=True))]
            yield headers, values


def _is_ootp_unsigned_free_agent(headers: list[str], values: list[str]) -> bool:
    if _safe_int(_ootp_get(headers, values, "del"), 0) != 0:
        return False
    team_id = _safe_int(_ootp_get(headers, values, "team_id"), 0)
    team_name = _ootp_get(headers, values, "Team Name").strip()
    return team_id == 0 and team_name in {"", "-"}


def import_ootp_free_agents_into_roster_db(path: Path | None = None) -> tuple[int, int]:
    """
    Backfill unsigned OOTP27 free agents into roster.db.

    Existing roster_players are matched before inserting, using the requested
    lastname-guarded ID order. Existing rostered players are never released here;
    this only inserts missing unsigned players as unrostered 2026 free agents.
    """
    path = Path(path or get_ootp_fa_roster_path())
    if not path.exists():
        logging.warning("OOTP FA import skipped: export does not exist: %s", path)
        return (0, 0)

    now_text = iso(utcnow())
    conn = get_roster_conn()
    cur = conn.cursor()
    ensure_roster_fa_import_columns(conn)

    cur.execute("SELECT * FROM roster_players")
    roster_rows = cur.fetchall()

    key_to_id: dict[str, int] = {}
    duplicate_keys: set[str] = set()
    for r in roster_rows:
        rid = int(r["id"])
        for key in _roster_row_match_keys(r):
            if key in key_to_id and key_to_id[key] != rid:
                duplicate_keys.add(key)
            else:
                key_to_id[key] = rid

    cur.execute("SELECT COALESCE(MAX(id), 0) FROM roster_players")
    next_id = int(cur.fetchone()[0] or 0) + 1

    seen = 0
    inserted = 0

    for headers, values in _iter_ootp_export_rows(path):
        if not _is_ootp_unsigned_free_agent(headers, values):
            continue
        seen += 1

        ootp_id = _safe_int(_ootp_get(headers, values, "id"), 0)
        first = _ootp_get(headers, values, "FirstName")
        last = _ootp_get(headers, values, "LastName")
        if not last:
            continue
        name = f"{first} {last}".strip()
        dob = _dob_from_ootp(
            _ootp_get(headers, values, "DayOB"),
            _ootp_get(headers, values, "MonthOB"),
            _ootp_get(headers, values, "YearOB"),
        )
        bbref = _ootp_get(headers, values, "bbref_id")
        bbrefm = _ootp_get(headers, values, "bbrefminors_id")
        team_name = _ootp_get(headers, values, "Team Name")

        match_id = None
        for key in _player_match_keys(first, last, dob, team_name, bbref, bbrefm, ootp_id):
            if key in duplicate_keys:
                continue
            found = key_to_id.get(key)
            if found is not None:
                match_id = found
                break

        if match_id is not None:
            cur.execute("""
                UPDATE roster_players
                SET bbref_id=COALESCE(NULLIF(bbref_id, ''), ?),
                    bbrefminors_id=COALESCE(NULLIF(bbrefminors_id, ''), ?),
                    ootp_id=COALESCE(ootp_id, ?),
                    ootp_fa_last_seen=?
                WHERE id=?
            """, (bbref or None, bbrefm or None, ootp_id or None, now_text, match_id))
            continue

        insert_id = ootp_id if ootp_id > 0 else next_id
        cur.execute("SELECT 1 FROM roster_players WHERE id=?", (insert_id,))
        if cur.fetchone():
            insert_id = next_id
        next_id = max(next_id, insert_id + 1)

        position = OOTP_POSITION_MAP.get(_safe_int(_ootp_get(headers, values, "Position"), 0), "")
        bats = OOTP_BT_MAP.get(_safe_int(_ootp_get(headers, values, "Bats"), 0), "")
        throws = OOTP_BT_MAP.get(_safe_int(_ootp_get(headers, values, "Throws"), 0), "")
        service_time = _service_years_from_ootp(_ootp_get(headers, values, "ML Service"))
        options_remaining = max(0, 3 - _safe_int(_ootp_get(headers, values, "options used"), 0))

        cur.execute("""
            INSERT INTO roster_players (
                id, name, last_name, first_name, suffix, nickname,
                position, date_of_birth, bats, throws, signed,
                contract_type, salary, contract_initial_season,
                contract_length, contract_option, contract_expires,
                service_time, previous_service_time, service_time_2025,
                franchise, affiliate_team, roster_status, active_roster,
                options_remaining, fa_class, fangraphs_id, mlbam_id,
                bbref_id, bbrefminors_id, ootp_id,
                ootp_fa_imported_at, ootp_fa_last_seen
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            insert_id, name, last, first, "", _ootp_get(headers, values, "NickName"),
            position, dob, bats, throws, 0,
            "FA", 0.0, None,
            None, 0, "",
            service_time, service_time, 0.0,
            "", "", "", 0,
            options_remaining, CURRENT_FA_CLASS, "", None,
            bbref or "", bbrefm or "", ootp_id or None,
            now_text, now_text,
        ))
        inserted += 1

        # Make the newly inserted player visible to later rows in the same import.
        fake = {
            "id": insert_id,
            "name": name,
            "first_name": first,
            "last_name": last,
            "date_of_birth": dob,
            "franchise": "",
            "bbref_id": bbref,
            "bbrefminors_id": bbrefm,
            "ootp_id": ootp_id,
        }
        for key in _player_match_keys(first, last, dob, "", bbref, bbrefm, ootp_id):
            if key in key_to_id and key_to_id[key] != insert_id:
                duplicate_keys.add(key)
            else:
                key_to_id[key] = insert_id

    conn.commit()
    conn.close()
    logging.info("OOTP FA import complete: %s unsigned rows seen, %s inserted into roster.db", seen, inserted)
    return (seen, inserted)


def _roster_db_row_is_unrostered(row: sqlite3.Row) -> bool:
    franchise = str(row["franchise"] or "").strip()
    roster_status = str(row["roster_status"] or "").strip()
    signed = _safe_int(row["signed"] if "signed" in row.keys() else 0, 0)
    return (franchise == "") or (signed == 0) or (roster_status == "")


def _find_existing_free_agent_id(cur: sqlite3.Cursor, roster_id: int | None, mlbam_id: int | None, fg_id: int | None, name: str) -> int | None:
    if roster_id is not None:
        cur.execute("SELECT id FROM free_agents WHERE roster_player_id=?", (roster_id,))
        r = cur.fetchone()
        if r:
            return int(r["id"])

    if mlbam_id is not None:
        cur.execute("SELECT id FROM free_agents WHERE mlbam_id=?", (mlbam_id,))
        r = cur.fetchone()
        if r:
            return int(r["id"])

    if fg_id is not None:
        cur.execute("SELECT id FROM free_agents WHERE fangraphs_id=?", (fg_id,))
        r = cur.fetchone()
        if r:
            return int(r["id"])

    # Last fallback: exact unaccented name match. This is intentionally last because
    # duplicate names are possible, but it helps preserve old seeded DB rows.
    if name:
        cur.execute("SELECT id FROM free_agents WHERE LOWER(unaccent(name)) = LOWER(unaccent(?)) LIMIT 1", (name,))
        r = cur.fetchone()
        if r:
            return int(r["id"])

    return None


def sync_free_agents_from_roster_db() -> tuple[int, int]:
    """
    Mirror currently unrostered roster.db players into fa.db/free_agents.

    roster.db remains the eligibility source of truth. fa.db only stores bidding,
    watchlist, and signed-state metadata keyed back to roster_players.id.
    """
    roster_path = get_roster_db_path()
    if not roster_path.exists():
        logging.warning("Roster DB sync skipped: roster.db does not exist: %s", roster_path)
        return (0, 0)

    now_text = iso(utcnow())
    rconn = get_roster_conn()
    rcur = rconn.cursor()
    ensure_roster_fa_import_columns(rconn)
    rcur.execute("SELECT * FROM roster_players")
    roster_rows = [r for r in rcur.fetchall() if _roster_db_row_is_unrostered(r)]
    rconn.close()

    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
        UPDATE free_agents
        SET is_roster_unrostered=0,
            removed_from_roster_sync=?
        WHERE roster_source IN ('roster_db', 'roster_csv', 'ootp27')
    """, (now_text,))

    upserted = 0
    for r in roster_rows:
        roster_id = int(r["id"])
        mlbam_id = _parse_positive_int(r["mlbam_id"] if "mlbam_id" in r.keys() else None)
        fg_id = _parse_positive_int(r["fangraphs_id"] if "fangraphs_id" in r.keys() else None)
        name = str(r["name"] or "").strip()
        if not name:
            continue
        pos = str(r["position"] or "").strip()
        last_team = roster_code_to_team(r["franchise"] if "franchise" in r.keys() else "")

        existing_id = _find_existing_free_agent_id(cur, roster_id, mlbam_id, fg_id, name)

        if existing_id is None:
            cur.execute("""
                INSERT INTO free_agents(
                    name, position, last_team,
                    hometown_team, hometown_seasons, seed_qo,
                    mlbam_id, fangraphs_id,
                    roster_player_id, roster_source, is_roster_unrostered,
                    last_seen_roster_sync, removed_from_roster_sync
                )
                VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)
            """, (
                name, pos, last_team,
                "", 0, 0,
                mlbam_id, fg_id,
                roster_id, "roster_db", 1,
                now_text, None,
            ))
        else:
            cur.execute("""
                UPDATE free_agents
                SET name=?,
                    position=COALESCE(NULLIF(?, ''), position),
                    last_team=COALESCE(NULLIF(?, ''), last_team),
                    mlbam_id=COALESCE(?, mlbam_id),
                    fangraphs_id=COALESCE(?, fangraphs_id),
                    roster_player_id=?,
                    roster_source='roster_db',
                    is_roster_unrostered=1,
                    last_seen_roster_sync=?,
                    removed_from_roster_sync=NULL
                WHERE id=?
            """, (
                name, pos, last_team, mlbam_id, fg_id,
                roster_id, now_text, existing_id,
            ))
        upserted += 1

    conn.commit()
    conn.close()
    logging.info("Roster DB FA sync complete: %s unrostered, %s upserted", len(roster_rows), upserted)
    return (len(roster_rows), upserted)


def update_roster_db_for_fa_signing(roster_player_id: int | None, team: str, years: int, has_option: bool, aav_m: float) -> None:
    if not roster_player_id:
        return

    team_abbr = TEAM_TO_ABBR.get(team, team)
    start_year = int(CURRENT_FA_CLASS)
    total_years = max(1, int(years)) + (1 if has_option else 0)
    contract_expires = start_year + total_years - 1
    new_fa_class = str(contract_expires + 1)

    conn = get_roster_conn()
    cur = conn.cursor()
    ensure_roster_fa_import_columns(conn)
    cur.execute("""
        UPDATE roster_players
        SET signed=1,
            contract_type='FA',
            salary=?,
            contract_initial_season=?,
            contract_length=?,
            contract_option=?,
            contract_expires=?,
            franchise=?,
            affiliate_team='',
            roster_status='Reserve',
            active_roster=0,
            fa_class=?
        WHERE id=?
    """, (
        float(aav_m) * 1_000_000.0,
        start_year,
        total_years,
        1 if has_option else 0,
        str(contract_expires),
        team_abbr,
        new_fa_class,
        int(roster_player_id),
    ))
    conn.commit()
    conn.close()


def sync_free_agents_from_roster_if_needed(force: bool = False) -> None:
    """
    Explicit maintenance sync:
      1) import missing unsigned OOTP export players into roster.db,
      2) mirror live unrostered roster.db players into fa.db.

    Do not call this from normal FA page/API reads. It scans thousands of
    players and may update roster.db metadata, so it belongs at startup, in a
    CLI maintenance command, or in a future roster release/do-not-renew action.
    """
    global _last_roster_sync_signature, _last_roster_sync_checked_at

    roster_path = get_roster_db_path()
    if not roster_path.exists():
        return

    ootp_path = get_ootp_fa_roster_path()
    now = time.time()

    try:
        roster_mtime = roster_path.stat().st_mtime
    except OSError:
        return
    try:
        ootp_mtime = ootp_path.stat().st_mtime if ootp_path.exists() else None
    except OSError:
        ootp_mtime = None

    signature = (roster_mtime, ootp_mtime)
    ttl_expired = (ROSTER_DB_SYNC_TTL_SECONDS <= 0) or ((now - _last_roster_sync_checked_at) >= ROSTER_DB_SYNC_TTL_SECONDS)
    changed = (_last_roster_sync_signature is None) or (signature != _last_roster_sync_signature)

    if force or changed or ttl_expired:
        if ootp_path.exists():
            import_ootp_free_agents_into_roster_db(ootp_path)
        sync_free_agents_from_roster_db()
        # Do not apply hometown discounts here. HTD assignment is an explicit
        # maintenance action only; normal FA searches/previews/bids should only
        # read already-stored hometown_team/hometown_seasons values.
        try:
            roster_mtime = roster_path.stat().st_mtime
        except OSError:
            pass
        _last_roster_sync_signature = (roster_mtime, ootp_mtime)
        _last_roster_sync_checked_at = now

def seed_qualifying_offers(qo_aav_m: float = 21.2):
    """
    Seed players with seed_qo=1 with an opening QO bid:
    - 1 year
    - no option
    - AAV = qo_aav_m
    - bidder team = last_team (or hometown_team if last_team missing)
    - 48-hour timer starts immediately
    """
    now = utcnow()
    exp = now + timedelta(hours=48)

    conn = get_conn()
    cur = conn.cursor()

    # If already have any bids, do not reseed
    cur.execute("SELECT COUNT(*) FROM bids")
    if int(cur.fetchone()[0] or 0) > 0:
        conn.close()
        return

    cur.execute("""
        SELECT id, last_team, hometown_team, hometown_seasons
        FROM free_agents
        WHERE seed_qo=1 AND (signed_team IS NULL OR signed_team='')
    """)
    rows = cur.fetchall()

    for r in rows:
        pid = int(r["id"])
        team = (r["last_team"] or r["hometown_team"] or "").strip()
        if not team:
            continue
        hm_mult = 1.0
        # Apply hometown multiplier if the bidding team matches hometown_team
        if (r["hometown_team"] or "").strip() and team == (r["hometown_team"] or "").strip():
            hm_mult = hometown_multiplier(int(r["hometown_seasons"] or 0))
        bid_val = compute_bid_value_1yr_equiv(qo_aav_m, years=1, has_option=False, hm_mult=hm_mult)

        # Mark any existing as outbid (should be none)
        cur.execute("UPDATE bids SET status='OUTBID', expires_at=NULL WHERE player_id=? AND status='ACTIVE'", (pid,))

        cur.execute("""
            INSERT INTO bids(player_id, team, years, has_option, aav_m, bid_value_m, hometown_mult, created_at, expires_at, status)
            VALUES(?,?,?,?,?,?,?,?,?, 'ACTIVE')
        """, (pid, team, 1, 0, float(qo_aav_m), float(bid_val), float(hm_mult), iso(now), iso(exp)))

    conn.commit()
    conn.close()


# ---------- Core state queries ----------
def enforce_expirations():
    """
    Auto-sign any player whose ACTIVE bid has expired.
    Called opportunistically on page/API hits.
    """
    now = utcnow()
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
        SELECT
          b.id AS bid_id, b.player_id, b.team, b.expires_at,
          b.years, b.has_option, b.aav_m,
          p.roster_player_id
        FROM bids b
        JOIN free_agents p ON p.id=b.player_id
        WHERE b.status='ACTIVE'
          AND b.expires_at IS NOT NULL
          AND (p.signed_team IS NULL OR p.signed_team='')
    """)
    rows = cur.fetchall()

    to_sign = []
    for r in rows:
        exp = parse_iso(r["expires_at"])
        if now >= exp:
            to_sign.append({
                "bid_id": int(r["bid_id"]),
                "player_id": int(r["player_id"]),
                "team": r["team"],
                "years": int(r["years"] or 1),
                "has_option": bool(int(r["has_option"] or 0)),
                "aav_m": float(r["aav_m"] or 0.0),
                "roster_player_id": int(r["roster_player_id"] or 0) if r["roster_player_id"] else None,
            })

    for item in to_sign:
        bid_id = item["bid_id"]
        pid = item["player_id"]
        team = item["team"]
        cur.execute("UPDATE free_agents SET signed_team=?, signed_at=? WHERE id=?", (team, iso(now), pid))
        cur.execute("UPDATE bids SET status='SIGNED' WHERE id=?", (bid_id,))
        # clear any other actives (should be none)
        cur.execute("UPDATE bids SET status='OUTBID', expires_at=NULL WHERE player_id=? AND status='ACTIVE' AND id<>?", (pid, bid_id))
        update_roster_db_for_fa_signing(
            item["roster_player_id"],
            team,
            item["years"],
            item["has_option"],
            item["aav_m"],
        )

    conn.commit()
    conn.close()

def get_current_leader(pid: int) -> Optional[sqlite3.Row]:
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT *
        FROM bids
        WHERE player_id=? AND status='ACTIVE'
        ORDER BY datetime(created_at) DESC
        LIMIT 1
    """, (pid,))
    row = cur.fetchone()
    conn.close()
    return row

def player_snapshot_row(p: sqlite3.Row) -> Dict[str, Any]:
    pid = int(p["id"])
    leader = get_current_leader(pid)

    mlbam_id = int(p["mlbam_id"] or 0) if "mlbam_id" in p.keys() else 0

    return {
        "id": pid,
        "mlbam_id": mlbam_id,
        "fg_url": p["fg_url"] if "fg_url" in p.keys() else None,

        "name": p["name"],
        "position": p["position"],
        "last_team": p["last_team"],
        "hometown_team": p["hometown_team"],
        "hometown_seasons": int(p["hometown_seasons"] or 0),
        "signed_team": p["signed_team"],
        "signed_at": p["signed_at"],
        "current_bid_value_m": float(leader["bid_value_m"]) if leader else None,
        "current_bid_team": leader["team"] if leader else None,
        "expires_at": leader["expires_at"] if leader else None,
    }

def fetch_free_agents(search: str = "", hide_signed: bool = False) -> List[Dict[str, Any]]:
    enforce_expirations()
    conn = get_conn()
    cur = conn.cursor()

    clauses = []
    params: List[Any] = []

    # Always show only players currently unrostered in live roster.db.
    clauses.append("COALESCE(is_roster_unrostered,0)=1")

    if search.strip():
        s = search.strip().lower()
        s2 = "".join(ch for ch in unicodedata.normalize("NFKD", s) if not unicodedata.combining(ch))
        clauses.append("(LOWER(unaccent(name)) LIKE ?)")
        params.append(f"%{s2}%")

    if hide_signed:
        clauses.append("(signed_team IS NULL OR signed_team='')")

    q = "SELECT * FROM free_agents"
    if clauses:
        q += " WHERE " + " AND ".join(clauses)
    q += " ORDER BY unaccent(name) COLLATE NOCASE ASC"

    cur.execute(q, params)
    rows = cur.fetchall()
    conn.close()
    return [player_snapshot_row(r) for r in rows]

def fetch_watchlist(team: str) -> List[Dict[str, Any]]:
    enforce_expirations()
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT p.*
        FROM watchlist w
        JOIN free_agents p ON p.id=w.player_id
        WHERE w.team=?
          AND COALESCE(p.is_roster_unrostered,0)=1
        ORDER BY datetime(w.created_at) DESC
    """, (team,))
    rows = cur.fetchall()
    conn.close()
    return [player_snapshot_row(r) for r in rows]

def compute_preview(team: str, pid: int, years: int, has_option: bool, aav_m: float) -> Dict[str, Any]:
    enforce_expirations()

    years = clamp_int(years, 1, 6, 1)
    has_option = bool(has_option)
    if has_option and years >= 6:
        # cannot exceed 6 total years
        years = 5

    aav_m = clamp_float(aav_m, 0.0, 9999.0, 0.0)

    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT * FROM free_agents WHERE id=?", (pid,))
    p = cur.fetchone()
    conn.close()
    if not p:
        raise ValueError("player not found")
    if int(p["is_roster_unrostered"] or 0) != 1:
        raise ValueError("player is no longer an eligible free agent")

    signed = bool((p["signed_team"] or "").strip())
    leader = get_current_leader(pid)

    # Hometown multiplier if this team is hometown_team
    hm_mult = 1.0
    if (p["hometown_team"] or "").strip() and team == (p["hometown_team"] or "").strip():
        hm_mult = hometown_multiplier(int(p["hometown_seasons"] or 0))

    min_aav = min_aav_millions(years, has_option)
    my_val = compute_bid_value_1yr_equiv(aav_m, years, has_option, hm_mult)

    cur_val = float(leader["bid_value_m"]) if leader else None
    required = None
    if cur_val is not None:
        required = cur_val * 1.10

    # validation booleans
    meets_min_aav = (aav_m >= min_aav - 1e-9)
    meets_outbid = True
    if required is not None:
        meets_outbid = (my_val >= required - 1e-9)

    return {
        "player_id": pid,
        "team": team,
        "signed": signed,

        "years": years,
        "has_option": has_option,
        "aav_m": aav_m,

        "hometown_team": p["hometown_team"],
        "hometown_seasons": int(p["hometown_seasons"] or 0),
        "hometown_multiplier": hm_mult,
        "is_hometown_bid": hm_mult > 1.0,

        "min_aav_m": min_aav,
        "my_bid_value_m": my_val,

        "current_bid_value_m": cur_val,
        "current_bid_team": (leader["team"] if leader else None),
        "current_expires_at": (leader["expires_at"] if leader else None),

        "required_min_bid_value_m": required,  # this is the 10% rule threshold

        "meets_min_aav": meets_min_aav,
        "meets_outbid": meets_outbid,
        "ok_to_submit": (not signed) and meets_min_aav and meets_outbid,
    }

def place_bid(team: str, pid: int, years: int, has_option: bool, aav_m: float) -> Tuple[bool, str]:
    enforce_expirations()

    years = clamp_int(years, 1, 6, 1)
    has_option = bool(has_option)
    if has_option and years >= 6:
        return (False, "Option year would exceed 6 total years (max is 5+opt).")

    aav_m = clamp_float(aav_m, 0.0, 9999.0, 0.0)

    # load player
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT * FROM free_agents WHERE id=?", (pid,))
    p = cur.fetchone()
    if not p:
        conn.close()
        return (False, "Player not found.")
    if int(p["is_roster_unrostered"] or 0) != 1:
        conn.close()
        return (False, "Player is no longer an eligible free agent.")
    if (p["signed_team"] or "").strip():
        conn.close()
        return (False, "Player already signed.")

    # preview for validations
    try:
        prev = compute_preview(team, pid, years, has_option, aav_m)
    except Exception as e:
        conn.close()
        return (False, str(e))

    if not prev["meets_min_aav"]:
        conn.close()
        return (False, f"AAV is below minimum for that contract: minimum is {fmt_money_m(prev['min_aav_m'])}/yr.")
    if not prev["meets_outbid"]:
        need = prev["required_min_bid_value_m"]
        conn.close()
        return (False, f"Bid must be at least 10% higher than current leader: need ≥ {fmt_money_m(need)} (1-year equiv).")

    # Calculate + insert
    now = utcnow()
    exp = now + timedelta(hours=48)

    # Mark previous active as outbid, clear their expiry
    cur.execute("UPDATE bids SET status='OUTBID', expires_at=NULL WHERE player_id=? AND status='ACTIVE'", (pid,))

    cur.execute("""
        INSERT INTO bids(player_id, team, years, has_option, aav_m, bid_value_m, hometown_mult, created_at, expires_at, status)
        VALUES(?,?,?,?,?,?,?,?,?, 'ACTIVE')
    """, (
        pid, team,
        int(prev["years"]),
        1 if prev["has_option"] else 0,
        float(prev["aav_m"]),
        float(prev["my_bid_value_m"]),
        float(prev["hometown_multiplier"]),
        iso(now),
        iso(exp),
    ))

    conn.commit()
    conn.close()
    return (True, "")


def bootstrap_fa():
    """
    Call this from app.py AFTER the Flask app exists (inside app.app_context()).
    """
    init_db()

    # If you have a real registry, prefer it.
    if db_is_empty() and PLAYER_REGISTRY_CSV.exists():
        import_player_registry_csv(PLAYER_REGISTRY_CSV)
    else:
        generate_sample_free_agents_csv(FREE_AGENTS_CSV)
        if db_is_empty():
            import_free_agents_csv(FREE_AGENTS_CSV)

    sync_free_agents_from_roster_if_needed(force=True)

    seed_qualifying_offers(qo_aav_m=21.2)


# ---------- UI (templates) ----------
BASE_STYLE = r"""
<style>
/* -----------------------------
   BNSL Free Agency — Game UI
   Drop-in CSS overhaul
-------------------------------- */

/* Optional font (safe fallback if blocked) */
@import url('https://fonts.googleapis.com/css2?family=Rajdhani:wght@500;600;700&family=Inter:wght@400;500;600&display=swap');

:root{
  --bg0:#070A12;
  --bg1:#0A1020;
  --panel: rgba(18, 26, 48, .62);
  --panel2: rgba(12, 18, 34, .70);
  --stroke: rgba(140, 170, 255, .18);
  --stroke2: rgba(255,255,255,.08);

  --text:#EAF0FF;
  --muted: rgba(234,240,255,.68);

  --accent:#7C5CFF;
  --accent2:#2EF2FF;
  --good:#36F9A2;
  --warn:#FF4D6D;
  --gold:#FFD166;

  --shadow: 0 18px 60px rgba(0,0,0,.55);
  --shadow2: 0 10px 30px rgba(0,0,0,.45);

  --r12: 12px;
  --r16: 16px;
  --r20: 20px;

  --gap: 14px;
}

*{ box-sizing:border-box; }
html, body{ height:100%; }
body{
  margin:0;
  color: var(--text);
  background:
    radial-gradient(1100px 680px at 70% -10%, rgba(124,92,255,.25), transparent 55%),
    radial-gradient(900px 620px at 20% 0%, rgba(46,242,255,.16), transparent 52%),
    radial-gradient(900px 700px at 70% 80%, rgba(255,209,102,.08), transparent 55%),
    linear-gradient(180deg, var(--bg0), var(--bg1));
  font-family: Inter, system-ui, -apple-system, Segoe UI, Roboto, Helvetica, Arial, sans-serif;
  overflow-x:hidden;
}

/* subtle animated aurora */
body::before{
  content:"";
  position:fixed; inset:-40vmax;
  background:
    conic-gradient(from 210deg,
      rgba(124,92,255,.10),
      rgba(46,242,255,.10),
      rgba(255,77,109,.08),
      rgba(124,92,255,.10)
    );
  filter: blur(60px);
  opacity:.65;
  animation: drift 14s ease-in-out infinite alternate;
  pointer-events:none;
  z-index:-3;
}
@keyframes drift{
  from{ transform: translate3d(-3%, -2%, 0) rotate(-4deg); }
  to{   transform: translate3d( 3%,  2%, 0) rotate( 6deg); }
}

/* scanlines + grid */
body::after{
  content:"";
  position:fixed; inset:0;
  background:
    linear-gradient(to bottom, rgba(255,255,255,.05), rgba(255,255,255,0) 2px) 0 0/100% 6px,
    linear-gradient(to right, rgba(124,92,255,.10), rgba(0,0,0,0) 20%) 0 0/260px 260px,
    radial-gradient(circle at 50% 50%, rgba(46,242,255,.10), transparent 55%);
  mix-blend-mode: overlay;
  opacity:.25;
  pointer-events:none;
  z-index:-2;
}

/* layout wrapper */
.page{
  max-width: 1240px;
  margin: 0 auto;
  padding: 22px 18px 40px;
}

a{ color: inherit; text-decoration:none; }
a:hover{ text-decoration:none; }

/* header brand */
.brand{
  display:flex; align-items:flex-end; justify-content:space-between;
  gap: 18px;
  margin: 10px 0 16px;
}
.brand h1{
  margin:0;
  font-family: Rajdhani, Inter, system-ui;
  font-weight: 700;
  letter-spacing: .6px;
  font-size: 34px;
  line-height: 1;
}
.brand .sub{
  margin-top: 6px;
  color: var(--muted);
  font-size: 13px;
}
.brand .right{
  display:flex; gap: 10px; align-items:center; flex-wrap:wrap;
}

/* panels */
.panel{
  background: var(--panel);
  border: 1px solid var(--stroke);
  border-radius: var(--r20);
  box-shadow: var(--shadow);
  backdrop-filter: blur(10px);
  -webkit-backdrop-filter: blur(10px);
  overflow:hidden;
}
.panel.pad{ padding: 14px; }

.topbar{
  display:flex;
  flex-wrap:wrap;
  gap: 10px;
  align-items:center;
}

.pill{
  display:inline-flex;
  gap:10px;
  align-items:center;
  padding: 10px 12px;
  border-radius: 999px;
  background: rgba(10, 16, 32, .55);
  border: 1px solid var(--stroke2);
  box-shadow: 0 6px 20px rgba(0,0,0,.25);
  color: var(--text);
}
.pill strong{ font-weight:600; }
.muted{ color: var(--muted); }

.badge{
  font-size: 12px;
  padding: 4px 10px;
  border-radius: 999px;
  background: rgba(124,92,255,.14);
  border: 1px solid rgba(124,92,255,.28);
  color: rgba(234,240,255,.92);
  letter-spacing:.2px;
}

/* inputs */
select, input[type="text"], input[type="number"]{
  background: rgba(5, 8, 16, .55);
  color: var(--text);
  border: 1px solid rgba(140,170,255,.22);
  border-radius: 12px;
  padding: 10px 12px;
  outline: none;
  transition: transform .12s ease, border-color .18s ease, box-shadow .18s ease;
}
select:focus, input:focus{
  border-color: rgba(46,242,255,.55);
  box-shadow: 0 0 0 3px rgba(46,242,255,.12);
}
input::placeholder{ color: rgba(234,240,255,.35); }

/* buttons */
.btn{
  position:relative;
  padding: 10px 12px;
  border-radius: 14px;
  border: 1px solid rgba(140,170,255,.22);
  background:
    linear-gradient(180deg, rgba(255,255,255,.08), rgba(255,255,255,.02));
  color: var(--text);
  cursor:pointer;
  transition: transform .12s ease, box-shadow .18s ease, border-color .18s ease, filter .18s ease;
  box-shadow: 0 10px 26px rgba(0,0,0,.25);
}
.btn:hover{
  transform: translateY(-1px);
  border-color: rgba(46,242,255,.45);
  box-shadow: 0 14px 36px rgba(0,0,0,.35);
}
.btn:active{ transform: translateY(0px) scale(.99); }
.btn[disabled]{
  opacity:.45;
  cursor:not-allowed;
  transform:none;
  box-shadow:none;
}
.btn.primary{
  border-color: rgba(124,92,255,.55);
  background:
    linear-gradient(180deg, rgba(124,92,255,.55), rgba(124,92,255,.18));
}
.btn.good{
  border-color: rgba(54,249,162,.50);
  background:
    linear-gradient(180deg, rgba(54,249,162,.35), rgba(54,249,162,.10));
}
.btn.danger{
  border-color: rgba(255,77,109,.55);
  background:
    linear-gradient(180deg, rgba(255,77,109,.35), rgba(255,77,109,.10));
}

hr.sep{
  border:none;
  border-top: 1px solid rgba(255,255,255,.08);
  margin: 14px 0;
}

/* data table -> “tactical grid” */
.table-wrap{
  overflow:auto;
}
table{
  width:100%;
  border-collapse:separate;
  border-spacing:0;
  min-width: 980px;
}
thead th{
  position: sticky;
  top: 0;
  z-index: 2;
  text-align:left;
  font-size: 12px;
  letter-spacing:.5px;
  text-transform: uppercase;
  color: rgba(234,240,255,.78);
  background: rgba(8, 12, 24, .85);
  border-bottom: 1px solid rgba(140,170,255,.18);
  padding: 12px 12px;
}
tbody td{
  padding: 12px 12px;
  border-bottom: 1px solid rgba(255,255,255,.06);
  vertical-align: middle;
}
tr.row-hover{
  background: linear-gradient(180deg, rgba(255,255,255,.02), rgba(255,255,255,.00));
}
tr.row-hover:hover{
  background:
    radial-gradient(900px 160px at 20% 50%, rgba(46,242,255,.10), transparent 60%),
    linear-gradient(180deg, rgba(124,92,255,.10), rgba(255,255,255,0));
}
tr.signed{
  opacity:.55;
  filter:saturate(.7);
}

.pname{
  display:flex;
  align-items:center;
  gap:10px;
}
.pimg{
  width: 34px; height:34px;
  border-radius: 12px;
  object-fit: cover;
  border: 1px solid rgba(140,170,255,.20);
  box-shadow: 0 10px 22px rgba(0,0,0,.35);
}

/* status colors */
.danger{ color: var(--warn); font-weight:700; }
.green{ color: var(--good); font-weight:700; }

/* modal -> glass + neon */
dialog{
  border:none;
  border-radius: 22px;
  padding:0;
  width: min(760px, 94vw);
  background: rgba(8, 12, 24, .72);
  color: var(--text);
  box-shadow: var(--shadow);
  backdrop-filter: blur(14px);
  -webkit-backdrop-filter: blur(14px);
  overflow:hidden;
}
dialog::backdrop{
  background: radial-gradient(circle at 50% 30%, rgba(124,92,255,.22), rgba(0,0,0,.70));
}
.modal-head{
  padding: 16px 16px;
  border-bottom: 1px solid rgba(255,255,255,.08);
  display:flex;
  gap:12px;
  align-items:center;
  justify-content:space-between;
  background: linear-gradient(180deg, rgba(124,92,255,.18), rgba(255,255,255,0));
}
.modal-head #modal-title{
  font-family: Rajdhani, Inter, system-ui;
  font-weight: 700;
  letter-spacing: .4px;
  font-size: 18px;
}
.modal-body{
  padding: 16px;
  display:grid;
  gap: 12px;
}
.modal-grid{
  display:grid;
  grid-template-columns: 1fr 1fr;
  gap: 12px;
}
.field{ display:grid; gap:8px; }
.field label{ color: rgba(234,240,255,.85); }
.modal-foot{
  padding: 16px;
  border-top: 1px solid rgba(255,255,255,.08);
  display:flex;
  justify-content:flex-end;
  gap:10px;
  background: rgba(0,0,0,.18);
}

.kv{
  display:flex;
  justify-content:space-between;
  gap:12px;
  padding: 10px 12px;
  border: 1px solid rgba(140,170,255,.18);
  border-radius: 16px;
  background: rgba(0,0,0,.16);
}
.kv b{
  font-weight: 700;
  letter-spacing:.2px;
}

/* checkbox */
input[type="checkbox"]{
  width: 16px; height: 16px;
  accent-color: var(--accent2);
}

/* responsive improvements */
@media (max-width: 840px){
  .brand{ align-items:flex-start; flex-direction:column; }
  .modal-grid{ grid-template-columns: 1fr; }
  table{ min-width: 860px; }
}

/* reduce motion */
@media (prefers-reduced-motion: reduce){
  body::before{ animation:none; }
  .btn, select, input{ transition:none; }
}
</style>
"""

INDEX_HTML = f"""
<!doctype html>
<html>
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Free Agency</title>
  {BASE_STYLE}
</head>
<body>
  <div class="page">

    <div class="brand">
      <div>
        <h1>FREE AGENCY</h1>
        <div class="sub">Contract bids • 48h clock • Hometown bonus baked into value</div>
      </div>
    </div>

    <div class="panel pad">
      <div class="topbar">

    <label class="pill">Your Team:
      <select id="team-select" style="margin-left:8px;"></select>
    </label>
    <button id="login-btn" class="btn">Login</button>

    <a class="btn" id="watchlist-link" href="watchlist" style="display:none;">Watchlist</a>
    <a class="btn" href="history">Bid History</a>

    <label class="pill">
      <input type="checkbox" id="hide-signed" /> Hide signed
    </label>

    <div class="pill" style="margin-left:auto;">
      <span>Search:</span>
      <input id="search" type="text" placeholder="Type a player name…" style="border:1px solid #ddd; padding:6px 8px; border-radius:6px; min-width: 260px;" />
      <span class="muted">(substring match)</span>
    </div>
  </div>

  <div class="pill" id="login-pill">
    <span id="login-status">🔒 Not logged in</span>
  </div>

  <hr class="sep" />
  <div class="table-wrap">
    <table>
    <thead>
      <tr>
        <th style="width:22%;">Player</th>
        <th style="width:7%;">Pos</th>
        <th style="width:16%;">Leader</th>
        <th style="width:14%;">1-yr equiv value</th>
        <th style="width:20%;">Bid expires / signs</th>
        <th style="width:10%;">Status</th>
        <th style="width:11%;">Actions</th>
      </tr>
    </thead>
    <tbody id="fa-body"></tbody>
  </table>
  </div>
  
  <dialog id="bid-modal">
    <div class="modal-head">
      <div>
        <div style="font-size:16px; font-weight:700;" id="modal-title">Place Bid</div>
        <div class="muted" id="modal-subtitle"></div>
      </div>
      <button class="btn" id="modal-close">Close</button>
    </div>
    <div class="modal-body">
      <div class="modal-grid">
        <div class="field">
          <label><b>Guaranteed years</b></label>
          <select id="years"></select>
        </div>
        <div class="field">
          <label><b>Club option?</b></label>
          <select id="opt">
            <option value="0">No option</option>
            <option value="1">Yes (adds 1 year)</option>
          </select>
        </div>
        <div class="field" style="grid-column: 1 / span 2;">
          <label><b>AAV ($M per year)</b> <span class="muted">e.g. 0.75 = $750k</span></label>
          <input id="aav" type="number" step="0.01" min="0" placeholder="e.g. 5.00" />
        </div>
      </div>

      <div class="kv"><span>Current leader</span><b id="cur-leader">—</b></div>
      <div class="kv"><span>Current bid value (1-yr equiv)</span><b id="cur-val">—</b></div>
      <div class="kv"><span>Minimum required to submit (10% rule)</span><b id="min-required">—</b></div>
      <div class="kv"><span>Minimum AAV allowed by rules</span><b id="min-aav">—</b></div>
      <div class="kv"><span>Your bid value (1-yr equiv)</span><b id="my-val">—</b></div>
      <div class="kv"><span>Hometown multiplier</span><b id="hm-mult">—</b></div>

      <div id="modal-warn" class="danger" style="display:none;"></div>
      <div id="modal-ok" class="green" style="display:none;"></div>
    </div>
    <div class="modal-foot">
      <button class="btn" id="submit-bid">Place Bid</button>
    </div>
  </dialog>

<script>
const faBody = document.getElementById('fa-body');
const teamSelect = document.getElementById('team-select');
const loginBtn = document.getElementById('login-btn');
const watchlistLink = document.getElementById('watchlist-link');
const searchInput = document.getElementById('search');
const hideSigned = document.getElementById('hide-signed');

const modal = document.getElementById('bid-modal');
const modalClose = document.getElementById('modal-close');
const modalTitle = document.getElementById('modal-title');
const modalSubtitle = document.getElementById('modal-subtitle');
const yearsSel = document.getElementById('years');
const optSel = document.getElementById('opt');
const aavInput = document.getElementById('aav');
const curLeader = document.getElementById('cur-leader');
const curVal = document.getElementById('cur-val');
const minRequired = document.getElementById('min-required');
const minAav = document.getElementById('min-aav');
const myVal = document.getElementById('my-val');
const hmMult = document.getElementById('hm-mult');
const modalWarn = document.getElementById('modal-warn');
const modalOk = document.getElementById('modal-ok');
const submitBidBtn = document.getElementById('submit-bid');

let state = {{
  team: "",
  authed: false,
  authedEmail: "",
  search: "",
  hideSigned: false,
  players: [],
  modalPlayer: null
}};

function moneyM(x) {{
  if (x === null || x === undefined) return "—";
  return `$${{Number(x).toFixed(2)}}M`;
}}

function fmtIso(isoStr) {{
  if (!isoStr) return "—";
  try {{
    const d = new Date(isoStr);
    return d.toLocaleString();
  }} catch {{
    return isoStr;
  }}
}}

function setTeams(teams) {{
  teamSelect.innerHTML = '<option value="">— Select Team —</option>' +
    teams.map(t => `<option value="${{t}}">${{t}}</option>`).join('');
}}

async function fetchStatus() {{
  const res = await fetch('/fa/api/fa_status');
  const data = await res.json();
  setTeams(data.teams || []);
  if (data.selected_team) teamSelect.value = data.selected_team;

  state.team = data.selected_team || "";
  state.authed = !!data.authed_for_selected;
  state.authedEmail = data.authed_email || "";

  const loginStatus = document.getElementById('login-status');
  if (state.authed && state.team) {{
    loginStatus.textContent = `🔓 Logged in as ${{state.authedEmail}} for ${{state.team}}`;
    watchlistLink.style.display = 'inline-block';
  }} else {{
    loginStatus.textContent = '🔒 Not logged in';
    watchlistLink.style.display = 'none';
  }}

  loginBtn.disabled = !teamSelect.value;
}}

async function fetchPlayers() {{
  const params = new URLSearchParams({{
    search: state.search,
    hide_signed: state.hideSigned ? '1' : '0',
  }});
  const res = await fetch('/fa/api/free_agents?' + params.toString());
  const data = await res.json();
  state.players = data.players || [];
  renderPlayers();
}}

function renderPlayers() {{
  faBody.innerHTML = '';
  for (const p of state.players) {{
    const tr = document.createElement('tr');
    tr.className = 'row-hover' + (p.signed_team ? ' signed' : '');
    const img = p.mlbam_id ? `/fa/player_image/${{p.mlbam_id}}.png` : '';
    const imgTag = p.mlbam_id ? `<img class="pimg" src="${{img}}" loading="lazy" />` : '';

    const leader = p.current_bid_team ? p.current_bid_team : '—';
    const val = p.current_bid_value_m !== null ? moneyM(p.current_bid_value_m) : '—';
    const exp = p.expires_at ? fmtIso(p.expires_at) : '—';

    const status = p.signed_team ? `Signed: ${{p.signed_team}}` : (p.current_bid_team ? 'Bidding open' : 'No bids');

    const actionTd = document.createElement('td');

    const bidBtn = document.createElement('button');
    bidBtn.className = 'btn';
    bidBtn.textContent = 'Bid';
    bidBtn.disabled = !state.authed || !!p.signed_team;
    bidBtn.onclick = () => openBidModal(p);

const wBtn = document.createElement('button');
wBtn.className = 'btn';

const alreadyWatching = !!p.in_watchlist;
wBtn.textContent = alreadyWatching ? '★ Watching' : '★ Watch';
wBtn.disabled = !state.authed || alreadyWatching;
    
    wBtn.onclick = async () => {{
      const resp = await fetch('/fa/api/watchlist/add', {{
        method: 'POST',
        headers: {{'Content-Type':'application/json'}},
        body: JSON.stringify({{ player_id: p.id }})
      }});
      if (!resp.ok) {{
        alert('Could not add to watchlist: ' + await resp.text());
      }} else {{
        wBtn.textContent = '★ Watching';
        wBtn.disabled = true;
      }}
    }};

    actionTd.appendChild(bidBtn);
    actionTd.appendChild(document.createTextNode(' '));
    actionTd.appendChild(wBtn);

const linkOpen = p.fg_url ? `<a href="${{p.fg_url}}" target="_blank" rel="noopener noreferrer">` : '';
const linkClose = p.fg_url ? `</a>` : '';

tr.innerHTML = `
  <td>
    ${{linkOpen}}
      <div class="pname">${{imgTag}}<b>${{p.name}}</b></div>
    ${{linkClose}}
    <div class="muted" style="font-size:12px;">Hometown Discount: ${{p.hometown_team || '—'}}</div>
  </td>
  <td>${{p.position || '—'}}</td>
  <td>${{leader}}</td>
  <td>${{val}}</td>
  <td>${{exp}}</td>
  <td>${{status}}</td>
`;

    tr.appendChild(actionTd);
    faBody.appendChild(tr);
  }}
}}

function fillYearsDropdown() {{
  yearsSel.innerHTML = '';
  for (let y=1; y<=6; y++) {{
    const opt = document.createElement('option');
    opt.value = String(y);
    opt.textContent = String(y);
    yearsSel.appendChild(opt);
  }}
}}

function normalizeOptionAvailability() {{
  const y = Number(yearsSel.value || 1);
  if (y >= 6) {{
    optSel.value = "0";
    optSel.querySelector('option[value="1"]').disabled = true;
  }} else {{
    optSel.querySelector('option[value="1"]').disabled = false;
  }}
}}

async function previewBid() {{
  modalWarn.style.display = 'none';
  modalOk.style.display = 'none';

  const p = state.modalPlayer;
  if (!p) return;

  const years = Number(yearsSel.value || 1);
  const has_option = (optSel.value === "1");
  const aav = Number(aavInput.value || 0);

  const params = new URLSearchParams({{
    player_id: String(p.id),
    years: String(years),
    has_option: has_option ? '1' : '0',
    aav_m: String(aav),
  }});

  const res = await fetch('/fa/api/bid_preview?' + params.toString());
  const data = await res.json();

  curLeader.textContent = data.current_bid_team ? data.current_bid_team : '—';
  curVal.textContent = data.current_bid_value_m !== null ? moneyM(data.current_bid_value_m) : '—';
  minRequired.textContent = data.required_min_bid_value_m !== null ? moneyM(data.required_min_bid_value_m) : '—';
  minAav.textContent = moneyM(data.min_aav_m) + '/yr';
  myVal.textContent = moneyM(data.my_bid_value_m);
  hmMult.textContent = data.is_hometown_bid ? `${{data.hometown_multiplier.toFixed(2)}}x (hometown)` : `${{data.hometown_multiplier.toFixed(2)}}x`;

  submitBidBtn.disabled = !data.ok_to_submit;

  if (data.signed) {{
    modalWarn.textContent = "This player is already signed.";
    modalWarn.style.display = 'block';
  }} else if (!data.meets_min_aav) {{
    modalWarn.textContent = `AAV is below minimum for that structure (min ${{moneyM(data.min_aav_m)}}/yr).`;
    modalWarn.style.display = 'block';
  }} else if (!data.meets_outbid) {{
    modalWarn.textContent = `Bid value must be at least 10% higher than current leader (need ≥ ${{moneyM(data.required_min_bid_value_m)}}).`;
    modalWarn.style.display = 'block';
  }} else {{
    modalOk.textContent = "Bid is valid to submit.";
    modalOk.style.display = 'block';
  }}
}}

function openBidModal(p) {{
  state.modalPlayer = p;
  modalTitle.textContent = `Bid: ${{p.name}}`;
  modalSubtitle.textContent = p.signed_team ? `Signed to ${{p.signed_team}}` : 'Set years/option/AAV. Preview updates live.';

  fillYearsDropdown();
  yearsSel.value = "1";
  optSel.value = "0";
  aavInput.value = "";

  normalizeOptionAvailability();
  previewBid();

  modal.showModal();
}}

modalClose.onclick = () => modal.close();

yearsSel.onchange = () => {{
  normalizeOptionAvailability();
  previewBid();
}};
optSel.onchange = previewBid;
aavInput.oninput = () => {{
  // small debounce-ish behavior by letting the event loop breathe
  window.requestAnimationFrame(previewBid);
}};

submitBidBtn.onclick = async () => {{
  const p = state.modalPlayer;
  if (!p) return;

  const payload = {{
    player_id: p.id,
    years: Number(yearsSel.value || 1),
    has_option: (optSel.value === "1"),
    aav_m: Number(aavInput.value || 0),
  }};

  submitBidBtn.disabled = true;
  const resp = await fetch('/fa/api/bid', {{
    method: 'POST',
    headers: {{'Content-Type':'application/json'}},
    body: JSON.stringify(payload)
  }});

  if (!resp.ok) {{
    alert('Bid failed: ' + await resp.text());
    submitBidBtn.disabled = false;
    return;
  }}

  modal.close();
  await fetchPlayers();
  submitBidBtn.disabled = false;
}};

function debounce(fn, ms) {{
  let t; return function(...args) {{
    clearTimeout(t);
    t = setTimeout(() => fn.apply(this, args), ms);
  }};
}}

searchInput.addEventListener('input', debounce(() => {{
  state.search = searchInput.value;
  fetchPlayers();
}}, 120));

hideSigned.addEventListener('change', () => {{
  state.hideSigned = hideSigned.checked;
  fetchPlayers();
}});

teamSelect.addEventListener('change', async () => {{
  const t = teamSelect.value || '';
  await fetch('/fa/api/select_team', {{
    method: 'POST',
    headers: {{'Content-Type':'application/json'}},
    body: JSON.stringify({{ team: t }})
  }});
  await fetchStatus();
  await fetchPlayers();
}});

loginBtn.addEventListener('click', async () => {{
  const t = teamSelect.value || '';
  if (!t) {{
    alert('Please select a team first.');
    return;
  }}
  const email = window.prompt(`Enter the manager email for ${{t}}:`);
  if (!email || !email.trim()) return;

  const resp = await fetch('/fa/api/login_team', {{
    method: 'POST',
    headers: {{'Content-Type':'application/json'}},
    body: JSON.stringify({{ team: t, email: email.trim() }})
  }});
  if (!resp.ok) {{
    alert('Login failed: ' + await resp.text());
  }}
  await fetchStatus();
  await fetchPlayers();
}});

(async function boot() {{
  await fetchStatus();
  await fetchPlayers();
}})();
</script>
  </div> <!-- /panel -->
</div>   <!-- /page -->
</body>
</html>
"""

WATCHLIST_HTML = f"""
<!doctype html>
<html>
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Watchlist</title>
  {BASE_STYLE}
</head>
<div class="page">
  <div class="brand">
    <div>
      <h1>WATCHLIST</h1>
      <div class="sub">Quick bid from your tracked players</div>
    </div>
    <div class="right"><span class="badge">TRACKING</span></div>
  </div>

  <div class="panel pad">
    <!-- existing topbar/table -->
<body>
  <div class="topbar">
    <a class="btn" href="./">← Back</a>
    <span class="pill">Watchlist</span>
    <a class="btn" href="history">Bid History</a>
  </div>

  <div class="pill" id="login-pill">
    <span id="login-status">Loading…</span>
  </div>

  <table>
    <thead>
      <tr>
        <th style="width:22%;">Player</th>
        <th style="width:7%;">Pos</th>
        <th style="width:16%;">Leader</th>
        <th style="width:14%;">1-yr equiv value</th>
        <th style="width:20%;">Bid expires / signs</th>
        <th style="width:10%;">Status</th>
        <th style="width:11%;">Actions</th>
      </tr>
    </thead>
    <tbody id="wl-body"></tbody>
  </table>

  <dialog id="bid-modal">
    <!-- reuse the same bid modal UI, but keep it inside an actual dialog element -->
    {INDEX_HTML.split('<dialog id="bid-modal">',1)[1].split('</dialog>',1)[0]}
  </dialog>

<script>
const wlBody = document.getElementById('wl-body');
const loginStatus = document.getElementById('login-status');

// modal refs (duplicated IDs match embedded chunk)
const modal = document.getElementById('bid-modal');
const modalClose = document.getElementById('modal-close');
const modalTitle = document.getElementById('modal-title');
const modalSubtitle = document.getElementById('modal-subtitle');
const yearsSel = document.getElementById('years');
const optSel = document.getElementById('opt');
const aavInput = document.getElementById('aav');
const curLeader = document.getElementById('cur-leader');
const curVal = document.getElementById('cur-val');
const minRequired = document.getElementById('min-required');
const minAav = document.getElementById('min-aav');
const myVal = document.getElementById('my-val');
const hmMult = document.getElementById('hm-mult');
const modalWarn = document.getElementById('modal-warn');
const modalOk = document.getElementById('modal-ok');
const submitBidBtn = document.getElementById('submit-bid');

let state = {{
  team: "",
  authed: false,
  authedEmail: "",
  players: [],
  modalPlayer: null
}};

function moneyM(x) {{
  if (x === null || x === undefined) return "—";
  return `$${{Number(x).toFixed(2)}}M`;
}}
function fmtIso(isoStr) {{
  if (!isoStr) return "—";
  try {{ return new Date(isoStr).toLocaleString(); }} catch {{ return isoStr; }}
}}

async function fetchStatus() {{
  const res = await fetch('/fa/api/fa_status');
  const data = await res.json();
  state.team = data.selected_team || "";
  state.authed = !!data.authed_for_selected;
  state.authedEmail = data.authed_email || "";
  if (state.authed && state.team) {{
    loginStatus.textContent = `🔓 Logged in as ${{state.authedEmail}} for ${{state.team}}`;
  }} else {{
    loginStatus.textContent = '🔒 Not logged in (go back and login)';
  }}
}}

async function fetchWatchlist() {{
  const res = await fetch('/fa/api/watchlist');
  if (!res.ok) {{
    wlBody.innerHTML = '<tr><td colspan="7" class="danger">Not logged in.</td></tr>';
    return;
  }}
  const data = await res.json();
  state.players = data.players || [];
  render();
}}

function render() {{
  wlBody.innerHTML = '';
  if (!state.players.length) {{
    wlBody.innerHTML = '<tr><td colspan="7" class="muted">No players in your watchlist yet.</td></tr>';
    return;
  }}

  for (const p of state.players) {{
    const tr = document.createElement('tr');
    tr.className = 'row-hover' + (p.signed_team ? ' signed' : '');
    const leader = p.current_bid_team ? p.current_bid_team : '—';
    const val = p.current_bid_value_m !== null ? moneyM(p.current_bid_value_m) : '—';
    const exp = p.expires_at ? fmtIso(p.expires_at) : '—';
    const status = p.signed_team ? `Signed: ${{p.signed_team}}` : (p.current_bid_team ? 'Bidding open' : 'No bids');
    const img = p.mlbam_id ? `/fa/player_image/${{p.mlbam_id}}.png` : '';
    const imgTag = p.mlbam_id ? `<img class="pimg" src="${{img}}" loading="lazy" />` : '';

    const actionTd = document.createElement('td');

    const bidBtn = document.createElement('button');
    bidBtn.className = 'btn';
    bidBtn.textContent = 'Bid';
    bidBtn.disabled = !state.authed || !!p.signed_team;
    bidBtn.onclick = () => openBidModal(p);

    const rmBtn = document.createElement('button');
    rmBtn.className = 'btn';
    rmBtn.textContent = 'Remove';
    rmBtn.disabled = !state.authed;
    rmBtn.onclick = async () => {{
      const resp = await fetch('/fa/api/watchlist/remove', {{
        method:'POST',
        headers: {{'Content-Type':'application/json'}},
        body: JSON.stringify({{ player_id: p.id }})
      }});
      if (resp.ok) {{
        state.players = state.players.filter(x => x.id !== p.id);
        render();
      }}
    }};

    actionTd.appendChild(bidBtn);
    actionTd.appendChild(document.createTextNode(' '));
    actionTd.appendChild(rmBtn);

const linkOpen = p.fg_url ? `<a href="${{p.fg_url}}" target="_blank" rel="noopener noreferrer">` : '';
const linkClose = p.fg_url ? `</a>` : '';

tr.innerHTML = `
  <td>
    ${{linkOpen}}
      <div class="pname">${{imgTag}}<b>${{p.name}}</b></div>
    ${{linkClose}}
    <div class="muted" style="font-size:12px;">Hometown Discount: ${{p.hometown_team || '—'}}</div>
  </td>
  <td>${{p.position || '—'}}</td>
  <td>${{leader}}</td>
  <td>${{val}}</td>
  <td>${{exp}}</td>
  <td>${{status}}</td>
`;


    tr.appendChild(actionTd);
    wlBody.appendChild(tr);
  }}
}}

function fillYearsDropdown() {{
  yearsSel.innerHTML = '';
  for (let y=1; y<=6; y++) {{
    const opt = document.createElement('option');
    opt.value = String(y);
    opt.textContent = String(y);
    yearsSel.appendChild(opt);
  }}
}}
function normalizeOptionAvailability() {{
  const y = Number(yearsSel.value || 1);
  if (y >= 6) {{
    optSel.value = "0";
    optSel.querySelector('option[value="1"]').disabled = true;
  }} else {{
    optSel.querySelector('option[value="1"]').disabled = false;
  }}
}}

async function previewBid() {{
  modalWarn.style.display = 'none';
  modalOk.style.display = 'none';

  const p = state.modalPlayer;
  if (!p) return;

  const years = Number(yearsSel.value || 1);
  const has_option = (optSel.value === "1");
  const aav = Number(aavInput.value || 0);

  const params = new URLSearchParams({{
    player_id: String(p.id),
    years: String(years),
    has_option: has_option ? '1' : '0',
    aav_m: String(aav),
  }});

  const res = await fetch('/fa/api/bid_preview?' + params.toString());
  const data = await res.json();

  curLeader.textContent = data.current_bid_team ? data.current_bid_team : '—';
  curVal.textContent = data.current_bid_value_m !== null ? moneyM(data.current_bid_value_m) : '—';
  minRequired.textContent = data.required_min_bid_value_m !== null ? moneyM(data.required_min_bid_value_m) : '—';
  minAav.textContent = moneyM(data.min_aav_m) + '/yr';
  myVal.textContent = moneyM(data.my_bid_value_m);
  hmMult.textContent = data.is_hometown_bid ? `${{data.hometown_multiplier.toFixed(2)}}x (hometown)` : `${{data.hometown_multiplier.toFixed(2)}}x`;

  submitBidBtn.disabled = !data.ok_to_submit;

  if (data.signed) {{
    modalWarn.textContent = "This player is already signed.";
    modalWarn.style.display = 'block';
  }} else if (!data.meets_min_aav) {{
    modalWarn.textContent = `AAV is below minimum for that structure (min ${{moneyM(data.min_aav_m)}}/yr).`;
    modalWarn.style.display = 'block';
  }} else if (!data.meets_outbid) {{
    modalWarn.textContent = `Bid value must be at least 10% higher than current leader (need ≥ ${{moneyM(data.required_min_bid_value_m)}}).`;
    modalWarn.style.display = 'block';
  }} else {{
    modalOk.textContent = "Bid is valid to submit.";
    modalOk.style.display = 'block';
  }}
}}

function openBidModal(p) {{
  state.modalPlayer = p;
  modalTitle.textContent = `Bid: ${{p.name}}`;
  modalSubtitle.textContent = p.signed_team ? `Signed to ${{p.signed_team}}` : 'Set years/option/AAV. Preview updates live.';
  fillYearsDropdown();
  yearsSel.value = "1";
  optSel.value = "0";
  aavInput.value = "";
  normalizeOptionAvailability();
  previewBid();
  modal.showModal();
}}

modalClose.onclick = () => modal.close();
yearsSel.onchange = () => {{ normalizeOptionAvailability(); previewBid(); }};
optSel.onchange = previewBid;
aavInput.oninput = () => window.requestAnimationFrame(previewBid);

submitBidBtn.onclick = async () => {{
  const p = state.modalPlayer;
  if (!p) return;
  const payload = {{
    player_id: p.id,
    years: Number(yearsSel.value || 1),
    has_option: (optSel.value === "1"),
    aav_m: Number(aavInput.value || 0),
  }};
  submitBidBtn.disabled = true;
  const resp = await fetch('/fa/api/bid', {{
    method:'POST',
    headers: {{'Content-Type':'application/json'}},
    body: JSON.stringify(payload)
  }});
  if (!resp.ok) {{
    alert('Bid failed: ' + await resp.text());
    submitBidBtn.disabled = false;
    return;
  }}
  modal.close();
  await fetchWatchlist();
  submitBidBtn.disabled = false;
}};

(async function boot() {{
  await fetchStatus();
  await fetchWatchlist();
}})();
</script>
  </div> <!-- /panel -->
</div>   <!-- /page -->
</body>
</html>
"""

HISTORY_HTML = f"""
<!doctype html>
<html>
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Bid History</title>
  {BASE_STYLE}
</head>
<div class="page">
  <div class="brand">
    <div>
      <h1>WATCHLIST</h1>
      <div class="sub">Quick bid from your tracked players</div>
    </div>
    <div class="right"><span class="badge">TRACKING</span></div>
  </div>

  <div class="panel pad">
    <!-- existing topbar/table -->
<body>
  <div class="topbar">
    <a class="btn" href="./">← Back</a>
    <span class="pill">Bid History</span>
  </div>

  <div class="topbar">
    <div class="pill">
      <span>Search player:</span>
      <input id="search" type="text" placeholder="e.g. Ruth" style="border:1px solid #ddd; padding:6px 8px; border-radius:6px; min-width: 240px;" />
    </div>
    <button class="btn" id="refresh">Refresh</button>
  </div>

  <table>
    <thead>
      <tr>
        <th style="width:16%;">Time</th>
        <th style="width:22%;">Player</th>
        <th style="width:16%;">Team</th>
        <th style="width:10%;">Contract</th>
        <th style="width:12%;">AAV</th>
        <th style="width:14%;">1-yr equiv</th>
        <th style="width:16%;">Expires / Signed</th>
        <th style="width:10%;">Status</th>
      </tr>
    </thead>
    <tbody id="hist-body"></tbody>
  </table>

<script>
const body = document.getElementById('hist-body');
const search = document.getElementById('search');
const refreshBtn = document.getElementById('refresh');

function moneyM(x) {{
  if (x === null || x === undefined) return "—";
  return `$${{Number(x).toFixed(2)}}M`;
}}
function fmtIso(isoStr) {{
  if (!isoStr) return "—";
  try {{ return new Date(isoStr).toLocaleString(); }} catch {{ return isoStr; }}
}}
function contractText(b) {{
  const y = b.years;
  const opt = b.has_option ? "+opt" : "";
  return `${{y}}y${{opt}}`;
}}

async function load() {{
  const params = new URLSearchParams({{
    search: search.value || ""
  }});
  const res = await fetch('/fa/api/bid_history?' + params.toString());
  const data = await res.json();
  const rows = data.bids || [];

  body.innerHTML = '';
  if (!rows.length) {{
    body.innerHTML = '<tr><td colspan="8" class="muted">No bids found.</td></tr>';
    return;
  }}

  for (const b of rows) {{
    const tr = document.createElement('tr');
    tr.className = 'row-hover';
    tr.innerHTML = `
      <td>${{fmtIso(b.created_at)}}</td>
      <td><b>${{b.player_name}}</b></td>
      <td>${{b.team}}</td>
      <td>${{contractText(b)}}</td>
      <td>${{moneyM(b.aav_m)}}/yr</td>
      <td>${{moneyM(b.bid_value_m)}}</td>
      <td>${{fmtIso(b.expires_at)}}</td>
      <td>${{b.status}}</td>
    `;
    body.appendChild(tr);
  }}
}}

function debounce(fn, ms) {{
  let t; return function(...args) {{
    clearTimeout(t);
    t = setTimeout(() => fn.apply(this, args), ms);
  }};
}}

search.addEventListener('input', debounce(load, 150));
refreshBtn.onclick = load;

load();
</script>
  </div> <!-- /panel -->
</div>   <!-- /page -->
</body>
</html>
"""


# ---------- Pages ----------
@fa_bp.route("/")
def index():
    return render_template_string(INDEX_HTML)

@fa_bp.route("/watchlist")
def watchlist_page():
    if not session.get("authed_team"):
        return redirect(url_for("index"))
    return render_template_string(WATCHLIST_HTML)

@fa_bp.route("/history")
def history_page():
    return render_template_string(HISTORY_HTML)


# ---------- APIs ----------
@fa_bp.get("/api/fa_status")
def api_fa_status():
    enforce_expirations()
    selected_team = session.get("selected_team", "") or ""
    authed_team = session.get("authed_team", "") or ""
    authed_email = session.get("authed_email", "") or ""
    return jsonify({
        "teams": MLB_TEAMS,
        "selected_team": selected_team,
        "authed_team": authed_team,
        "authed_email": authed_email,
        "authed_for_selected": bool(selected_team) and (selected_team == authed_team),
    })

@fa_bp.post("/api/select_team")
def api_select_team():
    data = request.get_json(force=True, silent=True) or {}
    team = (data.get("team") or "").strip()
    if team and team not in MLB_TEAMS:
        return ("Unknown team", 400)
    session["selected_team"] = team
    if session.get("authed_team") != team:
        session.pop("authed_team", None)
        session.pop("authed_email", None)
    return ("", 204)

@fa_bp.post("/api/login_team")
def api_login_team():
    data = request.get_json(force=True, silent=True) or {}
    team = (data.get("team") or "").strip()
    email = (data.get("email") or "").strip()

    if not team or team not in MLB_TEAMS:
        return ("Unknown or missing team", 400)
    expected = TEAM_EMAILS.get(team)
    if not expected:
        return ("No email configured for team", 400)

    if emails_equal(email, expected):
        session["authed_team"] = team
        session["authed_email"] = email
        if session.get("selected_team") != team:
            session["selected_team"] = team
        return jsonify({"ok": True}), 200
    return ("Invalid email for this team", 401)

@fa_bp.get("/api/free_agents")
def api_free_agents():
    search = (request.args.get("search") or "").strip()
    hide_signed = (request.args.get("hide_signed") == "1")
    players = fetch_free_agents(search=search, hide_signed=hide_signed)

    # Mark whether each player is in the current user's watchlist (if logged in)
    authed_team = session.get("authed_team", "") or ""
    in_watch = set()
    if authed_team:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("SELECT player_id FROM watchlist WHERE team=?", (authed_team,))
        in_watch = {int(r[0]) for r in cur.fetchall()}
        conn.close()

    for p in players:
        p["in_watchlist"] = (p["id"] in in_watch)

    return jsonify({"players": players})

@fa_bp.get("/player_image/<int:mlbam_id>.png")
def player_image(mlbam_id: int):
    # cache it
    local = ensure_headshot_cached(mlbam_id)
    if not local:
        abort(404)

    return send_file(local, mimetype="image/png")


@fa_bp.get("/api/watchlist")
def api_watchlist_get():
    team = _require_authed_team()
    players = fetch_watchlist(team)
    return jsonify({"players": players})

@fa_bp.post("/api/watchlist/add")
def api_watchlist_add():
    team = _require_authed_team()
    data = request.get_json(force=True, silent=True) or {}
    pid = int(data.get("player_id") or 0)
    if pid <= 0:
        return ("missing player_id", 400)

    conn = get_conn()
    cur = conn.cursor()
    cur.execute("INSERT OR IGNORE INTO watchlist(team, player_id, created_at) VALUES(?,?,?)",
                (team, pid, iso(utcnow())))
    conn.commit()
    conn.close()
    return ("", 204)

@fa_bp.post("/api/watchlist/remove")
def api_watchlist_remove():
    team = _require_authed_team()
    data = request.get_json(force=True, silent=True) or {}
    pid = int(data.get("player_id") or 0)
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("DELETE FROM watchlist WHERE team=? AND player_id=?", (team, pid))
    conn.commit()
    conn.close()
    return ("", 204)

@fa_bp.get("/api/bid_preview")
def api_bid_preview():
    # Must be logged in to preview because hometown multiplier depends on bidding team identity
    team = _require_authed_team()
    pid = int(request.args.get("player_id") or 0)
    years = int(request.args.get("years") or 1)
    has_option = (request.args.get("has_option") == "1")
    aav_m = float(request.args.get("aav_m") or 0.0)
    try:
        data = compute_preview(team, pid, years, has_option, aav_m)
        return jsonify(data)
    except Exception as e:
        return jsonify({"error": str(e)}), 400

@fa_bp.post("/api/bid")
def api_bid():
    team = _require_authed_team()
    data = request.get_json(force=True, silent=True) or {}
    pid = int(data.get("player_id") or 0)
    years = int(data.get("years") or 1)
    has_option = bool(data.get("has_option"))
    aav_m = float(data.get("aav_m") or 0.0)

    ok, msg = place_bid(team, pid, years, has_option, aav_m)
    if not ok:
        return (msg, 409)
    return ("", 204)

@fa_bp.get("/api/bid_history")
def api_bid_history():
    enforce_expirations()
    search = (request.args.get("search") or "").strip().lower()

    conn = get_conn()
    cur = conn.cursor()

    q = """
        SELECT
          b.*,
          p.name AS player_name
        FROM bids b
        JOIN free_agents p ON p.id=b.player_id
    """
    params: List[Any] = []
    if search:
        s2 = "".join(ch for ch in unicodedata.normalize("NFKD", search) if not unicodedata.combining(ch))
        q += " WHERE LOWER(unaccent(p.name)) LIKE ?"
        params.append(f"%{s2}%")
    q += " ORDER BY datetime(b.created_at) DESC"

    cur.execute(q, params)
    rows = cur.fetchall()
    conn.close()

    out = []
    for r in rows:
        out.append({
            "id": int(r["id"]),
            "player_id": int(r["player_id"]),
            "player_name": r["player_name"],
            "team": r["team"],
            "years": int(r["years"]),
            "has_option": bool(int(r["has_option"] or 0)),
            "aav_m": float(r["aav_m"]),
            "bid_value_m": float(r["bid_value_m"]),
            "created_at": r["created_at"],
            "expires_at": r["expires_at"],
            "status": r["status"],
        })
    return jsonify({"bids": out})

@fa_bp.post("/tasks/enforce_expirations")
def task_enforce_expirations():
    try:
        enforce_expirations()
        return ("", 204)
    except Exception as e:
        return (f"enforce failed: {e}", 500)


@fa_bp.get("/healthz")
def healthz():
    return {"ok": True}

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--sync-registry", action="store_true", help="Import/refresh players from player_registry.csv into DB")
    ap.add_argument("--sync-roster", action="store_true", help="Import/refresh free agents from live roster.db and OOTP FA export")
    ap.add_argument("--apply-htd", action="store_true", help="Apply hometown discounts from hometown_discounts.db to current free_agents rows")
    ap.add_argument("--prefetch-headshots", action="store_true", help="Download headshots for all players with mlbam_id")
    args = ap.parse_args()

    if args.sync_roster:
        init_db()
        sync_free_agents_from_roster_if_needed(force=True)
        print("✅ Roster free agents synced.")
        if args.apply_htd:
            checked, matched, cleared = apply_hometown_discounts_to_free_agents(clear_missing=True)
            print(f"✅ Hometown discounts applied: {checked} checked, {matched} matched, {cleared} cleared.")
        raise SystemExit(0)

    if args.apply_htd:
        init_db()
        checked, matched, cleared = apply_hometown_discounts_to_free_agents(clear_missing=True)
        print(f"✅ Hometown discounts applied: {checked} checked, {matched} matched, {cleared} cleared.")
        raise SystemExit(0)

    if args.sync_registry:
        init_db()
        import_player_registry_csv(PLAYER_REGISTRY_CSV)
        print("✅ Registry synced.")
        if args.sync_roster:
            sync_free_agents_from_roster_if_needed(force=True)
            print("✅ Roster free agents synced.")
        if args.apply_htd:
            checked, matched, cleared = apply_hometown_discounts_to_free_agents(clear_missing=True)
            print(f"✅ Hometown discounts applied: {checked} checked, {matched} matched, {cleared} cleared.")
        if args.prefetch_headshots:
            conn = get_conn()
            cur = conn.cursor()
            cur.execute("SELECT DISTINCT mlbam_id FROM free_agents WHERE mlbam_id IS NOT NULL")
            ids = [int(r[0]) for r in cur.fetchall() if r[0]]
            conn.close()

            for i, mid in enumerate(ids, 1):
                ensure_headshot_cached(mid)
                if i % 50 == 0:
                    print(f"... {i}/{len(ids)}")
                time.sleep(0.15)  # polite
            print("✅ Headshots prefetched.")
        raise SystemExit(0)

    port = int(os.environ.get("PORT", "5000"))
    app.run(host="0.0.0.0", port=port, debug=False)

