#!/usr/bin/env python3
"""
Flask Free Agency App — BNSL FA Framework
----------------------------------------
Single-file Flask app implementing:
- Team/email login (same pattern as your draft app)
- Free agent list (CSV import, plus demo iconic MLB names if missing)
- Fixed-contract bidding (1–6 years + optional club option year, fixed AAV)
- Minimum AAV rules by exact contract structure (years + optional club option)
- Hometown multiplier (1.05x / 1.10x) applied to bid VALUE for hometown team bids
- Outbid rule: new bid must be >= 5% higher bid value than the current high bid
- 48-hour timer resets on each new bid; Friday/Saturday bids get +24h; auto-sign on expiry
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
from zoneinfo import ZoneInfo
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
import csv
import io
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

from bnsl_paths import cache_path, db_path, generated_path, input_path
from discord_notifier import send_discord_message

APP_DIR = Path(__file__).resolve().parent

# ---------- DB (module default; app factory can override via app.config["FA_DB_PATH"]) ----------
DB_PATH = Path(os.environ.get("FA_DB_PATH") or os.environ.get("DB_PATH") or str(db_path("fa.db")))
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
FREE_AGENTS_CSV = Path(os.environ.get("BNSL_FREE_AGENTS_CSV", str(input_path("free_agents.csv"))))
PLAYER_REGISTRY_CSV = Path(os.environ.get("BNSL_PLAYER_REGISTRY_CSV", str(input_path("player_registry.csv"))))

# Roster CSV is now the source of truth for the FA tab.
# Override with:
#   export BNSL_ROSTER_CSV=/path/to/rostered_2025service_BNSL_arb_updated.csv
ROSTER_CSV = Path(os.environ.get("BNSL_ROSTER_CSV", str(input_path("rostered_2025service_BNSL_arb_updated.csv"))))

# Live roster DB is the source of truth for free agency eligibility.
# The OOTP export is only used to backfill missing unsigned players into roster.db.
OOTP_FA_ROSTER = Path(os.environ.get(
    "BNSL_OOTP_FA_ROSTER",
    str(input_path("bnsl_ootp27_fixed_rosters_oldids_optionsupdated.txt")),
))

# OOTP ratings exports used to populate roster.db ovr/pot/def before the FA
# table mirrors those values into fa.db/free_agents.  The second file covers
# free agents who are not present in the main all-ratings export.  You can
# override both at once with BNSL_OOTP_RATINGS_CSVS=/path/a.csv,/path/b.csv
# or individually with the two env vars below.
OOTP_RATINGS_CSV = Path(os.environ.get(
    "BNSL_OOTP_RATINGS_CSV",
    str(input_path("bnsl_ootp2027_allratingsexport.csv")),
))
OOTP_FA_RATINGS_CSV = Path(os.environ.get(
    "BNSL_OOTP_FA_RATINGS_CSV",
    str(input_path("bnsl_ootp27_allratingsexport_fa.csv")),
))
HOMETOWN_DISCOUNTS_DB = Path(os.environ.get(
    "BNSL_HOMETOWN_DISCOUNTS_DB",
    str(generated_path("hometown_discounts.db")),
))
ROSTER_DB_SYNC_TTL_SECONDS = int(os.environ.get("BNSL_ROSTER_DB_SYNC_TTL_SECONDS", "15"))
CURRENT_SEASON = 2025
CURRENT_FA_CLASS = str(CURRENT_SEASON + 1)
QO_AAV_M = 22.773
FA_LOCK_META_KEY = "fa_locked"

# FA bidding deadlines are stored in UTC, but the league rule about Sunday
# processing is defined in US Eastern time.  Keep this as an IANA timezone so
# DST is handled correctly.
FA_BID_TIMEZONE_NAME = os.environ.get("BNSL_FA_TIMEZONE", "America/New_York")
FA_BID_TZ = ZoneInfo(FA_BID_TIMEZONE_NAME)


# To avoid reparsing a large roster on every request, sync if the file mtime changed
# or after this TTL. Set to 0 if you want every request to check the roster file.
ROSTER_SYNC_TTL_SECONDS = int(os.environ.get("BNSL_ROSTER_SYNC_TTL_SECONDS", "15"))

# Status values that still count as rostered even if active/expanded flags are false.
ROSTERED_STATUSES = {"ACTIVE", "RESERVE"}


HEADSHOT_DIR = Path(os.environ.get("BNSL_HEADSHOT_DIR", str(cache_path("player_images"))))
HEADSHOT_DIR.mkdir(parents=True, exist_ok=True)

from team_config import (
    MLB_TEAMS, TEAM_EMAILS, ABBR_TO_TEAM, TEAM_NAME_TO_ABBR,
    canonical_team_abbr, emails_equal,
)

# Roster tab stores franchise as abbreviations, while FA tab uses full team names.
TEAM_TO_ABBR = dict(TEAM_NAME_TO_ABBR)
TEAM_TO_ABBR.update({
    "Athletics": "OAK",
    "Kansas City Royals": "KC",
    "San Diego Padres": "SD",
    "San Francisco Giants": "SF",
    "Tampa Bay Rays": "TB",
    "Washington Nationals": "WAS",
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
    """Parse an ISO timestamp and return an aware UTC datetime."""
    text = str(s or "").strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    dt = datetime.fromisoformat(text)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def as_utc(dt: datetime) -> datetime:
    """Treat naive app timestamps as UTC and normalize aware timestamps to UTC."""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def is_fa_sunday(dt: datetime) -> bool:
    """Return True when a stored UTC deadline lands on Sunday in FA league time."""
    return as_utc(dt).astimezone(FA_BID_TZ).weekday() == 6


def bid_expiration_at(start: datetime, base_hours: int = 48) -> datetime:
    """
    Return the FA bid deadline for a clock that starts at ``start``.

    Standard FA bids run for 48 hours.  The league does not process FA bid
    expirations on Sundays, and "Sunday" is defined in US Eastern time
    (``America/New_York``), not UTC/GMT.

    Friday/Saturday Eastern starts still get the existing +24h extension.  As a
    final guard for admin-reset/custom timers, any computed deadline that lands
    on an Eastern Sunday is pushed forward another 24 hours.
    """
    start_utc = as_utc(start)
    hours = int(base_hours or 48)
    if start_utc.astimezone(FA_BID_TZ).weekday() in (4, 5):  # Friday=4, Saturday=5 in ET
        hours += 24

    expires_at = start_utc + timedelta(hours=hours)
    if is_fa_sunday(expires_at):
        expires_at += timedelta(hours=24)
    return expires_at


def bid_expiration_iso(start: datetime, base_hours: int = 48) -> str:
    return iso(bid_expiration_at(start, base_hours=base_hours))

def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(str(get_db_path()), timeout=30)
    conn.execute("PRAGMA busy_timeout=30000")
    conn.row_factory = sqlite3.Row

    def _unaccent(s):
        if s is None:
            return ""
        return "".join(ch for ch in unicodedata.normalize("NFKD", str(s)) if not unicodedata.combining(ch))
    conn.create_function("unaccent", 1, _unaccent)
    return conn


def ensure_fa_meta_schema(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS app_meta (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        )
        """
    )
    conn.commit()


def get_fa_meta(key: str, default: str = "") -> str:
    conn = get_conn()
    ensure_fa_meta_schema(conn)
    cur = conn.cursor()
    cur.execute("SELECT value FROM app_meta WHERE key=?", (key,))
    row = cur.fetchone()
    conn.close()
    return str(row["value"] if row else default)


def set_fa_meta(key: str, value: Any) -> None:
    conn = get_conn()
    ensure_fa_meta_schema(conn)
    conn.execute(
        """
        INSERT INTO app_meta(key, value)
        VALUES (?, ?)
        ON CONFLICT(key) DO UPDATE SET value=excluded.value
        """,
        (key, str(value)),
    )
    conn.commit()
    conn.close()


def is_fa_locked() -> bool:
    return get_fa_meta(FA_LOCK_META_KEY, "0").strip().lower() in {"1", "true", "yes", "on", "locked"}


def set_fa_locked(locked: bool) -> dict[str, Any]:
    """
    Lock/unlock free agency.

    Locked: public FA bids are blocked and active bids have no signing deadline.
    Unlocked: every active, unsigned bid receives a fresh signing clock; Friday/Saturday Eastern unlocks get +24h.
    """
    conn = get_conn()
    ensure_fa_meta_schema(conn)
    now = utcnow()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO app_meta(key, value)
        VALUES (?, ?)
        ON CONFLICT(key) DO UPDATE SET value=excluded.value
        """,
        (FA_LOCK_META_KEY, "1" if locked else "0"),
    )
    if locked:
        cur.execute(
            """
            UPDATE bids
            SET expires_at=NULL
            WHERE status='ACTIVE'
              AND player_id IN (
                SELECT id FROM free_agents
                WHERE signed_team IS NULL OR signed_team=''
              )
            """
        )
    else:
        unlock_exp = bid_expiration_iso(now)
        cur.execute(
            """
            UPDATE bids
            SET expires_at=?
            WHERE status='ACTIVE'
              AND player_id IN (
                SELECT id FROM free_agents
                WHERE signed_team IS NULL OR signed_team=''
              )
            """,
            (unlock_exp,),
        )
    affected = int(cur.rowcount or 0)
    conn.commit()
    conn.close()
    return {"locked": bool(locked), "active_bids_updated": affected}


def team_to_abbr_for_financials(team: Any) -> str:
    text = str(team or "").strip()
    if not text:
        return ""
    if text in TEAM_TO_ABBR:
        return TEAM_TO_ABBR[text]
    code = canonical_team_abbr(text)
    if code in ABBR_TO_TEAM:
        return code
    return TEAM_TO_ABBR.get(text, code)


def normalize_bid_team(team: Any) -> str:
    """Store FA bid teams as the full MLB team name whenever possible."""
    text = str(team or "").strip()
    if not text:
        return ""
    abbr = team_to_abbr_for_financials(text)
    return ABBR_TO_TEAM.get(abbr, text)


def bid_team_variants(team: Any) -> list[str]:
    full = normalize_bid_team(team)
    abbr = team_to_abbr_for_financials(team)
    variants = [full]
    if abbr and abbr not in variants:
        variants.append(abbr)
    raw = str(team or "").strip()
    if raw and raw not in variants:
        variants.append(raw)
    return variants


def active_bid_commitments_for_team(team: Any, excluding_player_id: int | None = None) -> float:
    """Return active FA bid AAV commitments for this team in dollars."""
    variants = bid_team_variants(team)
    if not variants:
        return 0.0
    placeholders = ",".join("?" for _ in variants)
    params: list[Any] = list(variants)
    where_exclude = ""
    if excluding_player_id is not None:
        where_exclude = "AND b.player_id<>?"
        params.append(int(excluding_player_id))
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        f"""
        SELECT COALESCE(SUM(COALESCE(b.aav_m, 0) * 1000000.0), 0) AS committed
        FROM bids b
        JOIN free_agents p ON p.id=b.player_id
        WHERE b.status='ACTIVE'
          AND b.team IN ({placeholders})
          AND (p.signed_team IS NULL OR p.signed_team='')
          {where_exclude}
        """,
        params,
    )
    row = cur.fetchone()
    conn.close()
    return float(row["committed"] or 0.0) if row else 0.0


def get_team_cap_summary(team: Any, excluding_player_id: int | None = None) -> dict[str, Any] | None:
    """
    Current cap space minus active FA bid commitments.
    Values are returned in dollars and millions for convenient API/UI use.
    """
    abbr = team_to_abbr_for_financials(team)
    if not abbr:
        return None
    try:
        from financials_app import compute_financial_rows
        rows = compute_financial_rows(abbr)
    except Exception:
        current_app.logger.exception("Unable to compute FA cap space for %s", team)
        return None
    if not rows:
        return None
    row = rows[0]
    committed = active_bid_commitments_for_team(team, excluding_player_id=excluding_player_id)
    hard_cap = float(row.get("hard_cap") or 0.0)
    cap_space = float(row.get("cap_space") or 0.0)
    surplus = float(row.get("surplus") or 0.0)
    available = cap_space - committed
    remaining_surplus = surplus - committed
    return {
        "team": normalize_bid_team(team),
        "abbr": abbr,
        "hard_cap": hard_cap,
        "cap_space": cap_space,
        "surplus": surplus,
        "active_bid_commitments": committed,
        "available_cap": available,
        "remaining_surplus": remaining_surplus,
        "hard_cap_m": hard_cap / 1000000.0,
        "cap_space_m": cap_space / 1000000.0,
        "surplus_m": surplus / 1000000.0,
        "active_bid_commitments_m": committed / 1000000.0,
        "available_cap_m": available / 1000000.0,
        "remaining_surplus_m": remaining_surplus / 1000000.0,
    }

def _require_authed_team() -> str:
    team = session.get("authed_team")
    if not team:
        abort(401, "Not logged in")
    return team

BID_VALUE_MIN_INCREMENT = 1.05

def min_aav_millions(years: int, has_option: bool) -> float:
    """
    Minimum salary rules (AAV), in $M, by exact contract structure.

    User-facing rules:
      1yr: $0.75M            1yr + option: $2.5M
      2yr: $1.5M             2yr + option: $5M
      3yr: $2.5M             3yr + option: $7M
      4yr: $5M               4yr + option: $10M
      5yr: $7M               5yr + option: $10M
      6yr: $10M              6yr + option: not allowed
    """
    years = clamp_int(years, 1, 6, 1)
    has_option = bool(has_option)

    if has_option and years >= 6:
        # Not offerable; callers should block 6yr+option separately.
        return 999999.0

    mins_no_option = {
        1: 0.75,
        2: 1.50,
        3: 2.50,
        4: 5.00,
        5: 7.00,
        6: 10.00,
    }
    mins_with_option = {
        1: 2.50,
        2: 5.00,
        3: 7.00,
        4: 10.00,
        5: 10.00,
    }
    mins = mins_with_option if has_option else mins_no_option
    return float(mins.get(years, 999999.0))

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


def contract_length_text(years: int, has_option: bool) -> str:
    years = max(1, int(years or 1))
    option_text = " + club option" if has_option else ""
    return f"{years} year{'s' if years != 1 else ''}{option_text}"


def contract_value_text(years: int, has_option: bool, aav_m: float) -> str:
    years = max(1, int(years or 1))
    aav_m = float(aav_m or 0.0)
    guaranteed_m = aav_m * years
    if has_option:
        max_total_m = aav_m * (years + 1)
        return f"{fmt_money_m(guaranteed_m)} guaranteed ({fmt_money_m(aav_m)} AAV; {fmt_money_m(max_total_m)} including option)"
    return f"{fmt_money_m(guaranteed_m)} total ({fmt_money_m(aav_m)} AAV)"


def contract_details_text(years: int, has_option: bool, aav_m: float) -> str:
    return f"{contract_length_text(years, has_option)}, {contract_value_text(years, has_option, aav_m)}"


def notify_free_agent_bid(player_name: str, team: str, years: int, has_option: bool, aav_m: float, bid_value_m: float) -> None:
    """Notify the transactions channel whenever a new active FA bid is placed."""
    player_name = str(player_name or "Unknown player").strip()
    team = normalize_bid_team(team)
    contract_text = contract_details_text(years, has_option, aav_m)
    send_discord_message(
        "BNSL_DISCORD_TRANSACTIONS_WEBHOOK_URL",
        f"**FA bid:** {player_name} — {team}, {contract_text}; equivalent 1-year value: {fmt_money_m(float(bid_value_m or 0.0))}.",
        fallback_label="transactions",
    )


def notify_free_agent_signing(player_name: str, team: str, years: int, has_option: bool, aav_m: float) -> None:
    """Notify the transactions channel when a bid expires into a signed FA contract."""
    player_name = str(player_name or "Unknown player").strip()
    team = normalize_bid_team(team)
    contract_text = contract_details_text(years, has_option, aav_m)
    send_discord_message(
        "BNSL_DISCORD_TRANSACTIONS_WEBHOOK_URL",
        f"**Free agent signing:** {team} signed {player_name} — {contract_text}.",
        fallback_label="transactions",
    )


def _log_discord_notification_exception(message: str) -> None:
    try:
        if has_app_context():
            current_app.logger.exception(message)
            return
    except Exception:
        pass
    logging.exception(message)


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
    ensure_column(conn, "free_agents", "ovr", "ovr INTEGER")
    ensure_column(conn, "free_agents", "pot", "pot INTEGER")
    ensure_column(conn, "free_agents", "def", "def INTEGER")

    # --- roster-sync columns: free_agents is now populated from the roster tab/CSV ---
    ensure_column(conn, "free_agents", "roster_player_id", "roster_player_id INTEGER")
    ensure_column(conn, "free_agents", "roster_source", "roster_source TEXT")
    ensure_column(conn, "free_agents", "is_roster_unrostered", "is_roster_unrostered INTEGER NOT NULL DEFAULT 0")
    ensure_column(conn, "free_agents", "last_seen_roster_sync", "last_seen_roster_sync TEXT")
    ensure_column(conn, "free_agents", "removed_from_roster_sync", "removed_from_roster_sync TEXT")

    # --- admin blacklist: players hidden from FA pool/watchlists/QOs without deleting their roster identity ---
    ensure_column(conn, "free_agents", "is_blacklisted", "is_blacklisted INTEGER NOT NULL DEFAULT 0")
    ensure_column(conn, "free_agents", "blacklisted_at", "blacklisted_at TEXT")
    ensure_column(conn, "free_agents", "blacklist_note", "blacklist_note TEXT")
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
        CREATE INDEX IF NOT EXISTS free_agents_blacklisted_idx
        ON free_agents(is_blacklisted)
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

    ensure_fa_meta_schema(conn)

    conn.commit()
    conn.close()

def ensure_fa_blacklist_schema() -> None:
    """Ensure blacklist columns exist before admin-only helpers touch older FA DBs."""
    conn = get_conn()
    try:
        ensure_column(conn, "free_agents", "is_blacklisted", "is_blacklisted INTEGER NOT NULL DEFAULT 0")
        ensure_column(conn, "free_agents", "blacklisted_at", "blacklisted_at TEXT")
        ensure_column(conn, "free_agents", "blacklist_note", "blacklist_note TEXT")
        conn.execute("""
            CREATE INDEX IF NOT EXISTS free_agents_blacklisted_idx
            ON free_agents(is_blacklisted)
        """)
        conn.commit()
    finally:
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
_last_roster_sync_signature: tuple[float | None, ...] | None = None
_last_roster_sync_checked_at: float = 0.0


def get_roster_db_path() -> Path:
    if has_app_context():
        cfg = current_app.config.get("ROSTER_DB_PATH")
        if cfg:
            return Path(cfg)
    return db_path("roster.db")


def get_ootp_fa_roster_path() -> Path:
    if has_app_context():
        cfg = current_app.config.get("OOTP_FA_ROSTER_PATH")
        if cfg:
            return Path(cfg)
    return OOTP_FA_ROSTER


def get_ootp_ratings_csv_paths() -> list[Path]:
    """Return the ordered OOTP ratings CSVs to merge for OVR/POT/DEF."""
    raw_paths: Any = None
    if has_app_context():
        raw_paths = current_app.config.get("OOTP_RATINGS_CSV_PATHS")

    if raw_paths is None:
        env_paths = os.environ.get("BNSL_OOTP_RATINGS_CSVS", "").strip()
        if env_paths:
            # Accept comma-separated or os.pathsep-separated lists.
            raw_paths = re.split(r"[," + re.escape(os.pathsep) + r"]+", env_paths)

    if raw_paths is None:
        raw_paths = [OOTP_RATINGS_CSV, OOTP_FA_RATINGS_CSV]
    elif isinstance(raw_paths, (str, Path)):
        raw_paths = [raw_paths]

    out: list[Path] = []
    seen: set[str] = set()
    for item in raw_paths:
        text = str(item or "").strip()
        if not text:
            continue
        path = Path(text)
        key = str(path)
        if key in seen:
            continue
        seen.add(key)
        out.append(path)
    return out


def get_hometown_discounts_db_path() -> Path:
    if has_app_context():
        cfg = current_app.config.get("HOMETOWN_DISCOUNTS_DB_PATH")
        if cfg:
            return Path(cfg)
    return HOMETOWN_DISCOUNTS_DB


def get_roster_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(str(get_roster_db_path()), timeout=30)
    conn.execute("PRAGMA busy_timeout=30000")
    conn.row_factory = sqlite3.Row

    def _unaccent(s):
        if s is None:
            return ""
        return "".join(
            ch for ch in unicodedata.normalize("NFKD", str(s))
            if not unicodedata.combining(ch)
        )

    conn.create_function("unaccent", 1, _unaccent)
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
    ensure_roster_column(conn, "ovr", "ovr INTEGER")
    ensure_roster_column(conn, "pot", "pot INTEGER")
    ensure_roster_column(conn, "def", "def INTEGER")
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


def _optional_int(value: Any) -> int | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        return int(float(text))
    except Exception:
        return None


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


NAME_SUFFIX_TOKENS = {
    "jr", "junior", "sr", "senior",
    "ii", "iii", "iv", "v", "vi", "vii", "viii", "ix", "x",
}


def _is_name_suffix(value: Any) -> bool:
    return _norm_token(value) in NAME_SUFFIX_TOKENS


def _name_words(value: Any) -> list[str]:
    # Keep the displayed name untouched elsewhere, but tokenize loosely for matching.
    return [b.strip(".,;:()[]{}") for b in re.split(r"\s+", str(value or "").strip()) if b.strip(".,;:()[]{}")]


def _strip_trailing_name_suffixes(words: list[str]) -> list[str]:
    out = list(words or [])
    while out and _is_name_suffix(out[-1]):
        out.pop()
    return out


def _compact_suffix_stripped_tokens(token: str) -> list[str]:
    """Return suffix-stripped forms for already-normalized compact tokens.

    The hometown-discount key table stores tokens after punctuation has already
    been removed, so "Leiter Jr." may appear as "leiterjr" rather than
    as separable words.  Keep this as aliases rather than replacing the exact
    key, and let caller-side duplicate handling avoid unsafe collisions.
    """
    tok = _norm_token(token)
    if not tok:
        return []

    # Jr./Sr. are the main pain point and are reasonably safe to strip when
    # compacted.  Roman numerals are more collision-prone as ordinary word
    # endings, so only strip those when they were separated before normalization.
    out: list[str] = []
    for suffix in ("jr", "sr"):
        if tok.endswith(suffix) and len(tok) > len(suffix) + 1:
            base = tok[:-len(suffix)]
            if base and base not in out:
                out.append(base)
    return out


def _canonical_last_name_tokens(value: Any) -> list[str]:
    """Return last-name match tokens with generational suffixes removed.

    This lets rows like "Mark Leiter" and "Mark Leiter Jr." line up
    even when one export puts the suffix in the name and the other does not.
    For multi-word last names, keep both the compact full last name and the
    terminal token as fallbacks, while preserving duplicate-key protection.
    """
    words = _name_words(value)
    stripped = _strip_trailing_name_suffixes(words)

    candidates: list[str] = []
    if stripped:
        candidates.append(" ".join(stripped))
        candidates.append(stripped[-1])

    # Keep the raw token as a low-priority fallback only when it is not just a suffix.
    if words and not _is_name_suffix(" ".join(words)):
        candidates.append(" ".join(words))

    out: list[str] = []
    for cand in candidates:
        tok = _norm_token(cand)
        for alias in [tok, *_compact_suffix_stripped_tokens(tok)]:
            if alias and alias not in NAME_SUFFIX_TOKENS and alias not in out:
                out.append(alias)
    return out


def _canonical_last_name_token(value: Any) -> str:
    tokens = _canonical_last_name_tokens(value)
    return tokens[0] if tokens else ""


def _canonical_player_name_key(value: Any) -> str:
    first, last = _split_name(str(value or ""))
    last_token = _canonical_last_name_token(last)
    first_token = _norm_token(first)
    if first_token and last_token:
        return f"{first_token}:{last_token}"
    return last_token


def _split_name(name: str) -> tuple[str, str]:
    bits = _strip_trailing_name_suffixes(_name_words(name))
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
    last_tokens = _canonical_last_name_tokens(last_name)
    birth = _norm_token(dob)
    tm = _norm_token(team)
    bbref = _norm_token(bbref_id)
    bbrefm = _norm_token(bbrefminors_id)
    oid = _norm_token(ootp_id)

    if not last_tokens:
        return []

    keys: list[str] = []

    def add(key: str) -> None:
        if key and key not in keys:
            keys.append(key)

    for last in last_tokens:
        if bbref:
            add(f"bbref:{bbref}:{last}")
        if bbrefm:
            add(f"bbrefminors:{bbrefm}:{last}")
        if oid:
            add(f"ootp:{oid}:{last}")
        if first and tm:
            add(f"first_last_team:{first}:{last}:{tm}")
        if first and birth:
            add(f"first_last_dob:{first}:{last}:{birth}")
        if birth:
            add(f"last_dob:{last}:{birth}")
        if first:
            add(f"first_last:{first}:{last}")
    return keys


def _roster_row_match_keys(row: sqlite3.Row) -> list[str]:
    first = row["first_name"] if "first_name" in row.keys() else ""
    last = row["last_name"] if "last_name" in row.keys() else ""
    if not first or not last or _is_name_suffix(last):
        f2, l2 = _split_name(row["name"] if "name" in row.keys() else "")
        first = first or f2
        last = l2 if (not last or _is_name_suffix(last)) else last
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
    """Build suffix-tolerant match keys for a free_agents row against hometown_discounts.db."""
    name = str(_htd_row_value(roster_row, "name") or fa_row["name"] or "").strip()
    first = str(_htd_row_value(roster_row, "first_name") or "").strip()
    last = str(_htd_row_value(roster_row, "last_name") or "").strip()
    if not first or not last or _is_name_suffix(last):
        f2, l2 = _split_name(name)
        first = first or f2
        last = l2 if (not last or _is_name_suffix(last)) else last

    first_n = _norm_token(first)
    last_tokens = _canonical_last_name_tokens(last)
    if not last_tokens:
        return []

    keys: list[str] = []

    def add(key: str) -> None:
        if key and key not in keys:
            keys.append(key)

    for last_n in last_tokens:
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


def _hometown_discount_key_aliases(key: Any) -> list[str]:
    """Return suffix-tolerant aliases for stored hometown-discount keys.

    hometown_discounts.db is generated before the FA app applies current name
    cleanup, so its key table may contain last-name tokens like ``leiterjr``.
    The FA-side candidate keys now prefer ``leiter``.  Expanding the loaded key
    map here lets old/generated HTD keys match without forcing a rebuild.
    """
    raw = str(key or "").strip()
    if not raw:
        return []

    parts = raw.split(":")
    aliases: list[str] = []

    def add(value: str) -> None:
        if value and value not in aliases:
            aliases.append(value)

    add(raw)

    # Identify the component that represents last name for each key family.
    last_idx_by_prefix = {
        "bbref": 2,
        "bbrefminors": 2,
        "ootp": 2,
        "first_last_team": 2,
        "first_last_dob": 2,
        "last_dob": 1,
        "first_last": 2,
    }
    idx = last_idx_by_prefix.get(parts[0])
    if idx is None or idx >= len(parts):
        return aliases

    last_aliases = _canonical_last_name_tokens(parts[idx])
    for last_alias in last_aliases:
        new_parts = list(parts)
        new_parts[idx] = last_alias
        add(":".join(new_parts))

    return aliases


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

    # Exact stored keys always win.  Generated suffix aliases are added only
    # when they do not conflict with another discount claim.  That prevents an
    # unsafe first_last alias from assigning a discount when both Jr./Sr. rows
    # exist and only a stronger DOB/team/ID key should decide the match.
    rows = [dict(r) for r in cur.fetchall()]
    conn.close()

    key_map: dict[str, dict[str, Any]] = {}
    ambiguous_aliases: set[str] = set()

    for rec in rows:
        raw_key = str(rec.get("key") or "")
        if raw_key:
            key_map[raw_key] = rec

    for rec in rows:
        raw_key = str(rec.get("key") or "")
        for alias in _hometown_discount_key_aliases(raw_key):
            if alias == raw_key or alias in ambiguous_aliases:
                continue
            existing = key_map.get(alias)
            if existing is not None and existing.get("discount_id") != rec.get("discount_id"):
                key_map.pop(alias, None)
                ambiguous_aliases.add(alias)
                continue
            key_map[alias] = rec

    return key_map


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


def assign_hometown_discounts_now(clear_missing: bool = True) -> dict[str, int]:
    """Explicit HTD maintenance action for CLI and /fa/history button."""
    checked, matched, cleared = apply_hometown_discounts_to_free_agents(clear_missing=clear_missing)
    return {
        "checked": int(checked),
        "matched": int(matched),
        "cleared": int(cleared),
    }


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


def _normalize_rating_dob(value: Any) -> str:
    """Normalize OOTP ratings DOB values like MM/DD/YYYY to YYYY-MM-DD."""
    text = str(value or "").strip()
    if not text or text in {"-", "—"}:
        return ""
    for fmt in ("%m/%d/%Y", "%m-%d-%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(text, fmt).strftime("%Y-%m-%d")
        except Exception:
            pass
    return text


def _rating_optional_int(value: Any) -> int | None:
    text = str(value or "").strip()
    if not text or text in {"-", "—"}:
        return None
    try:
        return int(float(text))
    except Exception:
        return None


def _rating_row_match_keys(row: dict[str, str]) -> list[str]:
    name = _row_get_ci(row, "Name", "player")
    first = _row_get_ci(row, "First Name", "FirstName", "first_name")
    last = _row_get_ci(row, "Last Name", "LastName", "last_name")
    if not first or not last or _is_name_suffix(last):
        f2, l2 = _split_name(name)
        first = first or f2
        last = l2 if (not last or _is_name_suffix(last)) else last
    dob = _normalize_rating_dob(_row_get_ci(row, "DOB", "date_of_birth", "birth_date"))
    team = _row_get_ci(row, "TM", "Team", "Team Name", "franchise")
    ootp_id = _row_get_ci(row, "ID", "id", "ootp_id")
    return _player_match_keys(first, last, dob, team, "", "", ootp_id)


def _iter_ootp_ratings_csv(path: Path):
    """Yield OOTP all-ratings CSV rows with case-insensitive DictReader keys."""
    with Path(path).open(newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if not row:
                continue
            # Keep original keys for _row_get_ci, but normalize None values.
            yield {str(k or "").strip(): str(v or "").strip() for k, v in row.items()}


def _load_combined_ootp_rating_map(paths: list[Path]) -> tuple[dict[str, dict[str, Any]], set[str], int, int]:
    """
    Merge OOTP ratings exports into a key map.

    Later files in the ordered path list overwrite earlier files for the same
    player signature, so the FA ratings export can fill or supersede rows from
    the main ratings export. Ambiguous name-only keys are marked duplicate and
    skipped during matching, preserving the duplicate-player handling used
    elsewhere in the app.
    """
    rating_by_key: dict[str, dict[str, Any]] = {}
    owner_by_key: dict[str, str] = {}
    duplicate_keys: set[str] = set()
    files_loaded = 0
    rows_loaded = 0

    for source_index, path in enumerate(paths):
        if not path.exists():
            logging.info("OOTP ratings CSV skipped; file does not exist: %s", path)
            continue
        files_loaded += 1
        for row in _iter_ootp_ratings_csv(path):
            name = _row_get_ci(row, "Name", "player")
            first = _row_get_ci(row, "First Name", "FirstName", "first_name")
            last = _row_get_ci(row, "Last Name", "LastName", "last_name")
            if not first or not last:
                f2, l2 = _split_name(name)
                first = first or f2
                last = last or l2
            if not last:
                continue

            dob = _normalize_rating_dob(_row_get_ci(row, "DOB", "date_of_birth", "birth_date"))
            ootp_id = _row_get_ci(row, "ID", "id", "ootp_id")
            signature_last = _canonical_last_name_token(last)
            signature = _norm_token(ootp_id) or f"{_norm_token(first)}:{signature_last}:{_norm_token(dob)}"
            if not signature:
                continue

            rec = {
                "name": name or f"{first} {last}".strip(),
                "first_name": first,
                "last_name": last,
                "dob": dob,
                "ootp_id": _parse_positive_int(ootp_id),
                "ovr": _rating_optional_int(_row_get_ci(row, "OVR")),
                "pot": _rating_optional_int(_row_get_ci(row, "POT")),
                "def": _rating_optional_int(_row_get_ci(row, "DEF")),
                "source_path": str(path),
                "source_index": source_index,
            }
            rows_loaded += 1

            for key in _rating_row_match_keys(row):
                prev_owner = owner_by_key.get(key)
                if prev_owner is not None and prev_owner != signature:
                    duplicate_keys.add(key)
                    continue
                owner_by_key[key] = signature
                rating_by_key[key] = rec

    return rating_by_key, duplicate_keys, files_loaded, rows_loaded


def update_roster_db_ratings_from_exports(paths: list[Path] | None = None) -> tuple[int, int, int]:
    """
    Refresh roster_players.ovr/pot/def from the merged OOTP ratings exports.

    The FA table already copies these columns from roster.db into free_agents.
    Running this before sync_free_agents_from_roster_db makes the FA page pull
    ratings from the combined main + FA all-ratings CSV set.

    Returns: (files_loaded, rating_rows_loaded, roster_rows_updated)
    """
    roster_path = get_roster_db_path()
    if not roster_path.exists():
        logging.warning("OOTP ratings sync skipped: roster.db does not exist: %s", roster_path)
        return (0, 0, 0)

    rating_paths = paths or get_ootp_ratings_csv_paths()
    rating_by_key, duplicate_keys, files_loaded, rows_loaded = _load_combined_ootp_rating_map(rating_paths)
    if not rating_by_key:
        return (files_loaded, rows_loaded, 0)

    conn = get_roster_conn()
    cur = conn.cursor()
    ensure_roster_fa_import_columns(conn)
    cur.execute("SELECT * FROM roster_players")
    roster_rows = cur.fetchall()

    updated = 0
    for roster_row in roster_rows:
        match = None
        for key in _roster_row_match_keys(roster_row):
            if key in duplicate_keys:
                continue
            rec = rating_by_key.get(key)
            if rec is not None:
                match = rec
                break
        if match is None:
            continue

        cur.execute("""
            UPDATE roster_players
            SET ovr=?, pot=?, def=?
            WHERE id=?
        """, (
            match["ovr"],
            match["pot"],
            match["def"],
            int(roster_row["id"]),
        ))
        updated += 1

    conn.commit()
    conn.close()
    logging.info(
        "OOTP ratings sync complete: %s files, %s rating rows, %s roster rows updated",
        files_loaded, rows_loaded, updated,
    )
    return (files_loaded, rows_loaded, updated)


def _roster_db_row_is_unrostered(row: sqlite3.Row) -> bool:
    franchise = str(row["franchise"] or "").strip()
    roster_status = str(row["roster_status"] or "").strip()
    signed = _safe_int(row["signed"] if "signed" in row.keys() else 0, 0)
    return (franchise == "") or (signed == 0) or (roster_status == "")


def _find_existing_free_agent_id(
    cur: sqlite3.Cursor,
    roster_id: int | None,
    mlbam_id: int | None,
    fg_id: int | None,
    name: str,
    *,
    allow_name_fallback: bool = True,
) -> int | None:
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

    # Last fallback: exact unaccented name match.  This is useful for preserving
    # older seeded/manual FA rows that predate roster_player_id, but it must not
    # run for duplicate roster names; otherwise several players named e.g.
    # "Luis Garcia" all collapse onto the same free_agents row.
    if allow_name_fallback and name:
        cur.execute("""
            SELECT id
            FROM free_agents
            WHERE LOWER(unaccent(name)) = LOWER(unaccent(?))
              AND (roster_player_id IS NULL OR roster_player_id='')
            ORDER BY id ASC
            LIMIT 1
        """, (name,))
        r = cur.fetchone()
        if r:
            return int(r["id"])

        # Suffix-insensitive fallback for legacy rows with names like
        # "Mark Leiter" vs "Mark Leiter Jr." and no stable ID yet.
        target_name_key = _canonical_player_name_key(name)
        if target_name_key:
            cur.execute("""
                SELECT id, name
                FROM free_agents
                WHERE (roster_player_id IS NULL OR roster_player_id='')
                ORDER BY id ASC
            """)
            matches = [
                int(row["id"])
                for row in cur.fetchall()
                if _canonical_player_name_key(row["name"]) == target_name_key
            ]
            if len(matches) == 1:
                return matches[0]

    return None


def sync_free_agent_from_roster_player_id(player_id: int, *, last_team_abbr: str = "") -> dict[str, Any]:
    """
    Lightweight roster->FA mirror for one player.

    Use this from roster/admin release-style actions instead of the full FA
    maintenance sync. It reads exactly one roster_players row and either upserts
    that player into free_agents when he is currently unrostered, or marks the
    existing FA row as no longer roster-unrostered if he is back on a roster.
    It deliberately does not import OOTP FA rows, ratings, draft rows, or Rule V.
    """
    rid = int(player_id or 0)
    if rid <= 0:
        raise ValueError("Missing roster player id")

    roster_path = get_roster_db_path()
    if not roster_path.exists():
        return {"roster_player_id": rid, "changed": False, "reason": "roster.db missing"}

    now_text = iso(utcnow())
    rconn = get_roster_conn()
    rcur = rconn.cursor()
    ensure_roster_fa_import_columns(rconn)
    rcur.execute("SELECT * FROM roster_players WHERE id=?", (rid,))
    r = rcur.fetchone()
    rconn.close()
    if not r:
        return {"roster_player_id": rid, "changed": False, "reason": "roster player not found"}

    conn = get_conn()
    cur = conn.cursor()
    try:
        if not _roster_db_row_is_unrostered(r):
            cur.execute(
                """
                UPDATE free_agents
                SET is_roster_unrostered=0,
                    removed_from_roster_sync=?
                WHERE roster_player_id=?
                  AND roster_source IN ('roster_db', 'roster_csv', 'ootp27')
                """,
                (now_text, rid),
            )
            changed = int(cur.rowcount or 0)
            conn.commit()
            return {
                "roster_player_id": rid,
                "free_agent_id": None,
                "changed": bool(changed),
                "is_free_agent": False,
                "action": "marked_not_unrostered",
            }

        mlbam_id = _parse_positive_int(r["mlbam_id"] if "mlbam_id" in r.keys() else None)
        fg_id = _parse_positive_int(r["fangraphs_id"] if "fangraphs_id" in r.keys() else None)
        name = str(r["name"] or "").strip()
        if not name:
            return {"roster_player_id": rid, "changed": False, "reason": "blank roster player name"}
        pos = str(r["position"] or "").strip()
        roster_last_team = roster_code_to_team(r["franchise"] if "franchise" in r.keys() else "")
        explicit_last_team = roster_code_to_team(last_team_abbr) if last_team_abbr else ""
        last_team = explicit_last_team or roster_last_team
        ovr = _optional_int(r["ovr"] if "ovr" in r.keys() else None)
        pot = _optional_int(r["pot"] if "pot" in r.keys() else None)
        def_rating = _optional_int(r["def"] if "def" in r.keys() else None)

        existing_id = _find_existing_free_agent_id(
            cur, rid, mlbam_id, fg_id, name,
            allow_name_fallback=True,
        )
        if existing_id is None:
            cur.execute(
                """
                INSERT INTO free_agents(
                    name, position, last_team,
                    hometown_team, hometown_seasons, seed_qo,
                    mlbam_id, fangraphs_id, ovr, pot, def,
                    roster_player_id, roster_source, is_roster_unrostered,
                    last_seen_roster_sync, removed_from_roster_sync
                )
                VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    name, pos, last_team,
                    "", 0, 0,
                    mlbam_id, fg_id, ovr, pot, def_rating,
                    rid, "roster_db", 1,
                    now_text, None,
                ),
            )
            free_agent_id = int(cur.lastrowid)
            action = "inserted"
        else:
            cur.execute(
                """
                UPDATE free_agents
                SET name=?,
                    position=COALESCE(NULLIF(?, ''), position),
                    last_team=COALESCE(NULLIF(?, ''), last_team),
                    mlbam_id=CASE WHEN ? IS NULL THEN mlbam_id ELSE ? END,
                    fangraphs_id=COALESCE(?, fangraphs_id),
                    ovr=COALESCE(?, ovr),
                    pot=COALESCE(?, pot),
                    def=COALESCE(?, def),
                    roster_player_id=?,
                    roster_source='roster_db',
                    is_roster_unrostered=1,
                    last_seen_roster_sync=?,
                    removed_from_roster_sync=NULL,
                    signed_team=NULL,
                    signed_at=NULL
                WHERE id=?
                """,
                (
                    name, pos, last_team, mlbam_id, mlbam_id, fg_id,
                    ovr, pot, def_rating, rid, now_text, existing_id,
                ),
            )
            free_agent_id = int(existing_id)
            action = "updated"

        conn.commit()
        try:
            assign_hometown_discounts_now(clear_missing=False)
        except Exception:
            logging.exception("Unable to refresh HTD assignment after single-player FA sync")
        return {
            "roster_player_id": rid,
            "free_agent_id": free_agent_id,
            "changed": True,
            "is_free_agent": True,
            "action": action,
            "player_name": name,
        }
    finally:
        conn.close()

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

    # Duplicate player names are common enough that name-only matching cannot be
    # used safely for every unrostered row.  Count the roster-side names first so
    # the old name fallback is only allowed for names that uniquely identify one
    # current unrostered roster row.  Roster/player IDs still remain the primary
    # identity key, so existing rows with roster_player_id continue to update in
    # place and keep their bids/watchlist state.
    roster_name_counts: dict[str, int] = {}
    for rr in roster_rows:
        key = _canonical_player_name_key(rr["name"] if "name" in rr.keys() else "")
        if key:
            roster_name_counts[key] = roster_name_counts.get(key, 0) + 1

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
        ovr = _optional_int(r["ovr"] if "ovr" in r.keys() else None)
        pot = _optional_int(r["pot"] if "pot" in r.keys() else None)
        def_rating = _optional_int(r["def"] if "def" in r.keys() else None)

        name_key = _canonical_player_name_key(name)
        allow_name_fallback = bool(name_key and roster_name_counts.get(name_key, 0) == 1)
        existing_id = _find_existing_free_agent_id(
            cur, roster_id, mlbam_id, fg_id, name,
            allow_name_fallback=allow_name_fallback,
        )

        if existing_id is None:
            cur.execute("""
                INSERT INTO free_agents(
                    name, position, last_team,
                    hometown_team, hometown_seasons, seed_qo,
                    mlbam_id, fangraphs_id, ovr, pot, def,
                    roster_player_id, roster_source, is_roster_unrostered,
                    last_seen_roster_sync, removed_from_roster_sync
                )
                VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """, (
                name, pos, last_team,
                "", 0, 0,
                mlbam_id, fg_id, ovr, pot, def_rating,
                roster_id, "roster_db", 1,
                now_text, None,
            ))
        else:
            cur.execute("""
                UPDATE free_agents
                SET name=?,
                    position=COALESCE(NULLIF(?, ''), position),
                    last_team=COALESCE(NULLIF(?, ''), last_team),
                    mlbam_id=CASE WHEN ? IS NULL THEN mlbam_id ELSE ? END,
                    fangraphs_id=COALESCE(?, fangraphs_id),
                    ovr=?,
                    pot=?,
                    def=?,
                    roster_player_id=?,
                    roster_source='roster_db',
                    is_roster_unrostered=1,
                    last_seen_roster_sync=?,
                    removed_from_roster_sync=NULL
                WHERE id=?
            """, (
                name, pos, last_team, mlbam_id, mlbam_id, fg_id, ovr, pot, def_rating,
                roster_id, now_text, existing_id,
            ))
        upserted += 1

    conn.commit()
    conn.close()
    logging.info("Roster DB FA sync complete: %s unrostered, %s upserted", len(roster_rows), upserted)
    return (len(roster_rows), upserted)


def update_roster_db_for_fa_signing(
    roster_player_id: int | None,
    team: str,
    years: int,
    has_option: bool,
    aav_m: float,
    *,
    player_name: str = "",
    mlbam_id: int | None = None,
    fangraphs_id: int | None = None,
) -> dict[str, Any]:
    """
    Apply an official FA signing to roster.db.

    The FA app stores the bid/signing state in fa.db, while roster pages and
    financials read ownership and salary from roster.db.  This function is the
    handoff point: when an ACTIVE bid expires into SIGNED, the linked roster row
    must be assigned to the winning franchise with the negotiated contract.

    roster_player_id is still the primary identity key, but older FA rows can
    miss it.  In that case, fall back to stable IDs and finally an exact
    unaccented name match so a signing is not silently left off the roster.
    """
    roster_path = get_roster_db_path()
    if not roster_path.exists():
        return {"updated": False, "reason": f"roster DB missing: {roster_path}"}

    team_abbr = team_to_abbr_for_financials(team) or TEAM_TO_ABBR.get(team, str(team or "").strip())
    if not team_abbr:
        return {"updated": False, "reason": "missing team"}

    start_year = int(CURRENT_FA_CLASS)
    guaranteed_years = max(1, int(years or 1))
    total_years = guaranteed_years + (1 if has_option else 0)
    contract_expires = start_year + total_years - 1
    new_fa_class = str(contract_expires + 1)

    conn = get_roster_conn()
    cur = conn.cursor()
    try:
        ensure_roster_fa_import_columns(conn)

        resolved_id: int | None = int(roster_player_id) if roster_player_id else None
        if resolved_id:
            cur.execute("SELECT id FROM roster_players WHERE id=?", (resolved_id,))
            if not cur.fetchone():
                resolved_id = None

        if resolved_id is None and mlbam_id:
            cur.execute(
                """
                SELECT id
                FROM roster_players
                WHERE mlbam_id=?
                ORDER BY CASE WHEN COALESCE(franchise,'')='' THEN 0 ELSE 1 END, id
                LIMIT 1
                """,
                (int(mlbam_id),),
            )
            row = cur.fetchone()
            if row:
                resolved_id = int(row["id"])

        if resolved_id is None and fangraphs_id:
            cur.execute(
                """
                SELECT id
                FROM roster_players
                WHERE CAST(fangraphs_id AS TEXT)=CAST(? AS TEXT)
                ORDER BY CASE WHEN COALESCE(franchise,'')='' THEN 0 ELSE 1 END, id
                LIMIT 1
                """,
                (str(fangraphs_id),),
            )
            row = cur.fetchone()
            if row:
                resolved_id = int(row["id"])

        clean_name = str(player_name or "").strip()
        if resolved_id is None and clean_name:
            cur.execute(
                """
                SELECT id
                FROM roster_players
                WHERE LOWER(unaccent(name)) = LOWER(unaccent(?))
                ORDER BY CASE WHEN COALESCE(franchise,'')='' THEN 0 ELSE 1 END, id
                LIMIT 1
                """,
                (clean_name,),
            )
            row = cur.fetchone()
            if row:
                resolved_id = int(row["id"])

        if resolved_id is None:
            return {"updated": False, "reason": "no matching roster player", "roster_player_id": None}

        salary = float(aav_m or 0.0) * 1_000_000.0
        cur.execute("SELECT * FROM roster_players WHERE id=?", (resolved_id,))
        existing = cur.fetchone()
        existing_status = str(existing["roster_status"] or "") if existing else ""
        existing_active = int(existing["active_roster"] or 0) if existing else 0
        already_on_matching_contract = bool(
            existing
            and str(existing["franchise"] or "") == team_abbr
            and int(existing["signed"] or 0) == 1
            and str(existing["contract_type"] or "").strip().upper() == "FA"
            and abs(float(existing["salary"] or 0.0) - salary) < 0.5
            and int(existing["contract_initial_season"] or 0) == start_year
            and int(existing["contract_length"] or 0) == total_years
            and int(existing["contract_option"] or 0) == (1 if has_option else 0)
            and str(existing["contract_expires"] or "") == str(contract_expires)
            and str(existing["fa_class"] or "") == new_fa_class
            and existing_status in {"Active", "40-man", "Reserve"}
        )
        roster_status = existing_status if already_on_matching_contract else "Reserve"
        active_roster = existing_active if already_on_matching_contract and existing_status == "Active" else 0

        cur.execute(
            """
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
                roster_status=?,
                active_roster=?,
                fa_class=?,
                rulev_status=0,
                rulev_original_team=NULL,
                rulev_selected_by=NULL,
                rulev_selected_at=NULL
            WHERE id=?
            """,
            (
                salary,
                start_year,
                total_years,
                1 if has_option else 0,
                str(contract_expires),
                team_abbr,
                roster_status,
                active_roster,
                new_fa_class,
                resolved_id,
            ),
        )
        conn.commit()
        return {
            "updated": bool(cur.rowcount),
            "roster_player_id": resolved_id,
            "team_abbr": team_abbr,
            "salary": float(aav_m or 0.0) * 1_000_000.0,
            "contract_initial_season": start_year,
            "contract_length": total_years,
            "contract_option": 1 if has_option else 0,
            "contract_expires": str(contract_expires),
            "fa_class": new_fa_class,
        }
    finally:
        conn.close()


def reconcile_signed_free_agents_to_roster() -> dict[str, Any]:
    """
    Backfill/repair roster.db from already-signed FA rows.

    This is intentionally cheap relative to the full roster sync: it only scans
    signed FA rows and their one SIGNED bid.  Running it on bootstrap protects
    against older deployments where fa.db recorded a signing but roster.db was
    not updated, which would also make financials miss the salary.
    """
    if not get_roster_db_path().exists():
        return {"checked": 0, "updated": 0, "missing": 0, "reason": "roster DB missing"}

    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT
            p.id AS fa_id, p.name, p.roster_player_id, p.signed_team,
            p.mlbam_id, p.fangraphs_id,
            b.years, b.has_option, b.aav_m
        FROM free_agents p
        JOIN bids b ON b.player_id=p.id AND b.status='SIGNED'
        WHERE COALESCE(p.signed_team,'')<>''
        ORDER BY datetime(COALESCE(p.signed_at, b.expires_at, b.created_at)) ASC
        """
    )
    rows = cur.fetchall()

    updated = 0
    missing: list[str] = []
    stamp = iso(utcnow())

    try:
        for r in rows:
            result = update_roster_db_for_fa_signing(
                int(r["roster_player_id"] or 0) if r["roster_player_id"] else None,
                r["signed_team"],
                int(r["years"] or 1),
                bool(int(r["has_option"] or 0)),
                float(r["aav_m"] or 0.0),
                player_name=str(r["name"] or ""),
                mlbam_id=int(r["mlbam_id"] or 0) if r["mlbam_id"] else None,
                fangraphs_id=int(r["fangraphs_id"] or 0) if r["fangraphs_id"] else None,
            )
            if result.get("updated"):
                updated += 1
                resolved_id = result.get("roster_player_id")
                cur.execute(
                    """
                    UPDATE free_agents
                    SET roster_player_id=COALESCE(?, roster_player_id),
                        is_roster_unrostered=0,
                        removed_from_roster_sync=?
                    WHERE id=?
                    """,
                    (resolved_id, stamp, int(r["fa_id"])),
                )
            else:
                missing.append(str(r["name"] or f"FA #{r['fa_id']}"))
        conn.commit()
    finally:
        conn.close()

    if missing:
        logging.warning("Signed FA roster reconciliation could not match: %s", ", ".join(missing[:20]))
    return {"checked": len(rows), "updated": updated, "missing": len(missing), "missing_players": missing}


def sync_free_agents_from_roster_if_needed(force: bool = False) -> None:
    """
    Explicit maintenance sync.

    This is intentionally force-only now. Normal page loads, search endpoints,
    and admin autocomplete must not run it: it scans thousands of players and
    writes roster.db/fa.db, which can create SQLite lock contention and noisy
    repeated OOTP/rating/Rule V refreshes. Use force=True from an explicit admin
    maintenance action or CLI command. For ordinary releases/non-tenders, call
    sync_free_agent_from_roster_player_id(player_id) instead.
    """
    global _last_roster_sync_signature, _last_roster_sync_checked_at

    if not force:
        return

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

    ratings_mtimes: list[float | None] = []
    for ratings_path in get_ootp_ratings_csv_paths():
        try:
            ratings_mtimes.append(ratings_path.stat().st_mtime if ratings_path.exists() else None)
        except OSError:
            ratings_mtimes.append(None)

    signature = (roster_mtime, ootp_mtime, *ratings_mtimes)
    ttl_expired = (ROSTER_DB_SYNC_TTL_SECONDS <= 0) or ((now - _last_roster_sync_checked_at) >= ROSTER_DB_SYNC_TTL_SECONDS)
    changed = (_last_roster_sync_signature is None) or (signature != _last_roster_sync_signature)

    if force:
        if ootp_path.exists():
            import_ootp_free_agents_into_roster_db(ootp_path)
        update_roster_db_ratings_from_exports()
        sync_free_agents_from_roster_db()
        # Preserve the existing HTD machinery, but make sure any newly inserted
        # roster-derived FA rows get their hometown data.  clear_missing=False
        # avoids wiping manually reviewed/previously assigned discounts during
        # routine roster syncs; the explicit HTD maintenance action still uses
        # clear_missing=True when a full refresh is desired.
        try:
            assign_hometown_discounts_now(clear_missing=False)
        except Exception:
            logging.exception("Unable to refresh HTD assignments after roster FA sync")
        try:
            roster_mtime = roster_path.stat().st_mtime
        except OSError:
            pass
        # Recompute after writes so the cache reflects the post-sync state.
        try:
            ratings_mtimes = [
                (path.stat().st_mtime if path.exists() else None)
                for path in get_ootp_ratings_csv_paths()
            ]
        except OSError:
            ratings_mtimes = []
        _last_roster_sync_signature = (roster_mtime, ootp_mtime, *ratings_mtimes)
        _last_roster_sync_checked_at = now

def seed_qualifying_offers(qo_aav_m: float = QO_AAV_M):
    """
    Seed players with seed_qo=1 with an opening QO bid:
    - 1 year
    - no option
    - AAV = qo_aav_m
    - bidder team = last_team (or hometown_team if last_team missing)
    - 48-hour timer starts immediately; Friday/Saturday starts get +24h
    """
    now = utcnow()
    exp = None if is_fa_locked() else bid_expiration_at(now)

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
        team = normalize_bid_team((r["last_team"] or r["hometown_team"] or "").strip())
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
        """, (pid, team, 1, 0, float(qo_aav_m), float(bid_val), float(hm_mult), iso(now), iso(exp) if exp else None))

    conn.commit()
    conn.close()


# ---------- Core state queries ----------
def enforce_expirations():
    """
    Auto-sign any player whose ACTIVE bid has expired.
    Called opportunistically on page/API hits.
    """
    if is_fa_locked():
        return
    now = utcnow()
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
        SELECT
          b.id AS bid_id, b.player_id, b.team, b.expires_at,
          b.years, b.has_option, b.aav_m,
          p.name AS player_name, p.roster_player_id, p.mlbam_id, p.fangraphs_id
        FROM bids b
        JOIN free_agents p ON p.id=b.player_id
        WHERE b.status='ACTIVE'
          AND b.expires_at IS NOT NULL
          AND (p.signed_team IS NULL OR p.signed_team='')
          AND COALESCE(p.is_blacklisted,0)=0
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
                "player_name": str(r["player_name"] or "").strip(),
                "roster_player_id": int(r["roster_player_id"] or 0) if r["roster_player_id"] else None,
                "mlbam_id": int(r["mlbam_id"] or 0) if r["mlbam_id"] else None,
                "fangraphs_id": int(r["fangraphs_id"] or 0) if r["fangraphs_id"] else None,
            })

    for item in to_sign:
        bid_id = item["bid_id"]
        pid = item["player_id"]
        team = item["team"]
        roster_result = update_roster_db_for_fa_signing(
            item["roster_player_id"],
            team,
            item["years"],
            item["has_option"],
            item["aav_m"],
            player_name=item.get("player_name", ""),
            mlbam_id=item.get("mlbam_id"),
            fangraphs_id=item.get("fangraphs_id"),
        )
        resolved_roster_id = roster_result.get("roster_player_id") or item["roster_player_id"]
        if not roster_result.get("updated"):
            logging.warning(
                "FA signing recorded but roster update did not complete for %s: %s",
                item.get("player_name") or pid,
                roster_result.get("reason", "unknown reason"),
            )
        cur.execute(
            """
            UPDATE free_agents
            SET signed_team=?,
                signed_at=?,
                roster_player_id=COALESCE(?, roster_player_id),
                is_roster_unrostered=0,
                removed_from_roster_sync=?
            WHERE id=?
            """,
            (team, iso(now), resolved_roster_id, iso(now), pid),
        )
        cur.execute("UPDATE bids SET status='SIGNED' WHERE id=?", (bid_id,))
        # clear any other actives (should be none)
        cur.execute("UPDATE bids SET status='OUTBID', expires_at=NULL WHERE player_id=? AND status='ACTIVE' AND id<>?", (pid, bid_id))

    conn.commit()
    conn.close()

    for item in to_sign:
        try:
            notify_free_agent_signing(
                item.get("player_name") or f"player #{item['player_id']}",
                item["team"],
                item["years"],
                item["has_option"],
                item["aav_m"],
            )
        except Exception:
            _log_discord_notification_exception("Failed to post FA signing Discord notification")

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


def _roster_birthdates_for_ids(roster_ids: list[int]) -> dict[int, str]:
    """Return date_of_birth from roster.db for FA rows keyed by roster_player_id."""
    ids = sorted({int(x) for x in roster_ids if x})
    if not ids:
        return {}
    try:
        conn = get_roster_conn()
        cur = conn.cursor()
        placeholders = ",".join("?" for _ in ids)
        cur.execute(
            f"""
            SELECT id, date_of_birth
            FROM roster_players
            WHERE id IN ({placeholders})
            """,
            ids,
        )
        out = {
            int(r["id"]): str(r["date_of_birth"] or "").strip()
            for r in cur.fetchall()
        }
        conn.close()
        return out
    except Exception:
        logging.exception("Unable to load roster DOBs for FA display")
        return {}


def player_snapshot_row(p: sqlite3.Row, roster_dob_map: dict[int, str] | None = None) -> Dict[str, Any]:
    pid = int(p["id"])
    leader = get_current_leader(pid)

    mlbam_id = int(p["mlbam_id"] or 0) if "mlbam_id" in p.keys() else 0
    roster_player_id = int(p["roster_player_id"] or 0) if "roster_player_id" in p.keys() and p["roster_player_id"] else 0
    date_of_birth = ""
    if roster_dob_map and roster_player_id:
        date_of_birth = roster_dob_map.get(roster_player_id, "")

    return {
        "id": pid,
        "roster_player_id": roster_player_id,
        "mlbam_id": mlbam_id,
        "fg_url": p["fg_url"] if "fg_url" in p.keys() else None,

        "name": p["name"],
        "date_of_birth": date_of_birth,
        "dob": date_of_birth,
        "position": p["position"],
        "ovr": p["ovr"] if "ovr" in p.keys() else None,
        "pot": p["pot"] if "pot" in p.keys() else None,
        "def": p["def"] if "def" in p.keys() else None,
        "last_team": p["last_team"],
        "hometown_team": p["hometown_team"],
        "hometown_seasons": int(p["hometown_seasons"] or 0),
        "signed_team": p["signed_team"],
        "signed_at": p["signed_at"],
        "is_blacklisted": bool(int(p["is_blacklisted"] or 0)) if "is_blacklisted" in p.keys() else False,
        "blacklisted_at": p["blacklisted_at"] if "blacklisted_at" in p.keys() else None,
        "blacklist_note": p["blacklist_note"] if "blacklist_note" in p.keys() else None,
        "current_bid_value_m": float(leader["bid_value_m"]) if leader else None,
        "current_bid_team": leader["team"] if leader else None,
        "expires_at": leader["expires_at"] if leader else None,
    }

POSITION_FILTER_GROUPS = {
    "P": ["P", "SP", "RP"],
    "OF": ["OF", "LF", "CF", "RF"],
}


def _position_filter_clause(position: str) -> tuple[str, list[Any]]:
    """
    Build a token-aware SQLite position filter.

    Supports exact position tokens and useful groups:
      - P includes P/SP/RP
      - OF includes OF/LF/CF/RF
    It also handles simple multi-position strings like "2B/SS" or "LF, RF".
    """
    pos = str(position or "").strip().upper()
    if not pos:
        return "", []

    allowed = {"P", "SP", "RP", "C", "1B", "2B", "3B", "SS", "LF", "CF", "RF", "OF", "DH"}
    if pos not in allowed:
        return "", []

    tokens = POSITION_FILTER_GROUPS.get(pos, [pos])
    norm_expr = "(' ' || UPPER(REPLACE(REPLACE(REPLACE(COALESCE(position, ''), '/', ' '), ',', ' '), ';', ' ')) || ' ')"
    clause = "(" + " OR ".join(f"{norm_expr} LIKE ?" for _ in tokens) + ")"
    params = [f"% {tok} %" for tok in tokens]
    return clause, params


def fetch_free_agents(search: str = "", hide_signed: bool = False, show_unrated: bool = False, position: str = "") -> List[Dict[str, Any]]:
    enforce_expirations()
    conn = get_conn()
    cur = conn.cursor()

    clauses = []
    params: List[Any] = []

    clauses.append("COALESCE(is_roster_unrostered,0)=1")
    clauses.append("COALESCE(is_blacklisted,0)=0")

    if search.strip():
        s = search.strip().lower()
        s2 = "".join(ch for ch in unicodedata.normalize("NFKD", s) if not unicodedata.combining(ch))
        clauses.append("(LOWER(unaccent(name)) LIKE ?)")
        params.append(f"%{s2}%")

    if hide_signed:
        clauses.append("(signed_team IS NULL OR signed_team='')")

    pos_clause, pos_params = _position_filter_clause(position)
    if pos_clause:
        clauses.append(pos_clause)
        params.extend(pos_params)

    if not show_unrated:
        clauses.append("""
            (
              (ovr IS NOT NULL
               AND TRIM(CAST(ovr AS TEXT)) NOT IN ('', '-', '—')
               AND CAST(ovr AS INTEGER) <> 20)
              OR EXISTS (
                SELECT 1
                FROM bids b_pending
                WHERE b_pending.player_id = free_agents.id
                  AND b_pending.status = 'ACTIVE'
              )
            )
        """)

    q = "SELECT * FROM free_agents"
    if clauses:
        q += " WHERE " + " AND ".join(clauses)
    q += """
        ORDER BY
          CASE
            WHEN ovr IS NULL OR TRIM(CAST(ovr AS TEXT)) IN ('', '-', '—') THEN 1
            ELSE 0
          END ASC,
          CAST(ovr AS INTEGER) DESC,
          unaccent(name) COLLATE NOCASE ASC
    """

    cur.execute(q, params)
    rows = cur.fetchall()
    conn.close()
    roster_dob_map = _roster_birthdates_for_ids([
        int(r["roster_player_id"] or 0)
        for r in rows
        if "roster_player_id" in r.keys() and r["roster_player_id"]
    ])
    return [player_snapshot_row(r, roster_dob_map) for r in rows]

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
          AND COALESCE(p.is_blacklisted,0)=0
        ORDER BY datetime(w.created_at) DESC
    """, (team,))
    rows = cur.fetchall()
    conn.close()
    roster_dob_map = _roster_birthdates_for_ids([
        int(r["roster_player_id"] or 0)
        for r in rows
        if "roster_player_id" in r.keys() and r["roster_player_id"]
    ])
    return [player_snapshot_row(r, roster_dob_map) for r in rows]

def compute_preview(team: str, pid: int, years: int, has_option: bool, aav_m: float, *, ignore_lock: bool = False) -> Dict[str, Any]:
    enforce_expirations()
    team = normalize_bid_team(team)

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

    signed = bool((p["signed_team"] or "").strip())
    blacklisted = bool(int(p["is_blacklisted"] or 0)) if "is_blacklisted" in p.keys() else False
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
        required = cur_val * BID_VALUE_MIN_INCREMENT

    # validation booleans
    meets_min_aav = (aav_m >= min_aav - 1e-9)
    meets_outbid = True
    if required is not None:
        meets_outbid = (my_val >= required - 1e-9)

    fa_locked = is_fa_locked()
    cap_summary = get_team_cap_summary(team, excluding_player_id=pid)
    bid_cost = aav_m * 1000000.0
    meets_cap = True
    if cap_summary is not None:
        meets_cap = bid_cost <= float(cap_summary["available_cap"] or 0.0) + 0.01

    return {
        "player_id": pid,
        "team": team,
        "signed": signed,
        "blacklisted": blacklisted,

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

        "required_min_bid_value_m": required,  # 5% increment threshold over current high bid

        "fa_locked": fa_locked,
        "cap_summary": cap_summary,
        "bid_cost_m": bid_cost / 1000000.0,
        "meets_cap": meets_cap,

        "meets_min_aav": meets_min_aav,
        "meets_outbid": meets_outbid,
        "ok_to_submit": (ignore_lock or not fa_locked) and (not signed) and (not blacklisted) and meets_min_aav and meets_outbid and meets_cap,
    }

def place_bid(team: str, pid: int, years: int, has_option: bool, aav_m: float, *, allow_when_locked: bool = False) -> Tuple[bool, str]:
    enforce_expirations()
    team = normalize_bid_team(team)
    if is_fa_locked() and not allow_when_locked:
        return (False, "Free agency is currently locked. No FA bids can be made until an admin unlocks FA.")

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
    if (p["signed_team"] or "").strip():
        conn.close()
        return (False, "Player already signed.")
    if "is_blacklisted" in p.keys() and int(p["is_blacklisted"] or 0) == 1:
        conn.close()
        return (False, "Player is blacklisted from free agency.")
    if "is_roster_unrostered" in p.keys() and int(p["is_roster_unrostered"] or 0) != 1:
        conn.close()
        return (False, "Player is no longer an eligible free agent.")

    # preview for validations
    try:
        prev = compute_preview(team, pid, years, has_option, aav_m, ignore_lock=allow_when_locked)
    except Exception as e:
        conn.close()
        return (False, str(e))

    if not prev["meets_min_aav"]:
        conn.close()
        return (False, f"AAV is below minimum for that contract: minimum is {fmt_money_m(prev['min_aav_m'])}/yr.")
    if not prev["meets_outbid"]:
        need = prev["required_min_bid_value_m"]
        conn.close()
        return (False, f"Bid must be at least 5% higher than the current high bid: need ≥ {fmt_money_m(need)} (1-year equiv).")
    if not prev.get("meets_cap", True):
        cap = prev.get("cap_summary") or {}
        conn.close()
        return (False, f"Bid would exceed your hard-cap space. Available cap after current FA bids: {fmt_money_m(float(cap.get('available_cap_m', 0.0)))}; bid AAV: {fmt_money_m(float(prev.get('bid_cost_m', 0.0)))}.")

    # Calculate + insert
    now = utcnow()
    exp = None if is_fa_locked() else bid_expiration_at(now)
    player_name = str(p["name"] or "").strip() or f"player #{pid}"
    bid_years = int(prev["years"])
    bid_has_option = bool(prev["has_option"])
    bid_aav_m = float(prev["aav_m"])
    bid_value_m = float(prev["my_bid_value_m"])

    # Mark previous active as outbid, clear their expiry
    cur.execute("UPDATE bids SET status='OUTBID', expires_at=NULL WHERE player_id=? AND status='ACTIVE'", (pid,))

    cur.execute("""
        INSERT INTO bids(player_id, team, years, has_option, aav_m, bid_value_m, hometown_mult, created_at, expires_at, status)
        VALUES(?,?,?,?,?,?,?,?,?, 'ACTIVE')
    """, (
        pid, team,
        bid_years,
        1 if bid_has_option else 0,
        bid_aav_m,
        bid_value_m,
        float(prev["hometown_multiplier"]),
        iso(now),
        iso(exp) if exp else None,
    ))

    conn.commit()
    conn.close()

    try:
        notify_free_agent_bid(player_name, team, bid_years, bid_has_option, bid_aav_m, bid_value_m)
    except Exception:
        _log_discord_notification_exception("Failed to post FA bid Discord notification")

    return (True, "")


def list_blacklisted_free_agents(limit: int = 100) -> list[dict[str, Any]]:
    """Return active FA blacklist rows for the admin page."""
    ensure_fa_blacklist_schema()
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT
          p.id, p.name, p.position, p.last_team, p.roster_player_id,
          p.blacklisted_at, p.blacklist_note,
          COUNT(b.id) AS bid_rows
        FROM free_agents p
        LEFT JOIN bids b ON b.player_id=p.id
        WHERE COALESCE(p.is_blacklisted,0)=1
        GROUP BY p.id
        ORDER BY datetime(COALESCE(p.blacklisted_at, '1900-01-01')) DESC,
                 unaccent(p.name) COLLATE NOCASE ASC
        LIMIT ?
        """,
        (int(limit),),
    )
    rows = [dict(r) for r in cur.fetchall()]
    conn.close()
    return rows


def blacklist_free_agent(player_id: int, note: str = "") -> dict[str, Any]:
    """
    Admin helper: hide an unsigned FA from the FA pool and clear bid/watchlist state.

    Existing bid rows are preserved for audit but changed to VOIDED and no longer
    count as active commitments or signing timers.
    """
    ensure_fa_blacklist_schema()
    pid = int(player_id or 0)
    if pid <= 0:
        raise ValueError("Select a valid free agent")

    conn = get_conn()
    cur = conn.cursor()
    try:
        cur.execute("BEGIN IMMEDIATE")
        cur.execute("SELECT * FROM free_agents WHERE id=?", (pid,))
        player = cur.fetchone()
        if not player:
            raise ValueError("Player not found")
        if (player["signed_team"] or "").strip():
            raise ValueError("Player is already signed; remove them from the roster instead of blacklisting their FA row")

        now_text = iso(utcnow())
        cur.execute(
            """
            UPDATE bids
            SET status='VOIDED', expires_at=NULL
            WHERE player_id=? AND status IN ('ACTIVE','OUTBID')
            """,
            (pid,),
        )
        bids_voided = int(cur.rowcount or 0)
        cur.execute("DELETE FROM watchlist WHERE player_id=?", (pid,))
        watchlist_removed = int(cur.rowcount or 0)
        cur.execute(
            """
            UPDATE free_agents
            SET is_blacklisted=1,
                blacklisted_at=?,
                blacklist_note=?,
                signed_team=NULL,
                signed_at=NULL
            WHERE id=?
            """,
            (now_text, str(note or "").strip(), pid),
        )
        conn.commit()
        return {
            "player_id": pid,
            "player_name": player["name"],
            "blacklisted_at": now_text,
            "blacklist_note": str(note or "").strip(),
            "bids_voided": bids_voided,
            "watchlist_removed": watchlist_removed,
        }
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def unblacklist_free_agent(player_id: int) -> dict[str, Any]:
    """Admin helper: restore a previously blacklisted player to normal FA eligibility."""
    ensure_fa_blacklist_schema()
    pid = int(player_id or 0)
    if pid <= 0:
        raise ValueError("Select a valid free agent")

    conn = get_conn()
    cur = conn.cursor()
    try:
        cur.execute("BEGIN IMMEDIATE")
        cur.execute("SELECT * FROM free_agents WHERE id=?", (pid,))
        player = cur.fetchone()
        if not player:
            raise ValueError("Player not found")
        cur.execute(
            """
            UPDATE free_agents
            SET is_blacklisted=0,
                blacklisted_at=NULL,
                blacklist_note=NULL
            WHERE id=?
            """,
            (pid,),
        )
        conn.commit()
        return {"player_id": pid, "player_name": player["name"]}
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def set_qualifying_offer(team: str, player_id: int, qo_aav_m: float = QO_AAV_M) -> dict[str, Any]:
    """Admin helper: create a standard 1-year QO bid for the selected team/player."""
    bid_team = normalize_bid_team(team)
    pid = int(player_id or 0)
    if pid <= 0:
        raise ValueError("Select a valid free agent")
    if not bid_team:
        raise ValueError("Select a valid team")

    ok, msg = place_bid(bid_team, pid, 1, False, float(qo_aav_m), allow_when_locked=True)
    if not ok:
        raise ValueError(msg)

    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT p.name, b.expires_at, b.bid_value_m, b.aav_m
        FROM bids b
        JOIN free_agents p ON p.id=b.player_id
        WHERE b.player_id=? AND b.team=? AND b.status='ACTIVE'
        ORDER BY b.id DESC
        LIMIT 1
        """,
        (pid, bid_team),
    )
    row = cur.fetchone()
    conn.close()
    return {
        "player_id": pid,
        "player_name": row["name"] if row else f"player #{pid}",
        "team": bid_team,
        "aav_m": float(row["aav_m"] if row else qo_aav_m),
        "bid_value_m": float(row["bid_value_m"] if row else qo_aav_m),
        "expires_at": row["expires_at"] if row else None,
        "fa_locked": is_fa_locked(),
    }


def reset_active_bid_for_player(player_id: int, *, reset_hours: int = 48) -> dict[str, Any]:
    """
    Admin helper for accidental FA bids.

    Voids the current ACTIVE bid for an unsigned free agent. If there is a
    previous OUTBID bid, that bid is restored as ACTIVE and gets a fresh timer.
    If there is no previous bid, the player is left with no active bidding.

    The voided bid is preserved with status='VOIDED' for audit/history instead
    of being deleted. It will not count toward active bid commitments.
    """
    pid = int(player_id or 0)
    if pid <= 0:
        raise ValueError("Select a valid free agent")

    now = utcnow()
    reset_hours = clamp_int(reset_hours, 1, 24 * 14, 48)
    new_exp = None if is_fa_locked() else bid_expiration_iso(now, base_hours=reset_hours)

    conn = get_conn()
    cur = conn.cursor()
    try:
        cur.execute("BEGIN IMMEDIATE")

        cur.execute("SELECT * FROM free_agents WHERE id=?", (pid,))
        player = cur.fetchone()
        if not player:
            raise ValueError("Player not found")
        if (player["signed_team"] or "").strip():
            raise ValueError("Player is already signed; this tool only resets active unsigned-player bidding")

        cur.execute(
            """
            SELECT *
            FROM bids
            WHERE player_id=? AND status='ACTIVE'
            ORDER BY datetime(created_at) DESC, id DESC
            LIMIT 1
            """,
            (pid,),
        )
        current = cur.fetchone()
        if not current:
            raise ValueError("This player does not currently have an active bid to reset")

        current_id = int(current["id"])
        cur.execute(
            """
            SELECT *
            FROM bids
            WHERE player_id=? AND status='OUTBID' AND id<>?
            ORDER BY datetime(created_at) DESC, id DESC
            LIMIT 1
            """,
            (pid, current_id),
        )
        previous = cur.fetchone()

        cur.execute(
            "UPDATE bids SET status='VOIDED', expires_at=NULL WHERE id=?",
            (current_id,),
        )

        restored_payload: dict[str, Any] | None = None
        action = "cleared"
        if previous:
            previous_id = int(previous["id"])
            cur.execute(
                """
                UPDATE bids
                SET status='OUTBID', expires_at=NULL
                WHERE player_id=? AND status='ACTIVE' AND id<>?
                """,
                (pid, previous_id),
            )
            cur.execute(
                "UPDATE bids SET status='ACTIVE', expires_at=? WHERE id=?",
                (new_exp, previous_id),
            )
            action = "restored_previous"
            restored_payload = {
                "id": previous_id,
                "team": previous["team"],
                "years": int(previous["years"] or 1),
                "has_option": bool(int(previous["has_option"] or 0)),
                "aav_m": float(previous["aav_m"] or 0.0),
                "bid_value_m": float(previous["bid_value_m"] or 0.0),
                "created_at": previous["created_at"],
                "expires_at": new_exp,
            }

        cur.execute(
            "UPDATE free_agents SET signed_team=NULL, signed_at=NULL WHERE id=?",
            (pid,),
        )

        conn.commit()

        voided_payload = {
            "id": current_id,
            "team": current["team"],
            "years": int(current["years"] or 1),
            "has_option": bool(int(current["has_option"] or 0)),
            "aav_m": float(current["aav_m"] or 0.0),
            "bid_value_m": float(current["bid_value_m"] or 0.0),
            "created_at": current["created_at"],
            "expires_at": current["expires_at"],
        }
        return {
            "player_id": pid,
            "player_name": player["name"],
            "action": action,
            "voided_bid": voided_payload,
            "restored_bid": restored_payload,
            "timer_reset_hours": reset_hours if restored_payload and new_exp else None,
            "fa_locked": is_fa_locked(),
        }
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def bootstrap_fa():
    """
    Call this from app.py AFTER roster_app.bootstrap_roster() has run.

    The FA table is now sourced from live roster.db plus the OOTP FA export.
    Do not seed demo/sample players here; an empty FA DB should be populated
    only by syncing current unrostered players from roster.db.
    """
    init_db()
    if os.environ.get("BNSL_BOOTSTRAP_SYNC_FA", "0").strip().lower() in {"1", "true", "yes", "on"}:
        sync_free_agents_from_roster_if_needed(force=True)
    try:
        reconcile_signed_free_agents_to_roster()
    except Exception:
        logging.exception("Unable to reconcile signed FAs into roster.db during FA bootstrap")
    seed_qualifying_offers(qo_aav_m=QO_AAV_M)

# ---------- UI (templates) ----------

SORTABLE_TABLES_ASSETS = r"""
<style>
  table[data-sortable="true"] th.bnsl-sortable-header {
    cursor: pointer;
    user-select: none;
    white-space: nowrap;
  }
  table[data-sortable="true"] th.bnsl-sortable-header:hover {
    filter: brightness(1.18);
  }
  table[data-sortable="true"] th.bnsl-sortable-header:focus {
    outline: 2px solid rgba(140,170,255,.55);
    outline-offset: -2px;
  }
  .bnsl-sort-arrow {
    opacity: 0.78;
    margin-left: 6px;
    font-size: 11px;
  }
</style>
<script>
(function () {
  const DISABLED_HEADER_NAMES = new Set(["action", "actions", "reorder", "controls"]);

  function cellText(row, colIndex) {
    const cell = row.cells[colIndex];
    if (!cell) return "";
    return (cell.getAttribute("data-sort") || cell.textContent || "").trim();
  }

  function valueForSort(text) {
    const raw = String(text || "").trim();
    if (!raw || raw === "—" || raw === "-") return { empty: true, type: "text", value: "" };

    const maybeDate = raw.match(/^\d{4}-\d{2}-\d{2}/) ? Date.parse(raw) : NaN;
    if (Number.isFinite(maybeDate)) return { empty: false, type: "number", value: maybeDate };

    let compact = raw
      .replace(/[,$£€]/g, "")
      .replace(/\s+/g, "")
      .trim();

    let multiplier = 1;
    if (/m$/i.test(compact)) {
      multiplier = 1000000;
      compact = compact.slice(0, -1);
    } else if (/k$/i.test(compact)) {
      multiplier = 1000;
      compact = compact.slice(0, -1);
    } else if (/%$/.test(compact)) {
      compact = compact.slice(0, -1);
    }

    let neg = false;
    const paren = compact.match(/^\((.*)\)$/);
    if (paren) {
      neg = true;
      compact = paren[1];
    }

    if (/^[+-]?\d+(\.\d+)?$/.test(compact)) {
      let num = Number(compact) * multiplier;
      if (neg) num = -num;
      if (Number.isFinite(num)) return { empty: false, type: "number", value: num };
    }

    return { empty: false, type: "text", value: raw.toLocaleLowerCase() };
  }

  function compareSortValues(a, b, dir) {
    if (a.value.empty && !b.value.empty) return 1;
    if (!a.value.empty && b.value.empty) return -1;
    if (a.value.empty && b.value.empty) return a.index - b.index;

    let cmp = 0;
    if (a.value.type === "number" && b.value.type === "number") {
      cmp = a.value.value - b.value.value;
    } else {
      cmp = String(a.value.value).localeCompare(String(b.value.value), undefined, {
        numeric: true,
        sensitivity: "base"
      });
    }

    if (cmp === 0) return a.index - b.index;
    return dir === "desc" ? -cmp : cmp;
  }

  function clearIndicators(table) {
    table.querySelectorAll("th.bnsl-sortable-header").forEach(th => {
      th.setAttribute("aria-sort", "none");
      const arrow = th.querySelector(".bnsl-sort-arrow");
      if (arrow) arrow.remove();
    });
  }

  function markIndicator(table, th, dir) {
    clearIndicators(table);
    th.setAttribute("aria-sort", dir === "asc" ? "ascending" : "descending");
    const arrow = document.createElement("span");
    arrow.className = "bnsl-sort-arrow";
    arrow.textContent = dir === "asc" ? "▲" : "▼";
    th.appendChild(arrow);
  }

  function sortTable(table, colIndex, th, toggle) {
    const tbody = table.tBodies && table.tBodies[0];
    if (!tbody) return;

    let dir = "asc";
    if (toggle && table.dataset.sortCol === String(colIndex)) {
      dir = table.dataset.sortDir === "asc" ? "desc" : "asc";
    } else if (!toggle && table.dataset.sortDir) {
      dir = table.dataset.sortDir;
    }

    table.dataset.bnslSorting = "1";
    table.dataset.bnslIgnoreMutation = "1";
    const rows = Array.from(tbody.rows).map((row, index) => ({
      row,
      index,
      value: valueForSort(cellText(row, colIndex))
    }));
    rows.sort((a, b) => compareSortValues(a, b, dir));
    rows.forEach(item => tbody.appendChild(item.row));
    table.dataset.sortCol = String(colIndex);
    table.dataset.sortDir = dir;
    table.dataset.bnslSorting = "0";

    if (th) markIndicator(table, th, dir);
  }

  function initSortableTable(table) {
    if (!table || table.dataset.bnslSortInit === "1") return;
    const headerRow = table.tHead && table.tHead.rows.length ? table.tHead.rows[0] : null;
    if (!headerRow) return;

    Array.from(headerRow.cells).forEach((th, colIndex) => {
      const label = (th.textContent || "").trim().toLocaleLowerCase();
      if (th.dataset.noSort === "true" || th.dataset.sortDisabled === "true" || DISABLED_HEADER_NAMES.has(label)) return;

      th.classList.add("bnsl-sortable-header");
      th.setAttribute("role", "button");
      th.setAttribute("tabindex", "0");
      th.setAttribute("aria-sort", "none");
      th.addEventListener("click", () => sortTable(table, colIndex, th, true));
      th.addEventListener("keydown", (ev) => {
        if (ev.key === "Enter" || ev.key === " ") {
          ev.preventDefault();
          sortTable(table, colIndex, th, true);
        }
      });
    });

    const tbody = table.tBodies && table.tBodies[0];
    if (tbody && "MutationObserver" in window) {
      let queued = false;
      const observer = new MutationObserver(() => {
        if (table.dataset.bnslIgnoreMutation === "1") {
          table.dataset.bnslIgnoreMutation = "0";
          return;
        }
        if (table.dataset.bnslSorting === "1") return;
        if (!table.dataset.sortCol) return;
        if (queued) return;
        queued = true;
        window.requestAnimationFrame(() => {
          queued = false;
          const colIndex = Number(table.dataset.sortCol);
          const th = headerRow.cells[colIndex];
          if (th) sortTable(table, colIndex, th, false);
        });
      });
      observer.observe(tbody, { childList: true });
    }

    table.dataset.bnslSortInit = "1";
  }

  function initAllSortableTables() {
    document.querySelectorAll('table[data-sortable="true"]').forEach(initSortableTable);
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", initAllSortableTables);
  } else {
    initAllSortableTables();
  }
  window.BNSLSortableTables = { initAll: initAllSortableTables };
})();
</script>
"""

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
  {SORTABLE_TABLES_ASSETS}
</head>
<body>
  <div class="page">

    <div class="brand">
      <div>
        <h1>FREE AGENCY</h1>
        <div class="sub">Contract bids • 48h clock (Fri/Sat +24h) • Hometown bonus baked into value</div>
      </div>
    </div>

    <div class="panel pad">
      <div class="topbar">

    <label class="pill">Your Team:
      <select id="team-select" style="margin-left:8px;"></select>
    </label>
    <button id="login-btn" class="btn">Login</button>

    <a class="btn" href="/">← Home</a>
    <a class="btn" id="watchlist-link" href="watchlist" style="display:none;">Watchlist</a>
    <a class="btn" href="history">Bid History</a>
    <a class="btn" href="export.csv">Export CSV</a>

    <label class="pill">
      <input type="checkbox" id="hide-signed" /> Hide signed
    </label>

    <label class="pill">
      <input type="checkbox" id="show-unrated" /> Show unrated
    </label>

    <label class="pill">Position:
      <select id="position-filter" style="margin-left:8px; border:1px solid #ddd; padding:6px 8px; border-radius:6px;">
        <option value="">All</option>
        <option value="P">P</option>
        <option value="C">C</option>
        <option value="1B">1B</option>
        <option value="2B">2B</option>
        <option value="3B">3B</option>
        <option value="SS">SS</option>
        <option value="OF">OF</option>
        <option value="LF">LF</option>
        <option value="CF">CF</option>
        <option value="RF">RF</option>
        <option value="DH">DH</option>
      </select>
    </label>

    <div class="pill" style="margin-left:auto;">
      <span>Search:</span>
      <input id="search" type="text" placeholder="Type a player name…" style="border:1px solid #ddd; padding:6px 8px; border-radius:6px; min-width: 260px;" />
      <span class="muted">(substring match)</span>
    </div>
  </div>

  <div class="topbar" style="margin-top:10px;">
    <div class="pill" id="login-pill">
      <span id="login-status">🔒 Not logged in</span>
    </div>
    <div class="pill" id="fa-lock-pill">FA status: Loading…</div>
    <div class="pill" id="cap-space-pill" style="display:none;">
      <span id="cap-space-status">Financials: —</span>
    </div>
  </div>

  <hr class="sep" />
  <div class="table-wrap">
    <table data-sortable="true">
    <thead>
      <tr>
        <th style="width:22%;">Player</th>
        <th style="width:9%;">DOB</th>
        <th style="width:7%;">Pos</th>
        <th style="width:6%;">OVR</th>
        <th style="width:6%;">POT</th>
        <th style="width:6%;">DEF</th>
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

      <div class="kv"><span>Current high bidder</span><b id="cur-leader">—</b></div>
      <div class="kv"><span>Current bid value (1-yr equiv)</span><b id="cur-val">—</b></div>
      <div class="kv"><span>Minimum bid value</span><b id="min-required">—</b></div>
      <div class="kv"><span>Minimum AAV</span><b id="min-aav">—</b></div>
      <div class="kv"><span>Your bid value (1-yr equiv)</span><b id="my-val">—</b></div>
      <div class="kv"><span>Available cap after active FA bids</span><b id="cap-avail">—</b></div>
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
const showUnrated = document.getElementById('show-unrated');
const positionFilter = document.getElementById('position-filter');
const faLockPill = document.getElementById('fa-lock-pill');
const capSpacePill = document.getElementById('cap-space-pill');
const capSpaceStatus = document.getElementById('cap-space-status');

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
const capAvail = document.getElementById('cap-avail');
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
  faLocked: false,
  capSummary: null,
  search: "",
  hideSigned: false,
  showUnrated: false,
  position: "",
  players: [],
  modalPlayer: null
}};

function moneyM(x) {{
  if (x === null || x === undefined) return "—";
  const n = Number(x);
  if (!Number.isFinite(n)) return "—";
  const sign = n < 0 ? "-" : "";
  return `${{sign}}$${{Math.abs(n).toFixed(2)}}M`;
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
function rating(x) {{ return (x === null || x === undefined || x === '') ? '—' : String(x); }}
function bidConfirmationText(p, payload) {{
  const years = Number(payload.years || 1);
  const optionText = payload.has_option ? ' + club option' : '';
  const aav = Number(payload.aav_m || 0);
  const guaranteed = years * aav;
  const maxTotal = payload.has_option ? (years + 1) * aav : guaranteed;
  const lines = [
    `Place this FA bid on ${{p?.name || 'this player'}}?`,
    '',
    `Team: ${{state.team || 'your team'}}`,
    `Contract: ${{years}} year${{years === 1 ? '' : 's'}}${{optionText}}`,
    `AAV: ${{moneyM(aav)}}`,
    payload.has_option
      ? `Guaranteed/max value: ${{moneyM(guaranteed)}} guaranteed; ${{moneyM(maxTotal)}} including option`
      : `Total value: ${{moneyM(guaranteed)}}`,
  ];
  if (p?.current_bid_team) lines.push(`Current high bidder: ${{p.current_bid_team}}`);
  lines.push('', 'Submit this bid?');
  return lines.join('\\n');
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
  state.faLocked = !!data.fa_locked;
  state.capSummary = data.cap_summary || null;

  if (faLockPill) {{
    faLockPill.textContent = state.faLocked
      ? '🔒 FA locked — bids are frozen'
      : '🔓 FA unlocked — active bids sign after 48h (Fri/Sat +24h)';
  }}
  if (capSpacePill && capSpaceStatus) {{
    if (state.authed && state.capSummary) {{
      capSpacePill.style.display = 'inline-flex';
      capSpaceStatus.textContent = `Hard-cap room: ${{moneyM(state.capSummary.available_cap_m)}} | Remaining surplus: ${{moneyM(state.capSummary.remaining_surplus_m)}} (active FA bids: ${{moneyM(state.capSummary.active_bid_commitments_m)}})`;
    }} else {{
      capSpacePill.style.display = 'none';
    }}
  }}

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

let playersRequestSeq = 0;

async function fetchPlayers() {{
  const requestSeq = ++playersRequestSeq;
  const params = new URLSearchParams({{
    search: state.search,
    hide_signed: state.hideSigned ? '1' : '0',
    show_unrated: state.showUnrated ? '1' : '0',
    position: state.position || '',
  }});
  const res = await fetch('/fa/api/free_agents?' + params.toString());
  const data = await res.json();

  // Ignore stale responses from earlier searches/filters that finished late.
  if (requestSeq !== playersRequestSeq) return;

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

    const val = p.current_bid_value_m !== null ? moneyM(p.current_bid_value_m) : '—';
    const expSort = p.signed_at || p.expires_at || '';
    const exp = expSort ? fmtIso(expSort) : '—';

    const status = p.signed_team ? `Signed: ${{p.signed_team}}` : (state.faLocked ? (p.current_bid_team ? 'Bid frozen' : 'FA locked') : (p.current_bid_team ? 'Bidding open' : 'No bids'));

    const actionTd = document.createElement('td');

    const bidBtn = document.createElement('button');
    bidBtn.className = 'btn';
    bidBtn.textContent = 'Bid';
    bidBtn.disabled = !state.authed || state.faLocked || !!p.signed_team;
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
  <td>${{p.date_of_birth || '—'}}</td>
  <td>${{p.position || '—'}}</td>
  <td>${{rating(p.ovr)}}</td>
  <td>${{rating(p.pot)}}</td>
  <td>${{rating(p.def)}}</td>
  <td>${{val}}</td>
  <td data-sort="${{expSort}}">${{exp}}</td>
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

  if (curLeader) curLeader.textContent = data.current_bid_team ? data.current_bid_team : '—';
  curVal.textContent = data.current_bid_value_m !== null ? moneyM(data.current_bid_value_m) : '—';
  minRequired.textContent = data.required_min_bid_value_m !== null ? moneyM(data.required_min_bid_value_m) : '—';
  if (capAvail) capAvail.textContent = data.cap_summary ? moneyM(data.cap_summary.available_cap_m) : '—';
  minAav.textContent = moneyM(data.min_aav_m) + '/yr';
  myVal.textContent = moneyM(data.my_bid_value_m);
  hmMult.textContent = data.is_hometown_bid ? `${{data.hometown_multiplier.toFixed(2)}}x (hometown)` : `${{data.hometown_multiplier.toFixed(2)}}x`;

  submitBidBtn.disabled = !data.ok_to_submit;

  if (data.fa_locked) {{
    modalWarn.textContent = "Free agency is locked. Admin QOs can be entered, but public FA bids are frozen.";
    modalWarn.style.display = 'block';
  }} else if (data.blacklisted) {{
    modalWarn.textContent = "This player has been removed from the free-agent pool by an admin.";
    modalWarn.style.display = 'block';
  }} else if (data.signed) {{
    modalWarn.textContent = "This player is already signed.";
    modalWarn.style.display = 'block';
  }} else if (!data.meets_min_aav) {{
    modalWarn.textContent = `AAV is below minimum for that structure (min ${{moneyM(data.min_aav_m)}}/yr).`;
    modalWarn.style.display = 'block';
  }} else if (!data.meets_outbid) {{
    modalWarn.textContent = `Bid value must be at least 5% higher than the current high bid (need ≥ ${{moneyM(data.required_min_bid_value_m)}}).`;
    modalWarn.style.display = 'block';
  }} else if (!data.meets_cap) {{
    modalWarn.textContent = `Bid would exceed your hard-cap space. Available after active FA bids: ${{moneyM(data.cap_summary?.available_cap_m || 0)}}.`;
    modalWarn.style.display = 'block';
  }} else {{
    modalOk.textContent = "Bid is valid to submit.";
    modalOk.style.display = 'block';
  }}
}}

function openBidModal(p) {{
  state.modalPlayer = p;
  modalTitle.textContent = `Bid: ${{p.name}}`;
  modalSubtitle.textContent = p.signed_team ? `Signed to ${{p.signed_team}}` : (state.faLocked ? 'FA is locked; public bids are frozen.' : 'Set years/option/AAV. Preview updates live.');

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
function queuePreviewBid() {{
  window.requestAnimationFrame(previewBid);
}}
aavInput.oninput = queuePreviewBid;
aavInput.onchange = queuePreviewBid;
aavInput.onkeyup = queuePreviewBid;

submitBidBtn.onclick = async () => {{
  const p = state.modalPlayer;
  if (!p) return;

  const payload = {{
    player_id: p.id,
    years: Number(yearsSel.value || 1),
    has_option: (optSel.value === "1"),
    aav_m: Number(aavInput.value || 0),
  }};

  if (!confirm(bidConfirmationText(p, payload))) return;

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

showUnrated.addEventListener('change', () => {{
  state.showUnrated = showUnrated.checked;
  fetchPlayers();
}});

positionFilter.addEventListener('change', () => {{
  state.position = positionFilter.value || '';
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
  {SORTABLE_TABLES_ASSETS}
</head>
<div class="page">
  <div class="brand">
    <div>
      <h1>BID HISTORY</h1>
      <div class="sub">Bid log and maintenance actions</div>
    </div>
    <div class="right"><span class="badge">HISTORY</span></div>
  </div>

  <div class="panel pad">
    <!-- existing topbar/table -->
<body>
  <div class="topbar">
    <a class="btn" href="./">← Back to FA</a>
    <a class="btn" href="/">← Home</a>
    <span class="pill">Watchlist</span>
    <a class="btn" href="history">Bid History</a>
  </div>

  <div class="topbar" style="margin-top:10px;">
    <div class="pill" id="login-pill">
      <span id="login-status">Loading…</span>
    </div>
    <div class="pill" id="fa-lock-pill">FA status: Loading…</div>
    <div class="pill" id="cap-space-pill" style="display:none;"><span id="cap-space-status">Financials: —</span></div>
  </div>

  <table data-sortable="true">
    <thead>
      <tr>
        <th style="width:22%;">Player</th>
        <th style="width:9%;">DOB</th>
        <th style="width:7%;">Pos</th>
        <th style="width:6%;">OVR</th>
        <th style="width:6%;">POT</th>
        <th style="width:6%;">DEF</th>
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
const faLockPill = document.getElementById('fa-lock-pill');
const capSpacePill = document.getElementById('cap-space-pill');
const capSpaceStatus = document.getElementById('cap-space-status');

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
const capAvail = document.getElementById('cap-avail');
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
  faLocked: false,
  capSummary: null,
  players: [],
  modalPlayer: null
}};

function moneyM(x) {{
  if (x === null || x === undefined) return "—";
  const n = Number(x);
  if (!Number.isFinite(n)) return "—";
  const sign = n < 0 ? "-" : "";
  return `${{sign}}$${{Math.abs(n).toFixed(2)}}M`;
}}
function fmtIso(isoStr) {{
  if (!isoStr) return "—";
  try {{ return new Date(isoStr).toLocaleString(); }} catch {{ return isoStr; }}
}}
function rating(x) {{ return (x === null || x === undefined || x === '') ? '—' : String(x); }}
function bidConfirmationText(p, payload) {{
  const years = Number(payload.years || 1);
  const optionText = payload.has_option ? ' + club option' : '';
  const aav = Number(payload.aav_m || 0);
  const guaranteed = years * aav;
  const maxTotal = payload.has_option ? (years + 1) * aav : guaranteed;
  const lines = [
    `Place this FA bid on ${{p?.name || 'this player'}}?`,
    '',
    `Team: ${{state.team || 'your team'}}`,
    `Contract: ${{years}} year${{years === 1 ? '' : 's'}}${{optionText}}`,
    `AAV: ${{moneyM(aav)}}`,
    payload.has_option
      ? `Guaranteed/max value: ${{moneyM(guaranteed)}} guaranteed; ${{moneyM(maxTotal)}} including option`
      : `Total value: ${{moneyM(guaranteed)}}`,
  ];
  if (p?.current_bid_team) lines.push(`Current high bidder: ${{p.current_bid_team}}`);
  lines.push('', 'Submit this bid?');
  return lines.join('\\n');
}}

async function fetchStatus() {{
  const res = await fetch('/fa/api/fa_status');
  const data = await res.json();
  state.team = data.selected_team || "";
  state.authed = !!data.authed_for_selected;
  state.authedEmail = data.authed_email || "";
  state.faLocked = !!data.fa_locked;
  state.capSummary = data.cap_summary || null;
  if (faLockPill) {{
    faLockPill.textContent = state.faLocked
      ? '🔒 FA locked — bids are frozen'
      : '🔓 FA unlocked — active bids sign after 48h (Fri/Sat +24h)';
  }}
  if (capSpacePill && capSpaceStatus) {{
    if (state.authed && state.capSummary) {{
      capSpacePill.style.display = 'inline-flex';
      capSpaceStatus.textContent = `Hard-cap room: ${{moneyM(state.capSummary.available_cap_m)}} | Remaining surplus: ${{moneyM(state.capSummary.remaining_surplus_m)}} (active FA bids: ${{moneyM(state.capSummary.active_bid_commitments_m)}})`;
    }} else {{
      capSpacePill.style.display = 'none';
    }}
  }}
  if (state.authed && state.team) {{
    loginStatus.textContent = `🔓 Logged in as ${{state.authedEmail}} for ${{state.team}}`;
  }} else {{
    loginStatus.textContent = '🔒 Not logged in (go back and login)';
  }}
}}

async function fetchWatchlist() {{
  const res = await fetch('/fa/api/watchlist');
  if (!res.ok) {{
    wlBody.innerHTML = '<tr><td colspan="10" class="danger">Not logged in.</td></tr>';
    return;
  }}
  const data = await res.json();
  state.players = data.players || [];
  render();
}}

function render() {{
  wlBody.innerHTML = '';
  if (!state.players.length) {{
    wlBody.innerHTML = '<tr><td colspan="10" class="muted">No players in your watchlist yet.</td></tr>';
    return;
  }}

  for (const p of state.players) {{
    const tr = document.createElement('tr');
    tr.className = 'row-hover' + (p.signed_team ? ' signed' : '');
    const val = p.current_bid_value_m !== null ? moneyM(p.current_bid_value_m) : '—';
    const expSort = p.signed_at || p.expires_at || '';
    const exp = expSort ? fmtIso(expSort) : '—';
    const status = p.signed_team ? `Signed: ${{p.signed_team}}` : (state.faLocked ? (p.current_bid_team ? 'Bid frozen' : 'FA locked') : (p.current_bid_team ? 'Bidding open' : 'No bids'));
    const img = p.mlbam_id ? `/fa/player_image/${{p.mlbam_id}}.png` : '';
    const imgTag = p.mlbam_id ? `<img class="pimg" src="${{img}}" loading="lazy" />` : '';

    const actionTd = document.createElement('td');

    const bidBtn = document.createElement('button');
    bidBtn.className = 'btn';
    bidBtn.textContent = 'Bid';
    bidBtn.disabled = !state.authed || state.faLocked || !!p.signed_team;
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
  <td>${{p.date_of_birth || '—'}}</td>
  <td>${{p.position || '—'}}</td>
  <td>${{rating(p.ovr)}}</td>
  <td>${{rating(p.pot)}}</td>
  <td>${{rating(p.def)}}</td>
  <td>${{val}}</td>
  <td data-sort="${{expSort}}">${{exp}}</td>
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

  if (curLeader) curLeader.textContent = data.current_bid_team ? data.current_bid_team : '—';
  curVal.textContent = data.current_bid_value_m !== null ? moneyM(data.current_bid_value_m) : '—';
  minRequired.textContent = data.required_min_bid_value_m !== null ? moneyM(data.required_min_bid_value_m) : '—';
  if (capAvail) capAvail.textContent = data.cap_summary ? moneyM(data.cap_summary.available_cap_m) : '—';
  minAav.textContent = moneyM(data.min_aav_m) + '/yr';
  myVal.textContent = moneyM(data.my_bid_value_m);
  hmMult.textContent = data.is_hometown_bid ? `${{data.hometown_multiplier.toFixed(2)}}x (hometown)` : `${{data.hometown_multiplier.toFixed(2)}}x`;

  submitBidBtn.disabled = !data.ok_to_submit;

  if (data.fa_locked) {{
    modalWarn.textContent = "Free agency is locked. Admin QOs can be entered, but public FA bids are frozen.";
    modalWarn.style.display = 'block';
  }} else if (data.blacklisted) {{
    modalWarn.textContent = "This player has been removed from the free-agent pool by an admin.";
    modalWarn.style.display = 'block';
  }} else if (data.signed) {{
    modalWarn.textContent = "This player is already signed.";
    modalWarn.style.display = 'block';
  }} else if (!data.meets_min_aav) {{
    modalWarn.textContent = `AAV is below minimum for that structure (min ${{moneyM(data.min_aav_m)}}/yr).`;
    modalWarn.style.display = 'block';
  }} else if (!data.meets_outbid) {{
    modalWarn.textContent = `Bid value must be at least 5% higher than the current high bid (need ≥ ${{moneyM(data.required_min_bid_value_m)}}).`;
    modalWarn.style.display = 'block';
  }} else if (!data.meets_cap) {{
    modalWarn.textContent = `Bid would exceed your hard-cap space. Available after active FA bids: ${{moneyM(data.cap_summary?.available_cap_m || 0)}}.`;
    modalWarn.style.display = 'block';
  }} else {{
    modalOk.textContent = "Bid is valid to submit.";
    modalOk.style.display = 'block';
  }}
}}

function openBidModal(p) {{
  state.modalPlayer = p;
  modalTitle.textContent = `Bid: ${{p.name}}`;
  modalSubtitle.textContent = p.signed_team ? `Signed to ${{p.signed_team}}` : (state.faLocked ? 'FA is locked; public bids are frozen.' : 'Set years/option/AAV. Preview updates live.');
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
function queuePreviewBid() {{
  window.requestAnimationFrame(previewBid);
}}
aavInput.oninput = queuePreviewBid;
aavInput.onchange = queuePreviewBid;
aavInput.onkeyup = queuePreviewBid;

submitBidBtn.onclick = async () => {{
  const p = state.modalPlayer;
  if (!p) return;
  const payload = {{
    player_id: p.id,
    years: Number(yearsSel.value || 1),
    has_option: (optSel.value === "1"),
    aav_m: Number(aavInput.value || 0),
  }};
  if (!confirm(bidConfirmationText(p, payload))) return;

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
  {SORTABLE_TABLES_ASSETS}
</head>
<div class="page">
  <div class="brand">
    <div>
      <h1>BID HISTORY</h1>
      <div class="sub">All FA bids, plus your current active bids and completed signings when logged in</div>
    </div>
    <div class="right"><span class="badge">HISTORY</span></div>
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
    <button class="btn" id="assign-htd">Assign HTD</button>
    <span class="muted" id="htd-status"></span>
  </div>

  <section id="team-summary-section" style="display:none; margin: 14px 0 22px 0;">
    <div class="topbar" style="justify-content:space-between; padding-left:0; padding-right:0;">
      <div>
        <h2 style="margin:0; font-size:20px;" id="team-summary-title">Your FA Activity</h2>
        <div class="sub" id="team-summary-sub">Active winning bids and players you have signed</div>
      </div>
      <span class="badge" id="team-summary-count">0</span>
    </div>
    <table data-sortable="true">
      <thead>
        <tr>
          <th style="width:24%;">Player</th>
          <th style="width:14%;">Status</th>
          <th style="width:12%;">Contract</th>
          <th style="width:12%;">AAV</th>
          <th style="width:14%;">1-yr equiv</th>
          <th style="width:16%;">Expires / Signed</th>
          <th style="width:16%;">Last Action</th>
        </tr>
      </thead>
      <tbody id="team-summary-body"></tbody>
    </table>
  </section>

  <table data-sortable="true">
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
const assignHtdBtn = document.getElementById('assign-htd');
const htdStatus = document.getElementById('htd-status');
const teamSummarySection = document.getElementById('team-summary-section');
const teamSummaryBody = document.getElementById('team-summary-body');
const teamSummaryTitle = document.getElementById('team-summary-title');
const teamSummarySub = document.getElementById('team-summary-sub');
const teamSummaryCount = document.getElementById('team-summary-count');

function moneyM(x) {{
  if (x === null || x === undefined) return "—";
  const n = Number(x);
  if (!Number.isFinite(n)) return "—";
  const sign = n < 0 ? "-" : "";
  return `${{sign}}$${{Math.abs(n).toFixed(2)}}M`;
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
function statusText(b) {{
  if (b.status === 'ACTIVE') return 'Active bid';
  if (b.status === 'SIGNED') return 'Signed';
  return b.status || '—';
}}
function activityDate(b) {{
  if (b.status === 'SIGNED') return b.signed_at || b.expires_at || b.created_at;
  return b.expires_at || b.created_at;
}}

function renderTeamSummary(data) {{
  const authedTeam = data.authed_team || '';
  const rows = data.team_summary || [];
  if (!authedTeam) {{
    teamSummarySection.style.display = 'none';
    return;
  }}

  teamSummarySection.style.display = 'block';
  teamSummaryTitle.textContent = `Your FA Activity — ${{authedTeam}}`;
  teamSummarySub.textContent = 'Active bids you currently lead, plus completed free-agent signings.';
  teamSummaryCount.textContent = `${{rows.length}} item${{rows.length === 1 ? '' : 's'}}`;

  teamSummaryBody.innerHTML = '';
  if (!rows.length) {{
    teamSummaryBody.innerHTML = '<tr><td colspan="7" class="muted">No active bids or signed players for your team yet.</td></tr>';
    return;
  }}

  for (const b of rows) {{
    const tr = document.createElement('tr');
    tr.className = 'row-hover';
    tr.innerHTML = `
      <td><b>${{b.player_name}}</b></td>
      <td>${{statusText(b)}}</td>
      <td>${{contractText(b)}}</td>
      <td>${{moneyM(b.aav_m)}}/yr</td>
      <td>${{moneyM(b.bid_value_m)}}</td>
      <td data-sort="${{activityDate(b) || ''}}">${{fmtIso(activityDate(b))}}</td>
      <td data-sort="${{b.created_at || ''}}">${{fmtIso(b.created_at)}}</td>
    `;
    teamSummaryBody.appendChild(tr);
  }}
}}

let bidHistoryRequestSeq = 0;

async function load() {{
  const requestSeq = ++bidHistoryRequestSeq;
  const params = new URLSearchParams({{
    search: search.value || ""
  }});
  const res = await fetch('/fa/api/bid_history?' + params.toString());
  const data = await res.json();

  // Ignore stale responses from earlier bid-history searches that finished late.
  if (requestSeq !== bidHistoryRequestSeq) return;

  renderTeamSummary(data);
  const rows = data.bids || [];

  body.innerHTML = '';
  if (!rows.length) {{
    body.innerHTML = '<tr><td colspan="8" class="muted">No bids found.</td></tr>';
    return;
  }}

  for (const b of rows) {{
    const tr = document.createElement('tr');
    tr.className = 'row-hover';
    const expOrSigned = b.signed_at || b.expires_at || '';
    tr.innerHTML = `
      <td data-sort="${{b.created_at || ''}}">${{fmtIso(b.created_at)}}</td>
      <td><b>${{b.player_name}}</b></td>
      <td>${{b.team}}</td>
      <td>${{contractText(b)}}</td>
      <td>${{moneyM(b.aav_m)}}/yr</td>
      <td>${{moneyM(b.bid_value_m)}}</td>
      <td data-sort="${{expOrSigned}}">${{fmtIso(expOrSigned)}}</td>
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

assignHtdBtn.onclick = async () => {{
  const ok = confirm('Assign hometown discounts from hometown_discounts.db to the current FA pool?');
  if (!ok) return;

  assignHtdBtn.disabled = true;
  htdStatus.textContent = 'Assigning HTD…';

  try {{
    const resp = await fetch('/fa/api/assign_htd', {{ method: 'POST' }});
    if (!resp.ok) {{
      htdStatus.textContent = '';
      alert('HTD assignment failed: ' + await resp.text());
      return;
    }}
    const data = await resp.json();
    htdStatus.textContent = `HTD: ${{data.matched}} matched / ${{data.checked}} checked; ${{data.cleared}} cleared`;
    await load();
  }} finally {{
    assignHtdBtn.disabled = false;
  }}
}};

load();
</script>
  </div> <!-- /panel -->
</div>   <!-- /page -->
</body>
</html>
"""


# ---------- Pages ----------

def _csv_response_for_table(conn: sqlite3.Connection, table: str, filename: str):
    cur = conn.cursor()
    cur.execute(f"PRAGMA table_info({table})")
    columns = [row[1] for row in cur.fetchall()]
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(columns)
    order_clause = " ORDER BY id" if "id" in columns else ""
    cur.execute(f"SELECT * FROM {table}{order_clause}")
    for row in cur.fetchall():
        writer.writerow([row[col] for col in columns])
    conn.close()
    return current_app.response_class(
        output.getvalue(),
        mimetype="text/csv",
        headers={"Content-Disposition": f"attachment; filename={filename}"},
    )


@fa_bp.get("/export.csv")
def export_free_agents_csv():
    init_db()
    # Export is read-only now. Run the admin/CLI FA maintenance sync first if
    # you intentionally want to refresh the whole FA table before exporting.
    conn = get_conn()
    ensure_column(conn, "free_agents", "mlbam_id", "mlbam_id INTEGER")
    return _csv_response_for_table(conn, "free_agents", "free_agents.csv")

@fa_bp.route("/")
def index():
    return render_template_string(INDEX_HTML)

@fa_bp.route("/watchlist")
def watchlist_page():
    if not session.get("authed_team"):
        return redirect(url_for("fa.index"))
    return render_template_string(WATCHLIST_HTML)

@fa_bp.route("/history")
def history_page():
    return render_template_string(HISTORY_HTML)


# ---------- APIs ----------
@fa_bp.get("/api/fa_status")
def api_fa_status():
    init_db()
    enforce_expirations()
    selected_team = session.get("selected_team", "") or ""
    authed_team = session.get("authed_team", "") or ""
    authed_email = session.get("authed_email", "") or ""
    cap_summary = get_team_cap_summary(authed_team) if authed_team else None
    return jsonify({
        "teams": MLB_TEAMS,
        "selected_team": selected_team,
        "authed_team": authed_team,
        "authed_email": authed_email,
        "authed_for_selected": bool(selected_team) and (selected_team == authed_team),
        "fa_locked": is_fa_locked(),
        "cap_summary": cap_summary,
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
    init_db()
    search = (request.args.get("search") or "").strip()
    hide_signed = (request.args.get("hide_signed") == "1")
    show_unrated = (request.args.get("show_unrated") == "1")
    position = (request.args.get("position") or "").strip()
    players = fetch_free_agents(search=search, hide_signed=hide_signed, show_unrated=show_unrated, position=position)

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
    init_db()
    team = _require_authed_team()
    players = fetch_watchlist(team)
    return jsonify({"players": players})

@fa_bp.post("/api/watchlist/add")
def api_watchlist_add():
    init_db()
    team = _require_authed_team()
    data = request.get_json(force=True, silent=True) or {}
    pid = int(data.get("player_id") or 0)
    if pid <= 0:
        return ("missing player_id", 400)

    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT 1
        FROM free_agents
        WHERE id=?
          AND COALESCE(is_roster_unrostered,0)=1
          AND COALESCE(is_blacklisted,0)=0
          AND (signed_team IS NULL OR signed_team='')
        """,
        (pid,),
    )
    if not cur.fetchone():
        conn.close()
        return ("Player is not an eligible free agent.", 409)
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
    init_db()
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
    init_db()
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

@fa_bp.post("/api/assign_htd")
def api_assign_htd():
    _require_authed_team()
    try:
        summary = assign_hometown_discounts_now(clear_missing=True)
        return jsonify({"ok": True, **summary})
    except Exception as exc:
        logging.exception("HTD assignment failed")
        return (f"HTD assignment failed: {exc}", 500)


def _bid_history_json_row(r: sqlite3.Row) -> Dict[str, Any]:
    return {
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
        "signed_at": r["signed_at"] if "signed_at" in r.keys() else None,
        "status": r["status"],
    }


@fa_bp.get("/api/bid_history")
def api_bid_history():
    enforce_expirations()
    search = (request.args.get("search") or "").strip().lower()
    authed_team = session.get("authed_team", "") or ""

    conn = get_conn()
    cur = conn.cursor()

    team_summary: List[Dict[str, Any]] = []
    if authed_team:
        team_variants = bid_team_variants(authed_team)
        if team_variants:
            placeholders = ",".join("?" for _ in team_variants)
            cur.execute(f"""
                SELECT
                  b.*,
                  p.name AS player_name,
                  p.signed_at AS signed_at
                FROM bids b
                JOIN free_agents p ON p.id=b.player_id
                WHERE b.team IN ({placeholders})
                  AND (
                    (b.status='ACTIVE' AND (p.signed_team IS NULL OR p.signed_team=''))
                    OR b.status='SIGNED'
                  )
                ORDER BY
                  CASE WHEN b.status='ACTIVE' THEN 0 ELSE 1 END,
                  datetime(COALESCE(p.signed_at, b.expires_at, b.created_at)) DESC
            """, team_variants)
            team_summary = [_bid_history_json_row(r) for r in cur.fetchall()]

    q = """
        SELECT
          b.*,
          p.name AS player_name,
          p.signed_at AS signed_at
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

    out = [_bid_history_json_row(r) for r in rows]
    return jsonify({
        "bids": out,
        "authed_team": authed_team,
        "team_summary": team_summary,
    })

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
    ap.add_argument("--sync-ratings", action="store_true", help="Import/refresh OOTP ratings from the merged main + FA ratings CSVs into roster.db")
    ap.add_argument("--apply-htd", action="store_true", help="Apply hometown discounts from hometown_discounts.db to current free agents")
    ap.add_argument("--sync-signed-fa-rosters", action="store_true", help="Repair roster.db from fa.db SIGNED free-agent bids")
    ap.add_argument("--prefetch-headshots", action="store_true", help="Download headshots for all players with mlbam_id")
    args = ap.parse_args()

    if args.sync_signed_fa_rosters:
        init_db()
        summary = reconcile_signed_free_agents_to_roster()
        print(f"✅ Signed FA roster reconciliation complete: {summary}")
        raise SystemExit(0)

    if args.sync_roster:
        init_db()
        sync_free_agents_from_roster_if_needed(force=True)
        print("✅ Roster free agents synced, including merged OOTP ratings.")
        if args.apply_htd:
            summary = assign_hometown_discounts_now(clear_missing=True)
            print(f"✅ Hometown discounts applied: {summary}")
        raise SystemExit(0)

    if args.sync_ratings:
        init_db()
        files_loaded, rows_loaded, updated = update_roster_db_ratings_from_exports()
        unrostered, upserted = sync_free_agents_from_roster_db()
        print(
            f"✅ OOTP ratings synced from {files_loaded} file(s): "
            f"{rows_loaded} rating rows, {updated} roster rows updated; "
            f"{upserted}/{unrostered} FA rows refreshed."
        )
        raise SystemExit(0)

    if args.apply_htd:
        init_db()
        summary = assign_hometown_discounts_now(clear_missing=True)
        print(f"✅ Hometown discounts applied: {summary}")
        raise SystemExit(0)

    if args.sync_registry:
        init_db()
        import_player_registry_csv(PLAYER_REGISTRY_CSV)
        print("✅ Registry synced.")
        if args.sync_roster:
            sync_free_agents_from_roster_if_needed(force=True)
            print("✅ Roster free agents synced.")
        if args.apply_htd:
            summary = assign_hometown_discounts_now(clear_missing=True)
            print(f"✅ Hometown discounts applied: {summary}")
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

