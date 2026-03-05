#!/usr/bin/env python3
"""
Flask Draft App — Baseball Draft Framework
-----------------------------------------
A single-file Flask application to run a simple league draft with:
- CSV import for players and draft order
- Substring search over player names
- Team selection dropdown (MLB teams)
- Current pick tracking (round/pick/team)
- Draft button appears *only* when your selected team is on the clock
- Owned players are grayed out, with a toggle to hide them

Quickstart
==========
1) Ensure you have Flask installed:  
   pip install Flask

2) Save this file as app.py and run it:  
   python app.py

3) On first run, if no `players.csv` and `draft_order.csv` are present, 
   sample CSVs will be generated automatically for a demo 8-team, 10-round draft.

4) Visit http://bnsldraft.onrender.com/ in your browser.

CSV Schemas
===========
players.csv columns (exact, case-insensitive accepted):
- Player name
- DOB
- position
- franchise
- Draft eligible?

Note: `franchise` can be blank for undrafted players. `Draft eligible?` can be true/false, 1/0, yes/no.

draft_order.csv columns:
- Round
- Pick
- Team

Database
========
- SQLite file `draft.db` is created beside this script.
- Tables: players, draft_order
- `draft_order.player_id` is NULL until the pick is made; after drafting, it stores the chosen player's id.

To reset: delete `draft.db` and restart the app (it will re-import CSVs / regenerate sample CSVs if missing).
"""
from __future__ import annotations
from datetime import datetime, timedelta   # add timedelta
import csv
import os
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, List
from draft_order_page import order_bp, EASTERN, next_non_sunday_date, END_OF_DAY_MISS_HOUR

import unicodedata
from flask import (
    Flask, request, jsonify, session, redirect, url_for, render_template_string, abort
)
import logging

from flask import current_app

APP_DIR = Path(__file__).resolve().parent

env_db = os.environ.get("DB_PATH")
#if env_db:
#    DB_PATH = Path(env_db)
#elif Path("/data").exists():
#    DB_PATH = Path("/data/draft.db")
#else:
DB_PATH = APP_DIR / "draft.db"   # ← local default

# Make sure /data exists so SQLite can create the file there.
DB_PATH.parent.mkdir(parents=True, exist_ok=True)

PLAYERS_CSV = APP_DIR / "players.csv"
DRAFT_ORDER_CSV = APP_DIR / "draft_order.csv"

from flask import Blueprint
draft_bp = Blueprint("draft", __name__)


from flask import current_app

# --- Blueprint-safe accessors (no global `app` here) ---
def get_db_path() -> Path:
    # Use the Flask app's config if set (factory pattern), else fall back.
    cfg = current_app.config.get("DB_PATH")
    return Path(cfg) if cfg else DB_PATH

@draft_bp.before_app_request
def _ensure_db_dir_exists():
    # Runs with an application context, so current_app is available.
    p = get_db_path()
    p.parent.mkdir(parents=True, exist_ok=True)

# Logging: don't touch app.logger here; let app.py configure logging if you want.
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

# Map all teams to four test emails (round-robin) so you can exercise the system easily.
#_TEST_EMAILS = [
#    "ryanschmitz43@yahoo.com",
#    "ryanschmitz43@gmail.com",
#    "schmitz@ucsb.edu",
#    "condor2199@yahoo.com",
#]
#TEAM_EMAILS = {team: _TEST_EMAILS[i % len(_TEST_EMAILS)] for i, team in enumerate(MLB_TEAMS)}

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


# -------------------------
# Utility Helpers
# -------------------------

import smtplib
from email.message import EmailMessage

def init_meta():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
      CREATE TABLE IF NOT EXISTS app_meta (
        key TEXT PRIMARY KEY,
        value TEXT
      )
    """)
    conn.commit()
    conn.close()

def remove_player_from_all_queues(player_id: int):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("DELETE FROM draft_queue WHERE player_id = ?", (player_id,))
    conn.commit()
    conn.close()

def get_team_queue(team: str) -> list[sqlite3.Row]:
    """
    Return this team's queue in queue order, including full player fields
    so the queue page can render the same columns as the main page.
    """
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT
            dq.player_id,
            dq.position AS qpos,
            p.name,
            p.dob,
            p.position,
            p.franchise,
            COALESCE(p.eligible, 1) AS eligible,
            p.mlbamid,
            p.bats,
            p.throws,
            p.dob_month,
            p.dob_day,
            p.dob_year,
            p.mlb_org,
            p.fg_30, p.fg_fv, p.mlb_30, p.mlb_fv, p.fg100, p.mlb100
        FROM draft_queue dq
        JOIN players p ON p.id = dq.player_id
        WHERE dq.team = ?
        ORDER BY dq.position ASC, dq.id ASC
    """, (team,))
    rows = cur.fetchall()
    conn.close()
    return rows



def get_team_queue_top_available(team: str) -> int | None:
    """
    Return the first *available & eligible* player in this team's draft_queue,
    scanning in true queue order (position ASC, then FIFO).
    """
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT dq.player_id
          FROM draft_queue dq
         WHERE dq.team = ?
         ORDER BY dq.position ASC, dq.id ASC
    """, (team,))
    pids = [int(r[0]) for r in cur.fetchall()]
    if not pids:
        conn.close()
        return None

    qmarks = ",".join("?" for _ in pids)
    cur.execute(f"""
        SELECT id, franchise, COALESCE(eligible,1) AS eligible
          FROM players
         WHERE id IN ({qmarks})
    """, pids)
    by_id = {int(r["id"]): r for r in cur.fetchall()}

    for pid in pids:
        row = by_id.get(pid)
        if not row:
            continue
        if (not row["franchise"]) and int(row["eligible"] or 0) == 1:
            conn.close()
            return pid
    conn.close()
    return None


def set_queue_mode(team: str, use_at_start: bool):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
      INSERT INTO team_prefs(team, use_queue_at_start) VALUES(?, ?)
      ON CONFLICT(team) DO UPDATE SET use_queue_at_start=excluded.use_queue_at_start
    """, (team, 1 if use_at_start else 0))
    conn.commit()
    conn.close()

def get_queue_mode(team: str) -> bool:
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT use_queue_at_start FROM team_prefs WHERE team=?", (team,))
    row = cur.fetchone()
    conn.close()
    return bool(row[0]) if row else False  # default: end-of-clock

def perform_draft_internal(team: str, player_id: int, draft_order_id: int) -> None:
    """Bypass session—used for auto-draft from queue. Raises on failure."""
    conn = get_conn()
    cur = conn.cursor()
    # validate availability
    cur.execute("SELECT franchise, eligible FROM players WHERE id=?", (player_id,))
    prow = cur.fetchone()
    if not prow:
        conn.close()
        raise RuntimeError("player not found")
    if prow["franchise"]:
        conn.close()
        raise RuntimeError("player already owned")
    if int(prow["eligible"] or 0) != 1:
        conn.close()
        raise RuntimeError("player not eligible")

    # assign
    cur.execute("UPDATE players SET franchise=? WHERE id=?", (team, player_id))
    cur.execute(
        "UPDATE draft_order SET player_id=?, drafted_at=? WHERE id=?",
        (player_id, datetime.utcnow().isoformat(timespec='seconds'), draft_order_id)
    )
    conn.commit()
    # after successfully updating players + draft_order and committing:
    try:
        notify_discord_pick(draft_order_id)
    except Exception as e:
        print(f"[discord] failed: {e}")
    conn.close()
    # clean queues
    remove_player_from_all_queues(player_id)

def get_team_queue_top_available(team: str) -> int | None:
    """
    Return the first *available & eligible* player in this team's draft_queue,
    scanning in true queue order (position ASC, then FIFO).
    """
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT dq.player_id
          FROM draft_queue dq
         WHERE dq.team = ?
         ORDER BY dq.position ASC, dq.id ASC
    """, (team,))
    pids = [int(r[0]) for r in cur.fetchall()]
    if not pids:
        conn.close()
        return None

    qmarks = ",".join("?" for _ in pids)
    cur.execute(f"""
        SELECT id, franchise, COALESCE(eligible,1) AS eligible
          FROM players
         WHERE id IN ({qmarks})
    """, pids)
    by_id = {int(r["id"]): r for r in cur.fetchall()}

    for pid in pids:
        row = by_id.get(pid)
        if not row:
            continue
        if (not row["franchise"]) and int(row["eligible"] or 0) == 1:
            conn.close()
            return pid
    conn.close()
    return None

def _team_queue_count(team: str) -> int:
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM draft_queue WHERE team=?", (team,))
    n = int(cur.fetchone()[0] or 0)
    conn.close()
    return n

def get_meta(key: str) -> str | None:
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT value FROM app_meta WHERE key=?", (key,))
    row = cur.fetchone()
    conn.close()
    return row[0] if row else None

def set_meta(key: str, value: str):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
      INSERT INTO app_meta(key, value) VALUES(?,?)
      ON CONFLICT(key) DO UPDATE SET value=excluded.value
    """, (key, value))
    conn.commit()
    conn.close()

def send_email(to_addr: str, subject: str, body: str):
    """
    Minimal SMTP sender. Configure via env:
      SMTP_HOST, SMTP_PORT, SMTP_USERNAME, SMTP_PASSWORD, SMTP_FROM
    Extra toggles:
      SMTP_STARTTLS=1 (default) -> attempt STARTTLS if server advertises it
      SMTP_REQUIRE_TLS=0 (default) -> if 1, fail if STARTTLS not available
    Falls back to console print if SMTP env isn’t provided.
    """
    host = os.environ.get("SMTP_HOST")
    port = int(os.environ.get("SMTP_PORT", "0") or "0")
    user = os.environ.get("SMTP_USERNAME")
    pw   = os.environ.get("SMTP_PASSWORD")
    from_addr = os.environ.get("SMTP_FROM", "draftbot@localhost")

    if not host or not port:
        print(f"[EMAIL-DRYRUN] To: {to_addr}\nSubject: {subject}\n\n{body}\n---")
        return

    starttls_wanted = (os.environ.get("SMTP_STARTTLS", "1").lower() in ("1", "true", "yes"))
    require_tls     = (os.environ.get("SMTP_REQUIRE_TLS", "0").lower() in ("1", "true", "yes"))

    from email.message import EmailMessage
    msg = EmailMessage()
    msg["From"] = from_addr
    msg["To"] = to_addr
    msg["Subject"] = subject
    msg.set_content(body)

    import smtplib
    with smtplib.SMTP(host, port, timeout=15) as s:
        s.ehlo()
        if starttls_wanted:
            try:
                if s.has_extn("starttls"):
                    s.starttls()
                    s.ehlo()
                elif require_tls:
                    raise RuntimeError("STARTTLS required but not supported by server")
            except Exception as e:
                if require_tls:
                    raise
                # Otherwise continue without TLS in dev
                print(f"[EMAIL] STARTTLS not used: {e}")
        if user and pw:
            s.login(user, pw)
        s.send_message(msg)

def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    # Register unaccent(text) for accent-insensitive search/sort
    def _unaccent(s):
        if s is None:
            return ""
        # NFKD -> strip combining marks
        return "".join(ch for ch in unicodedata.normalize("NFKD", str(s)) if not unicodedata.combining(ch))
    conn.create_function("unaccent", 1, _unaccent)

    return conn


def emails_equal(a: str | None, b: str | None) -> bool:
    if not a or not b:
        return False
    return a.strip().lower() == b.strip().lower()

from zoneinfo import ZoneInfo
EASTERN = ZoneInfo("America/New_York")

def fmt_email_et(dt: datetime) -> str:
    """Return 'Fri, Nov 1, 2025 at 1:00 PM ET' in local Eastern time, no ISO."""
    local = dt.astimezone(EASTERN)
    # Try Unix-style %-d / %-I (no leading zeros); fall back on Windows-friendly format
    try:
        return local.strftime("%a, %b %-d, %Y at %-I:%M %p ET")
    except ValueError:
        # Windows doesn't support %-d / %-I; use zero-padded and tidy up.
        s = local.strftime("%a, %b %d, %Y at %I:%M %p ET")
        # remove leading zeros from day/hour (cosmetic)
        s = s.replace(" 0", " ")
        if s.startswith("0"):
            s = s[1:]
        return s


def init_db():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS players (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            dob TEXT,
            position TEXT,
            franchise TEXT,
            eligible INTEGER NOT NULL DEFAULT 1,
            mlbamid INTEGER,
            first TEXT,
            last TEXT,
            bats TEXT,
            throws TEXT,
            dob_month INTEGER,
            dob_day INTEGER,
            dob_year INTEGER,
            mlb_org TEXT
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS draft_order (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            round INTEGER NOT NULL,
            pick INTEGER NOT NULL,
            team TEXT NOT NULL,
            player_id INTEGER,
            drafted_at TEXT,
            label TEXT,
            UNIQUE(round, pick) ON CONFLICT IGNORE
        )
    """)
    # --- Draft queue + prefs ---
    cur.execute("""
        CREATE TABLE IF NOT EXISTS draft_queue (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            team TEXT NOT NULL,
            player_id INTEGER NOT NULL,
            position INTEGER NOT NULL,
            created_at TEXT NOT NULL,
            UNIQUE(team, player_id) ON CONFLICT IGNORE
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS team_prefs (
            team TEXT PRIMARY KEY,
            use_queue_at_start INTEGER NOT NULL DEFAULT 0   -- 0=end-of-clock (default), 1=start-of-clock
        )
    """)

    # Ensure added columns exist (no-ops if already there)
    cur.execute("PRAGMA table_info(players)")
    pcols = {row[1] for row in cur.fetchall()}
    for col, typ in [
        ("mlbamid","INTEGER"),("first","TEXT"),("last","TEXT"),("bats","TEXT"),
        ("throws","TEXT"),("dob_month","INTEGER"),("dob_day","INTEGER"),
        ("dob_year","INTEGER"),("mlb_org","TEXT"),
    ]:
        if col not in pcols:
            cur.execute(f"ALTER TABLE players ADD COLUMN {col} {typ}")

    cur.execute("PRAGMA table_info(draft_order)")
    dcols = {row[1] for row in cur.fetchall()}
    if "label" not in dcols:
        cur.execute("ALTER TABLE draft_order ADD COLUMN label TEXT")

    # ---- Unique indexes for idempotent import ----
    # Unique when mlbamid is present (>0)
    cur.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS players_unique_mlbamid
        ON players(mlbamid)
        WHERE mlbamid IS NOT NULL AND mlbamid > 0
    """)
    # Fallback uniqueness by (name,dob) only when mlbamid is missing/0
    cur.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS players_unique_name_dob
        ON players(name, dob)
        WHERE (mlbamid IS NULL OR mlbamid = 0)
    """)    
    # Ensure 'label' exists if table pre-existed
    cur.execute("PRAGMA table_info(draft_order)")
    cols = {row[1] for row in cur.fetchall()}
    if "label" not in cols:
        cur.execute("ALTER TABLE draft_order ADD COLUMN label TEXT")

    # — Add-on columns for the richer playerlist.csv —
    # (SQLite: ALTER TABLE ADD COLUMN is idempotent if we first check pragma_table_info)
    cur.execute("PRAGMA table_info(players)")
    cols = {row[1] for row in cur.fetchall()}
    add_cols = []
    if "mlbamid" not in cols:     add_cols.append(("mlbamid", "INTEGER"))
    if "first" not in cols:       add_cols.append(("first", "TEXT"))
    if "last" not in cols:        add_cols.append(("last", "TEXT"))
    if "bats" not in cols:        add_cols.append(("bats", "TEXT"))
    if "throws" not in cols:      add_cols.append(("throws", "TEXT"))
    if "dob_month" not in cols:   add_cols.append(("dob_month", "INTEGER"))
    if "dob_day" not in cols:     add_cols.append(("dob_day", "INTEGER"))
    if "dob_year" not in cols:    add_cols.append(("dob_year", "INTEGER"))
    if "mlb_org" not in cols:     add_cols.append(("mlb_org", "TEXT"))

    for col, typ in add_cols:
        cur.execute(f"ALTER TABLE players ADD COLUMN {col} {typ}")
    # — Add-on columns for draft grades —
    cur.execute("PRAGMA table_info(players)")
    pcols2 = {row[1] for row in cur.fetchall()}
    for col in ("fg_30","fg_fv","mlb_30","mlb_fv","fg100","mlb100"):
        if col not in pcols2:
            cur.execute(f"ALTER TABLE players ADD COLUMN {col} INTEGER")
    conn.commit()
    conn.close()

OWNER_TO_FULL = {
    "Diamondbacks": "Arizona Diamondbacks",
    "Braves": "Atlanta Braves",
    "Orioles": "Baltimore Orioles",
    "Red Sox": "Boston Red Sox",
    "Cubs": "Chicago Cubs",
    "White Sox": "Chicago White Sox",
    "Reds": "Cincinnati Reds",
    "Guardians": "Cleveland Guardians",
    "Rockies": "Colorado Rockies",
    "Tigers": "Detroit Tigers",
    "Astros": "Houston Astros",
    "Royals": "Kansas City Royals",
    "Angels": "Los Angeles Angels",
    "Dodgers": "Los Angeles Dodgers",
    "Marlins": "Miami Marlins",
    "Brewers": "Milwaukee Brewers",
    "Twins": "Minnesota Twins",
    "Mets": "New York Mets",
    "Yankees": "New York Yankees",
    "Athletics": "Oakland Athletics",
    "Phillies": "Philadelphia Phillies",
    "Pirates": "Pittsburgh Pirates",
    "Padres": "San Diego Padres",
    "Giants": "San Francisco Giants",
    "Mariners": "Seattle Mariners",
    "Cardinals": "St. Louis Cardinals",
    "Rays": "Tampa Bay Rays",
    "Rangers": "Texas Rangers",
    "Blue Jays": "Toronto Blue Jays",
    "Nationals": "Washington Nationals",
}

# --- Discord: team abbreviations for message like "3.1 [TB]: ..."
TEAM_ABBR = {
    "Arizona Diamondbacks": "ARI",
    "Atlanta Braves": "ATL",
    "Baltimore Orioles": "BAL",
    "Boston Red Sox": "BOS",
    "Chicago Cubs": "CHC",
    "Chicago White Sox": "CWS",
    "Cincinnati Reds": "CIN",
    "Cleveland Guardians": "CLE",
    "Colorado Rockies": "COL",
    "Detroit Tigers": "DET",
    "Houston Astros": "HOU",
    "Kansas City Royals": "KC",
    "Los Angeles Angels": "LAA",
    "Los Angeles Dodgers": "LAD",
    "Miami Marlins": "MIA",
    "Milwaukee Brewers": "MIL",
    "Minnesota Twins": "MIN",
    "New York Mets": "NYM",
    "New York Yankees": "NYY",
    "Oakland Athletics": "OAK",
    "Philadelphia Phillies": "PHI",
    "Pittsburgh Pirates": "PIT",
    "San Diego Padres": "SD",
    "San Francisco Giants": "SF",
    "Seattle Mariners": "SEA",
    "St. Louis Cardinals": "STL",
    "Tampa Bay Rays": "TB",
    "Texas Rangers": "TEX",
    "Toronto Blue Jays": "TOR",
    "Washington Nationals": "WSH",
}

def _discord_post(content: str) -> None:
    """Post a simple message to Discord via webhook. Uses stdlib only, with diagnostics."""
    url = os.environ.get("DISCORD_WEBHOOK_URL")
    if not url:
        print(f"[DISCORD-DRYRUN] {content}")
        return

    import json, urllib.request, urllib.error, time
    payload = {"content": content}
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=data,
        headers={
            "Content-Type": "application/json",
            "User-Agent": "bnsl-draft-bot/1.0 (+https://bnsldraft.onrender.com)"
        },
        method="POST",
    )

    def do_post():
        with urllib.request.urlopen(req, timeout=15) as resp:
            # Success is usually 204 No Content
            if resp.status in (200, 204):
                return True, resp.status, b""
            # Non-2xx with body
            body = resp.read()
            return False, resp.status, body

    try:
        ok, status, body = do_post()
        if ok:
            print(f"[DISCORD] ok status={status}")
            return

        # Handle rate limit (429) once
        if status == 429:
            retry_after = 1.0
            try:
                # discord returns JSON: {"retry_after": seconds, ...}
                obj = json.loads(body.decode("utf-8", "ignore"))
                retry_after = float(obj.get("retry_after", retry_after))
            except Exception:
                pass
            print(f"[DISCORD] rate limited, retrying after {retry_after}s")
            time.sleep(min(3.0, max(0.5, retry_after)))
            ok2, status2, body2 = do_post()
            if ok2:
                print(f"[DISCORD] ok after retry status={status2}")
                return
            print(f"[DISCORD] failed after retry status={status2} body={body2[:300]!r}")
            return

        print(f"[DISCORD] non-2xx status={status} body={body[:300]!r}")

    except urllib.error.HTTPError as e:
        body = e.read() if hasattr(e, "read") else b""
        print(f"[DISCORD] HTTPError status={getattr(e, 'code', '???')} body={body[:300]!r}")
    except urllib.error.URLError as e:
        # This will show SSL errors / DNS / connection issues if any
        print(f"[DISCORD] URLError: {e}")
    except Exception as e:
        print(f"[DISCORD] post failed: {e}")



def notify_discord_pick(draft_order_id: int) -> None:
    """
    Look up the just-made pick and send a Discord message like:
      3.1 [TB]: CF Kenedy Corona [DOB = 2000-03-21]
    """
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT 
          d.id, d.round, d.pick, COALESCE(d.label, printf('%d.%02d', d.round, d.pick)) AS pick_label,
          d.team,
          p.name, p.position, p.dob, p.first, p.last
        FROM draft_order d
        JOIN players p ON p.id = d.player_id
        WHERE d.id = ?
    """, (draft_order_id,))
    row = cur.fetchone()
    conn.close()
    if not row:
        return

    team_full = row["team"]
    abbr = TEAM_ABBR.get(team_full, team_full)
    pick_label = row["pick_label"]

    # Best-effort position & name for the format you want
    pos = (row["position"] or "").strip() or "—"
    name = row["name"] or f"{(row['first'] or '').strip()} {(row['last'] or '').strip()}".strip()
    dob = (row["dob"] or "").strip() or "—"

    # Example: 3.1 [TB]: CF Kenedy Corona [DOB = 2000-03-21]
    content = f"{pick_label} [{abbr}]: {pos} {name} [DOB = {dob}]"
    _discord_post(content)


def ensure_player_unique_indexes():
    """Deduplicate, then add the UNIQUE indexes required by UPSERT."""
    conn = get_conn()
    cur = conn.cursor()

    # 1) Deduplicate by mlbamid (>0): keep the smallest id
    cur.execute("""
        DELETE FROM players
        WHERE mlbamid IS NOT NULL AND mlbamid > 0
          AND id NOT IN (
            SELECT MIN(id) FROM players
            WHERE mlbamid IS NOT NULL AND mlbamid > 0
            GROUP BY mlbamid
          )
    """)

    # 2) Deduplicate by (name, dob) where mlbamid missing/0: keep the smallest id
    cur.execute("""
        DELETE FROM players
        WHERE (mlbamid IS NULL OR mlbamid = 0)
          AND (name, IFNULL(dob, '')) IN (
            SELECT name, IFNULL(dob, '')
            FROM players
            WHERE (mlbamid IS NULL OR mlbamid = 0)
            GROUP BY name, IFNULL(dob, '')
            HAVING COUNT(*) > 1
          )
          AND id NOT IN (
            SELECT MIN(id) FROM players
            WHERE (mlbamid IS NULL OR mlbamid = 0)
            GROUP BY name, IFNULL(dob, '')
          )
    """)

    # 3) Add UNIQUE indexes (partial) that the UPSERT targets
    cur.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS players_unique_mlbamid
        ON players(mlbamid)
        WHERE mlbamid IS NOT NULL AND mlbamid > 0
    """)
    cur.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS players_unique_name_dob
        ON players(name, dob)
        WHERE (mlbamid IS NULL OR mlbamid = 0)
    """)

    conn.commit()
    conn.close()


def import_players_from_playerlist(path: Path):
    """
    Idempotent import w/o SQLite UPSERT:
      - If MLBAMID > 0 -> upsert on mlbamid
      - Else           -> upsert on (name, dob)
    Never overwrites an existing franchise or eligible.
    """
    if not path.exists():
        return
    conn = get_conn()
    cur = conn.cursor()

    def safe_update(existing_row, values):
        # preserve franchise/eligible if already set
        keep_franchise = existing_row["franchise"]
        keep_eligible  = int(existing_row["eligible"] or 0)

        cur.execute("""
            UPDATE players
               SET name      = ?,
                   dob       = ?,
                   position  = ?,
                   bats      = ?,
                   throws    = ?,
                   dob_month = ?,
                   dob_day   = ?,
                   dob_year  = ?,
                   mlb_org   = ?,
                   franchise = COALESCE(NULLIF(?, ''), franchise),
                   eligible  = ?,
                   fg_30     = ?,
                   fg_fv     = ?,
                   mlb_30    = ?,
                   mlb_fv    = ?,
                   fg100     = ?,
                   mlb100    = ?
             WHERE id = ?
        """, (
            values["name"], values["dob"], values["position"],
            values["bats"], values["throws"],
            values["dob_month"], values["dob_day"], values["dob_year"], values["mlb_org"],
            "",                    # don't clobber franchise during import
            keep_eligible,         # preserve eligible flag
            values["fg_30"], values["fg_fv"], values["mlb_30"], values["mlb_fv"], values["fg100"], values["mlb100"],
            existing_row["id"],
        ))

    with path.open(newline='', encoding='utf-8') as f:
        r = csv.DictReader(f)
        for row in r:
            mlbamid = int(row.get("MLBAMID") or 0)
            name    = (row.get("Name") or "").strip()
            bats    = (row.get("Bats") or "").strip()
            throws  = (row.get("Throws") or "").strip()
            pos     = (row.get("Position") or "").strip()
            dob_m   = int(row.get("DOB_Month") or 0)
            dob_d   = int(row.get("DOB_Day") or 0)
            dob_y   = int(row.get("DOB_Year") or 0)
            org     = (row.get("MLB org.") or "").strip()
            fg_30  = row.get("FG_30")   or row.get("FG30")   or ""
            fg_fv  = row.get("FG_FV")   or row.get("FGFV")   or ""
            mlb_30 = row.get("MLB_30")  or row.get("MLB30")  or ""
            mlb_fv = row.get("MLB_FV")  or row.get("MLBFV")  or ""
            fg100  = row.get("FG100")   or ""
            mlb100 = row.get("MLB100")  or ""

            def _int_or_none(x):
                try:
                    x = str(x).strip()
                    return int(x) if x != "" else None
                except Exception:
                    return None

            dob = ""
            if dob_y and dob_m and dob_d:
                dob = f"{dob_y:04d}-{dob_m:02d}-{dob_d:02d}"

            values = {
                "mlbamid": mlbamid if mlbamid > 0 else None,
                "name": name,
                "dob": dob or None,
                "position": pos,
                "bats": bats,
                "throws": throws,
                "dob_month": dob_m or None,
                "dob_day": dob_d or None,
                "dob_year": dob_y or None,
                "mlb_org": org,
            }
            values.update({
               "fg_30":  _int_or_none(fg_30),
               "fg_fv":  _int_or_none(fg_fv),
               "mlb_30": _int_or_none(mlb_30),
               "mlb_fv": _int_or_none(mlb_fv),
               "fg100":  _int_or_none(fg100),
               "mlb100": _int_or_none(mlb100),
            })

            if mlbamid > 0:
                # Upsert on mlbamid
                cur.execute("SELECT id, franchise, eligible FROM players WHERE mlbamid = ?", (mlbamid,))
                existing = cur.fetchone()
                if existing:
                    safe_update(existing, values)
                else:
                    cur.execute("""
                        INSERT INTO players
                          (mlbamid, name, dob, position, franchise, eligible,
                           bats, throws, dob_month, dob_day, dob_year, mlb_org, fg_30, fg_fv, mlb_30, mlb_fv, fg100, mlb100)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (values["mlbamid"], values["name"], values["dob"], values["position"],
                          "", 1, values["bats"], values["throws"],
                          values["dob_month"], values["dob_day"], values["dob_year"], values["mlb_org"],values["fg_30"], values["fg_fv"], values["mlb_30"], values["mlb_fv"], values["fg100"], values["mlb100"]))
            else:
                # Upsert on (name, dob) when no mlbamid
                cur.execute("""
                    SELECT id, franchise, eligible FROM players
                     WHERE (mlbamid IS NULL OR mlbamid = 0)
                       AND name = ?
                       AND ((dob IS NULL AND ? IS NULL) OR dob = ?)
                """, (values["name"], values["dob"], values["dob"]))
                existing = cur.fetchone()
                if existing:
                    safe_update(existing, values)
                else:
                    cur.execute("""
                        INSERT INTO players
                          (mlbamid, name, dob, position, franchise, eligible,
                           bats, throws, dob_month, dob_day, dob_year, mlb_org, fg_30, fg_fv, mlb_30, mlb_fv, fg100, mlb100)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (None, values["name"], values["dob"], values["position"],
                          "", 1, values["bats"], values["throws"],
                          values["dob_month"], values["dob_day"], values["dob_year"], values["mlb_org"],values["fg_30"], values["fg_fv"], values["mlb_30"], values["mlb_fv"], values["fg100"], values["mlb100"]))

    conn.commit()
    conn.close()



import re

_RP_NORMAL = re.compile(r"^\s*(\d+)\.(\d+)\s*$")   # e.g. 1.01
_RP_COMP   = re.compile(r"^\s*[Cc]\s*(\d+)\.(\d+)\s*$")  # e.g. C2.01

def parse_round_pick_token(token: str) -> dict:
    """
    Returns a dict:
      {
        "round": int,           # base round number
        "pick_sort": int,       # integer used for ORDER BY within round
        "label": str            # original/human label to display
      }
    Normal: '1.01' -> round=1, pick_sort=1,  label='1.01'
    Comp:   'C2.03'-> round=2, pick_sort=30+3, label='C2.03'
    """
    s = (token or "").strip()
    m = _RP_COMP.match(s)
    if m:
        r = int(m.group(1))
        k = int(m.group(2))
        return {"round": r, "pick_sort": 30 + k, "label": s}  # after 2.30, before 3.01
    m = _RP_NORMAL.match(s)
    if m:
        r = int(m.group(1))
        k = int(m.group(2))
        return {"round": r, "pick_sort": k, "label": f"{r}.{str(k).zfill(2)}"}
    # fallback
    return {"round": 0, "pick_sort": 0, "label": s}

def import_draft_order_from_pickorder(path: Path, reset: bool = False):
    """
    pickorder.csv columns:
      Overall Pick,Round/Pick,Slot,Owner,Day,Time,Date

    If reset=False (default): upsert rows without touching drafted picks.
    If reset=True: clear the table first (use only when you truly want to reset a draft).
    """
    if not path.exists():
        return

    conn = get_conn()
    cur = conn.cursor()

    if reset:
        cur.execute("DELETE FROM draft_order")

    with path.open(newline='', encoding='utf-8') as f:
        r = csv.DictReader(f)
        for row in r:
            token = row.get("Round/Pick") or ""
            owner = row.get("Owner") or row.get("Slot") or ""
            parsed = parse_round_pick_token(token)  # you already have this
            rnd, pks, label = parsed["round"], parsed["pick_sort"], parsed["label"]
            team = normalize_team(owner)

            if rnd > 0 and pks > 0 and team:
                # Insert new row; if it exists:
                #  - If undrafted, refresh team/label from CSV
                #  - If drafted, leave as-is
                cur.execute("""
                    INSERT INTO draft_order (round, pick, team, label)
                    VALUES (?, ?, ?, ?)
                    ON CONFLICT(round, pick) DO UPDATE SET
                        team  = CASE WHEN draft_order.player_id IS NULL THEN excluded.team  ELSE draft_order.team  END,
                        label = CASE WHEN draft_order.player_id IS NULL THEN excluded.label ELSE draft_order.label END
                """, (rnd, pks, team, label))

    conn.commit()
    conn.close()



def str_to_bool(x: str) -> bool:
    if x is None:
        return False
    s = str(x).strip().lower()
    return s in {"1","true","t","yes","y"}


def normalized_header_map(header: List[str]) -> Dict[str, int]:
    idx = {}
    for i, h in enumerate(header):
        key = h.strip().lower().replace("_", " ")
        idx[key] = i
    return idx

def normalize_team(name: str) -> str:
    n = (name or "").strip()
    return OWNER_TO_FULL.get(n, n)

def enforce_queue_actions():
    """
    Priority:
      1) END-OF-CLOCK: For any pick whose DESIGNATED deadline has arrived/passed,
         try drafting from that team's queue (in scheduled/designated order).
      2) START-OF-CLOCK: If nothing happened, and the current pick's scheduled
         time has arrived, auto-draft from queue if team has use_queue_at_start=1.
    """
    from draft_order_page import (
        _load_picks_overrides_and_designated,
        _compute_scheduled_times,
        get_current_pick_info,
        EASTERN,
    )

    safety = 0
    while safety < 50:
        safety += 1
        progressed = False
        now = datetime.now(tz=EASTERN)

        try:
            picks, designated = _load_picks_overrides_and_designated()
            if not picks:
                current_app.logger.info("[enforce] no picks; exiting")
                break
            scheduled = _compute_scheduled_times(now)
        except Exception as e:
            current_app.logger.exception("[enforce] schedule compute failed: %s", e)
            break

        # ---------- 1) END-OF-CLOCK FIRST (deadline passed → try queue) ----------
        undrafted = [i for i, r in enumerate(picks) if not r["player_id"]]
        if not undrafted:
            current_app.logger.info("[enforce] draft complete; exiting")
            break

        # Order by DESIGNATED time so deadlines match the /order miss logic
        ordered = sorted(undrafted, key=lambda i: (designated[i], i))

        far = now + timedelta(days=36500)
        deadline_by_idx = {}
        for pos, i in enumerate(ordered):
            if pos + 1 < len(ordered):
                nxt = ordered[pos + 1]
                deadline_by_idx[i] = designated[nxt]
            else:
                deadline_by_idx[i] = far

        acted_this_pass = False
        for i in ordered:
            dl = deadline_by_idx[i]
            if now < dl:
                # nothing later is overdue (ordered by designated time)
                break

            rec = picks[i]
            team = rec["team"]
            pid = get_team_queue_top_available(team)
            current_app.logger.info(
                "[enforce] OVERDUE(designated) pick_id=%s team=%s designated=%s deadline=%s now=%s top_from_queue=%s",
                rec["id"], team,
                designated[i].isoformat(timespec="minutes"),
                dl.isoformat(timespec="minutes"),
                now.isoformat(timespec="minutes"),
                pid
            )
            if pid:
                try:
                    perform_draft_internal(team, pid, int(rec["id"]))
                except Exception as e:
                    current_app.logger.exception(
                        "[enforce] perform_draft_internal failed (MISS->QUEUE) team=%s pick_id=%s pid=%s: %s",
                        team, rec["id"], pid, e
                    )
                else:
                    acted_this_pass = True
                    progressed = True
                    break  # state changed; recompute whole state

        if acted_this_pass:
            continue  # loop, recompute everything from DB/state

        # ---------- KEY CHANGE: refresh schedule once, then evaluate START-OF-CLOCK ----------
        # We *do not* bail out just because we saw overdue items with no queue action.
        # Instead, recompute the scheduled map so any evening re-miss (e.g., DET) is rolled
        # to TOMORROW's evening tail *before* we consider a start-of-clock auto-pick.
        try:
            scheduled = _compute_scheduled_times(now)  # refresh using the same 'now'
        except Exception as e:
            current_app.logger.exception("[enforce] _compute_scheduled_times refresh failed: %s", e)
            # If refresh fails, fall back to the previously computed 'scheduled'


        # ---------- 2) START-OF-CLOCK (only if nothing overdue fired) ----------
        try:
            info = get_current_pick_info()
        except Exception as e:
            current_app.logger.exception("[enforce] get_current_pick_info failed: %s", e)
            info = None

        if info:
            cur_idx = None
            for i, r in enumerate(picks):
                if r["id"] == info["id"]:
                    cur_idx = i
                    break

            if cur_idx is not None:
                team = picks[cur_idx]["team"]
                sched_t = scheduled.get(cur_idx, designated[cur_idx])
                # Gate start-of-clock on actual start time to avoid early drafting
                if now >= sched_t and get_queue_mode(team):
                    current_app.logger.info(
                        "[enforce] START-OF-CLOCK team=%s pick_id=%s sched=%s now=%s",
                        team, info["id"],
                        sched_t.isoformat(timespec="minutes"),
                        now.isoformat(timespec="minutes"),
                    )
                    pid = get_team_queue_top_available(team)
                    if pid:
                        try:
                            perform_draft_internal(team, pid, int(info["id"]))
                        except Exception as e:
                            current_app.logger.exception(
                                "[enforce] perform_draft_internal failed (START->QUEUE) team=%s pick_id=%s pid=%s: %s",
                                team, info["id"], pid, e
                            )
                        else:
                            progressed = True

        if not progressed:
            break

    # Notify after enforcement
    try:
        notify_if_new_on_clock()
    except Exception as e:
        current_app.logger.exception("[enforce] notify_if_new_on_clock failed: %s", e)


def import_players_from_csv(path: Path):
    conn = get_conn()
    cur = conn.cursor()

    if not path.exists():
        return

    with path.open(newline='', encoding='utf-8') as f:
        r = csv.reader(f)
        header = next(r)
        h = normalized_header_map(header)

        # Accept a few synonyms leniently
        name_key = next((k for k in h if k in {"player name","name"}), None)
        dob_key = next((k for k in h if k in {"dob","date of birth"}), None)
        pos_key = next((k for k in h if k in {"position"}), None)
        fr_key = next((k for k in h if k in {"franchise","team"}), None)
        el_key = next((k for k in h if k in {"draft eligible?","draft eligible","eligible"}), None)
        if not name_key or not pos_key or el_key is None:
            raise RuntimeError("players.csv missing required columns: Player name, position, Draft eligible?")

        for row in r:
            name = row[h[name_key]].strip()
            dob = row[h[dob_key]].strip() if dob_key else ""
            pos = row[h[pos_key]].strip()
            franchise = row[h[fr_key]].strip() if fr_key else ""
            eligible = 1 if str_to_bool(row[h[el_key]]) else 0
            cur.execute(
                "INSERT INTO players(name, dob, position, franchise, eligible) VALUES (?,?,?,?,?)",
                (name, dob, pos, franchise, eligible)
            )
    conn.commit()
    conn.close()


def import_draft_order_from_csv(path: Path):
    conn = get_conn()
    cur = conn.cursor()

    if not path.exists():
        return

    with path.open(newline='', encoding='utf-8') as f:
        r = csv.reader(f)
        header = next(r)
        h = normalized_header_map(header)

        round_key = next((k for k in h if k == "round"), None)
        pick_key = next((k for k in h if k == "pick"), None)
        team_key = next((k for k in h if k == "team"), None)
        if not (round_key and pick_key and team_key):
            raise RuntimeError("draft_order.csv missing required columns: Round, Pick, Team")

        for row in r:
            rnum = int(row[h[round_key]].strip())
            pnum = int(row[h[pick_key]].strip())
            team = row[h[team_key]].strip()
            cur.execute(
                "INSERT OR IGNORE INTO draft_order(round, pick, team) VALUES (?,?,?)",
                (rnum, pnum, team)
            )
    conn.commit()
    conn.close()


def get_current_pick() -> Dict[str, Any] | None:
    # Import here to avoid circulars at import time; we already registered the blueprint above.
    from draft_order_page import get_current_on_clock_pick
    current = get_current_on_clock_pick()  # time-aware, respects overrides + misses
    if current is None:
        return None

    # keep progress stats the same
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM draft_order")
    total_picks = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM draft_order WHERE player_id IS NOT NULL")
    made = cur.fetchone()[0]
    conn.close()

    current.update({
        "picks_made": made,
        "total_picks": total_picks,
    })
    return current

def notify_if_new_on_clock():
    """
    If the 'current pick' (earliest scheduled undrafted pick) changed since last notification,
    send an email to that team's address with the pick label and deadline.
    """
    # Import here to avoid circular import at module import time
    from draft_order_page import get_current_pick_info, EASTERN

    info = get_current_pick_info()
    if not info:
        return

    last_id = get_meta("last_on_clock_pick_id")
    cur_id = str(info["id"])

    if last_id == cur_id:
        return  # nothing new

    # Compose email
    team = info["team"]
    pick_label = info["pick_label"]
    deadline_iso = info["deadline_time_iso"]  # can be None for last pick

    pretty_deadline = None
    if deadline_iso:
        dt = datetime.fromisoformat(deadline_iso)
        pretty_deadline = fmt_email_et(dt)

    to_addr = TEAM_EMAILS.get(team)

    # Subject: keep concise; (optional) swap to pretty_deadline if you prefer
    subj_deadline = f" due {pretty_deadline}" if pretty_deadline else ""
    subject = f"BNSL Draft: Pick {pick_label}{subj_deadline}"

    # Body: use human-friendly ET string (no ISO T/offset gobbledygook)
    if pretty_deadline:
        body_deadline = f"Deadline: {pretty_deadline}\n"
    else:
        body_deadline = ""

    body = (
        f"You are on the clock for pick {pick_label} ({team}).\n"
        f"{body_deadline}\n"
        "Submit your selection at http://bnsldraft.onrender.com/\n"
    )

    if to_addr:
        send_email(to_addr, subject, body)

    set_meta("last_on_clock_pick_id", cur_id)


def generate_sample_csvs(players_path: Path, order_path: Path):
    """Create small demo CSVs if not present: 8 teams x 10 rounds, ~36 sample players."""
    if not players_path.exists():
        sample_players = [
            ["Player name","DOB","position","franchise","Draft eligible?"],
            ["Jackson Miller","1998-05-14","C","", "true"],
            ["Ethan Carter","1997-07-02","1B","", "true"],
            ["Liam Rodriguez","1999-09-20","2B","", "true"],
            ["Noah Kim","1996-03-08","SS","", "true"],
            ["Mason Thompson","1995-11-25","3B","", "true"],
            ["Logan Rivera","1999-12-01","OF","", "true"],
            ["Carter James","1998-02-17","OF","", "true"],
            ["Aiden Brooks","2000-06-30","OF","", "true"],
            ["Oliver Chen","1997-01-10","SP","", "true"],
            ["Elijah Nguyen","1998-08-22","SP","", "true"],
            ["Lucas Patel","1996-10-05","SP","", "true"],
            ["Alexander Park","1995-04-18","RP","", "true"],
            ["Henry Scott","1994-12-29","RP","", "true"],
            ["Levi Turner","1997-09-13","C","", "true"],
            ["Mateo Garcia","1998-07-07","1B","", "true"],
            ["Sebastian Flores","1997-05-03","2B","", "true"],
            ["Benjamin Diaz","1999-10-12","SS","", "true"],
            ["Daniel Ortiz","1996-02-26","3B","", "true"],
            ["Michael Hughes","1995-08-09","OF","", "true"],
            ["Samuel Reed","1998-03-15","OF","", "true"],
            ["Wyatt Morales","1999-01-22","OF","", "true"],
            ["Julian Rivera","1997-11-04","SP","", "true"],
            ["Grayson Lee","1996-06-16","SP","", "true"],
            ["Hudson Clark","1995-09-27","SP","", "true"],
            ["Asher Gomez","1998-12-19","RP","", "true"],
            ["Caleb Perry","1997-04-08","RP","", "true"],
            ["Nolan Baker","1999-07-01","C","", "true"],
            ["Easton Ward","1996-05-21","1B","", "true"],
            ["Ezra Watson","1998-02-03","2B","", "true"],
            ["Adrian Torres","1997-10-14","SS","", "true"],
            ["Aaron Foster","1995-03-11","3B","", "true"],
            ["Leo Simmons","1998-08-28","OF","", "true"],
            ["Josiah Price","1996-01-31","OF","", "true"],
            ["Anthony Ross","1999-06-06","OF","", "true"],
            ["Dominic Bell","1997-09-09","SP","", "true"],
            ["Brayden Ward","1995-12-23","SP","", "true"],
            ["Ian Cooper","1998-03-03","RP","", "true"],
            ["Carson Gray","1996-07-17","RP","", "true"],
            # A couple owned / ineligible examples
            ["Prospect Owned","2002-01-01","SS","New York Yankees", "true"],
            ["Ineligible Guy","1990-01-01","OF","", "false"],
        ]
        with players_path.open('w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerows(sample_players)

    if not order_path.exists():
        # 8 demo teams from MLB for readability
        teams = [
            "New York Yankees","Los Angeles Dodgers","Atlanta Braves","Chicago Cubs",
            "Houston Astros","Boston Red Sox","San Francisco Giants","St. Louis Cardinals",
        ]
        rows = [["Round","Pick","Team"]]
        rounds = 10
        for r in range(1, rounds + 1):
            # snake draft? Spec didn't require; use straight order for simplicity
            order = teams if r % 2 == 1 else teams  # keep straight order
            for i, t in enumerate(order, start=1):
                rows.append([r, i, t])
        with order_path.open('w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerows(rows)


# -------------------------
# Initial Setup on Startup
# -------------------------

init_db()
init_meta()
# Count what's already in the DB
conn_chk = get_conn()
cur_chk = conn_chk.cursor()
cur_chk.execute("SELECT COUNT(*) FROM players")
players_count = cur_chk.fetchone()[0]
cur_chk.execute("SELECT COUNT(*) FROM draft_order")
order_count = cur_chk.fetchone()[0]
conn_chk.close()

# Import players (your new manual upsert version is idempotent, so this is safe if you want)
if PLAYERS_CSV.exists() and players_count == 0:
    import_players_from_playerlist(PLAYERS_CSV)

# Import draft order ONLY when the table is empty
if DRAFT_ORDER_CSV.exists() and order_count == 0:
    import_draft_order_from_pickorder(DRAFT_ORDER_CSV)

# Optional: allow an explicit reset via env var
#if os.environ.get("RESET_DRAFT_ORDER", "").lower() in ("1","true","yes"):
#    import_draft_order_from_pickorder(DRAFT_ORDER_CSV, reset=True)


# -------------------------
# Routes / API
# -------------------------

QUEUE_HTML = r"""
<!doctype html>
<html>
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Draft Queue</title>
  <style>
    body { font-family: system-ui, -apple-system, Segoe UI, Roboto, Helvetica, Arial, sans-serif; margin: 24px; }
    a { color: #184a7d; text-decoration: none; }
    .pill { padding: 6px 10px; border-radius: 999px; background: #f2f2f2; display: inline-flex; gap:8px; align-items:center; }
    .btn { padding: 6px 10px; border: 1px solid #333; background: #fff; border-radius: 6px; cursor: pointer; }
    .btn[disabled]{opacity:.5; cursor:not-allowed;}
    .row { display:flex; gap:16px; align-items:center; margin:12px 0; flex-wrap:wrap; }
    .muted { color:#666; }
    table { border-collapse: collapse; width: 100%; margin-top: 12px; }
    th, td { border-bottom: 1px solid #e5e5e5; padding: 8px 10px; text-align: left; }
    th { background: #fafafa; position: sticky; top: 0; z-index: 1; }
    .controls { display:flex; gap:6px; }
    .owned { opacity: 0.45; }
  </style>
</head>
<body>
  <div class="row">
    <a href="/draft/">← Back to Player Draft</a>
    <span class="pill">Draft Queue</span>
    <span id="team-pill" class="pill"></span>
  </div>

  <div class="row">
    <label class="pill" style="background:#fff;">
      <input type="radio" name="mode" id="mode-start"> Use queue at start of clock
    </label>
    <label class="pill" style="background:#fff;">
      <input type="radio" name="mode" id="mode-end"> Use queue at end of clock (default)
    </label>
    <button id="save-mode" class="btn">Save Mode</button>
    <span class="muted">Queue order is top → bottom.</span>
  </div>

  <table>
    <thead>
      <tr>
        <th style="width:6%;">#</th>
        <th style="width:7%;">MLBAMID</th>
        <th style="width:18%;">Name</th>
        <th style="width:6%;">Bats</th>
        <th style="width:6%;">Throws</th>
        <th style="width:7%;">Pos</th>
        <th style="width:12%;">DOB</th>
        <th style="width:14%;">MLB Org</th>
        <th style="width:6%;">FG 30</th>
        <th style="width:6%;">FG FV</th>
        <th style="width:6%;">MLB 30</th>
        <th style="width:6%;">MLB FV</th>
        <th style="width:6%;">FG100</th>
        <th style="width:6%;">MLB100</th>
        <th style="width:10%;">Actions</th>
      </tr>
    </thead>
    <tbody id="queue-body"></tbody>
  </table>

  <script>
    const tbody = document.getElementById('queue-body');
    const saveModeBtn = document.getElementById('save-mode');
    const modeStart = document.getElementById('mode-start');
    const modeEnd = document.getElementById('mode-end');
    const teamPill = document.getElementById('team-pill');

    let queue = [];
    let team = "";
    let useStart = false;

    async function load() {
      const res = await fetch('/draft/api/queue');
      if (!res.ok) {
        if (res.status === 401) {
          alert('Please login from the main page first.');
          location.href = '/draft/';
          return;
        }
        const msg = await res.text();
        alert('Failed to load queue: ' + msg);
        return;
      }
      const data = await res.json();
      team = data.team;
      useStart = !!data.use_at_start;
      queue = data.items || [];
      render();
    }

    function show(x){ return (x === null || x === undefined) ? '' : x; }

    function dobText(p){
      if (p.dob && p.dob.length) return p.dob;
      if (p.dob_year && p.dob_month && p.dob_day) {
        const y = String(p.dob_year).padStart(4,'0');
        const m = String(p.dob_month).padStart(2,'0');
        const d = String(p.dob_day).padStart(2,'0');
        return `${y}-${m}-${d}`;
      }
      return '';
    }

    function render() {
      teamPill.textContent = team ? ('Team: ' + team) : '';
      modeStart.checked = useStart;
      modeEnd.checked = !useStart;

      tbody.innerHTML = '';
      queue.forEach((p, idx) => {
        const tr = document.createElement('tr');
        if (p.franchise) tr.classList.add('owned');

        // index / queue position
        const tdIdx = document.createElement('td');
        tdIdx.textContent = String(idx + 1);

        const tdMlbamid = document.createElement('td');
        tdMlbamid.textContent = show(p.mlbamid);

        const tdName = document.createElement('td');
        tdName.textContent = p.name || '';

        const tdBats = document.createElement('td');
        tdBats.textContent = p.bats || '';

        const tdThrows = document.createElement('td');
        tdThrows.textContent = p.throws || '';

        const tdPos = document.createElement('td');
        tdPos.textContent = p.position || '';

        const tdDob = document.createElement('td');
        tdDob.textContent = dobText(p);

        const tdOrg = document.createElement('td');
        tdOrg.textContent = p.mlb_org || '';

        const tdFg30 = document.createElement('td');
        tdFg30.textContent = show(p.fg_30);

        const tdFgFv = document.createElement('td');
        tdFgFv.textContent = show(p.fg_fv);

        const tdMlb30 = document.createElement('td');
        tdMlb30.textContent = show(p.mlb_30);

        const tdMlbFv = document.createElement('td');
        tdMlbFv.textContent = show(p.mlb_fv);

        const tdFg100 = document.createElement('td');
        tdFg100.textContent = show(p.fg100);

        const tdMlb100 = document.createElement('td');
        tdMlb100.textContent = show(p.mlb100);

        const tdAct = document.createElement('td');
        const ctrls = document.createElement('div');
        ctrls.className = 'controls';
        const up = document.createElement('button');
        up.className = 'btn';
        up.textContent = '↑';
        up.disabled = idx === 0;
        up.onclick = () => move(idx, -1);

        const down = document.createElement('button');
        down.className = 'btn';
        down.textContent = '↓';
        down.disabled = idx === queue.length - 1;
        down.onclick = () => move(idx, +1);

        const del = document.createElement('button');
        del.className = 'btn';
        del.textContent = 'Remove';
        del.onclick = () => remove(p.player_id);

        ctrls.appendChild(up);
        ctrls.appendChild(down);
        ctrls.appendChild(del);
        tdAct.appendChild(ctrls);

        tr.appendChild(tdIdx);
        tr.appendChild(tdMlbamid);
        tr.appendChild(tdName);
        tr.appendChild(tdBats);
        tr.appendChild(tdThrows);
        tr.appendChild(tdPos);
        tr.appendChild(tdDob);
        tr.appendChild(tdOrg);
        tr.appendChild(tdFg30);
        tr.appendChild(tdFgFv);
        tr.appendChild(tdMlb30);
        tr.appendChild(tdMlbFv);
        tr.appendChild(tdFg100);
        tr.appendChild(tdMlb100);
        tr.appendChild(tdAct);

        tbody.appendChild(tr);
      });
    }

    function move(i, delta) {
      const j = i + delta;
      if (j < 0 || j >= queue.length) return;
      [queue[i], queue[j]] = [queue[j], queue[i]];
      render();
      saveOrder();
    }

    async function saveOrder() {
      const order = queue.map(x => x.player_id);
      await fetch('/draft/api/queue/reorder', {
        method: 'POST',
        headers: {'Content-Type':'application/json'},
        body: JSON.stringify({ order })
      });
    }

    async function remove(pid) {
      const res = await fetch('/draft/api/queue/remove', {
        method: 'POST',
        headers: {'Content-Type':'application/json'},
        body: JSON.stringify({ player_id: pid })
      });
      if (res.ok) {
        queue = queue.filter(x => x.player_id !== pid);
        render();
      }
    }

    saveModeBtn.onclick = async () => {
      useStart = modeStart.checked;
      await fetch('/draft/api/queue/mode', {
        method: 'POST',
        headers: {'Content-Type':'application/json'},
        body: JSON.stringify({ use_at_start: useStart })
      });
      alert('Queue mode saved.');
    };

    load();
  </script>
</body>
</html>
"""



@draft_bp.route("/queue")
def queue_page():
    # require login
    if not session.get("authed_team"):
        return redirect(url_for("index"))
    return render_template_string(QUEUE_HTML)


INDEX_HTML = r"""
<!doctype html>
<html>
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>League Draft</title>
  <style>
    th.sortable { cursor: pointer; user-select: none; }
    th.sortable .sort-ind { opacity: 0.6; margin-left: 4px; font-size: 12px; }
    body { font-family: system-ui, -apple-system, Segoe UI, Roboto, Helvetica, Arial, sans-serif; margin: 24px; }
    .topbar { display: flex; flex-wrap: wrap; gap: 12px; align-items: center; margin-bottom: 12px; }
    .pill { padding: 6px 10px; border-radius: 999px; background: #f2f2f2; display: inline-flex; gap: 8px; align-items: center; }
    .owned { opacity: 0.45; }
    table { border-collapse: collapse; width: 100%; }
    th, td { border-bottom: 1px solid #e5e5e5; padding: 8px 10px; text-align: left; }
    th { background: #fafafa; position: sticky; top: 0; z-index: 1; }
    .btn { padding: 6px 10px; border: 1px solid #333; background: #fff; border-radius: 6px; cursor: pointer; }
    .btn[disabled] { opacity: 0.5; cursor: not-allowed; }
    .muted { color: #666; }
    .green { color: #0a7a0a; font-weight: 600; }
    .danger { color: #b00020; }
    .flex { display: flex; align-items: center; gap: 8px; }
    .right { margin-left: auto; }
    .badge { font-size: 12px; background: #eef7ff; color: #184a7d; border: 1px solid #cfe5ff; padding: 2px 6px; border-radius: 4px; }
    .kbd { font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", "Courier New", monospace; border:1px solid #ccc; padding:2px 4px; border-radius:4px; background:#f9f9f9; }
    .row-hover:hover { background: #fcfcfc; }
  </style>
</head>
  <div class="pill" style="display:inline-block; margin-bottom:12px;">
    <a href="/draft/order" style="text-decoration:none; color:#184a7d;">View Draft Order & Times →</a>
  </div>
  <p class="muted" style="margin-top:16px;">
    Tip: Use <span class="kbd">/</span> to focus search. Owned players are grayed out. Draft button appears only when your selected team is on the clock and the player is eligible.
  </p>
<body>
  <h1>League Draft</h1>
  <div id="status" class="pill">
    <span>Current Pick:</span>
    <span id="current-pick">Loading…</span>
    <span id="picks-progress" class="badge"></span>
  </div>
<div class="topbar">
  <label class="pill">Your Team:
    <select id="team-select" style="margin-left:8px;"></select>
  </label>

  <button id="login-btn" class="btn" style="margin-left:4px;">Login</button>
  <a id="queue-link" class="btn" href="/draft/queue" style="margin-left:4px; display:none;">View Draft Queue</a>

  <label class="pill">
    <input type="checkbox" id="hide-owned" /> Hide owned players
  </label>
  <div class="pill right">
    <span>Search:</span>
    <input id="search" type="text" placeholder="Type a player name…" style="border:1px solid #ddd; padding:6px 8px; border-radius:6px; min-width: 260px;" />
    <span class="muted">(substring match)</span>
  </div>
</div>

  <div class="pill" id="login-pill" style="margin-top:8px;">
    <span id="login-status">🔒 Not logged in</span>
  </div>


  <table>
<thead>
  <tr>
    <th class="sortable" data-col="mlbamid"   style="width:7%;">MLBAMID</th>
    <th class="sortable" data-col="name"      style="width:18%;">Name</th>
    <th class="sortable" data-col="bats"      style="width:6%;">Bats</th>
    <th class="sortable" data-col="throws"    style="width:6%;">Throws</th>
    <th class="sortable" data-col="position"  style="width:7%;">Pos</th>
    <th class="sortable" data-col="dob"       style="width:12%;">DOB</th>
    <th class="sortable" data-col="mlb_org"   style="width:14%;">MLB Org</th>
    <th class="sortable" data-col="fg_30"     style="width:6%;">FG 30</th>
    <th class="sortable" data-col="fg_fv"     style="width:6%;">FG FV</th>
    <th class="sortable" data-col="mlb_30"    style="width:6%;">MLB 30</th>
    <th class="sortable" data-col="mlb_fv"    style="width:6%;">MLB FV</th>
    <th class="sortable" data-col="fg100"     style="width:6%;">FG100</th>
    <th class="sortable" data-col="mlb100"    style="width:6%;">MLB100</th>
    <th class=""           style="width:9%;">Action</th>
  </tr>
</thead>

<tbody id="players-body"></tbody>
  </table>



<script>
const tableHead = document.querySelector('thead');
const playersBody = document.getElementById('players-body');
const searchInput = document.getElementById('search');
const hideOwned = document.getElementById('hide-owned');
const teamSelect = document.getElementById('team-select');
const loginBtn = document.getElementById('login-btn');   // NEW
const currentPickSpan = document.getElementById('current-pick');
const picksProgress = document.getElementById('picks-progress');
const queueLink = document.getElementById('queue-link');


let state = {
  search: '',
  hideOwned: false,
  myTeam: null,
  currentPick: null,
  authedForSelected: false,
  authedEmail: '',
  sort: { key: null, dir: 'asc' }
};

function setHeaderIndicators() {
  // remove old indicators
  document.querySelectorAll('th.sortable').forEach(th => {
    const span = th.querySelector('.sort-ind');
    if (span) span.remove();
  });
  // add for active
  if (!state.sort.key) return;
  const th = document.querySelector(`th.sortable[data-col="${state.sort.key}"]`);
  if (!th) return;
  const mark = document.createElement('span');
  mark.className = 'sort-ind';
  mark.textContent = state.sort.dir === 'asc' ? '▲' : '▼';
  th.appendChild(mark);
}

tableHead.addEventListener('click', (e) => {
  const th = e.target.closest('th.sortable');
  if (!th) return;
  const key = th.getAttribute('data-col');
  if (state.sort.key === key) {
    state.sort.dir = (state.sort.dir === 'asc') ? 'desc' : 'asc';
  } else {
    state.sort.key = key;
    state.sort.dir = 'asc';
  }
  // re-fetch (keeps results current) then render with sorting
  fetchPlayers();
  setHeaderIndicators();
});



function focusSearchSlashShortcut(e){
  if (e.key === '/') { e.preventDefault(); searchInput.focus(); }
}
document.addEventListener('keydown', focusSearchSlashShortcut);

function setSelectOptions(teams) {
  teamSelect.innerHTML = '<option value="">— Select Team —</option>' + teams.map(t => `<option value="${t}">${t}</option>`).join('');
}

function updateLoginButtonState() {
  const t = teamSelect.value || '';
  loginBtn.disabled = !t;
}


async function fetchDraftStatus() {
  try {
    const res = await fetch('/draft/api/draft_status');
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    const data = await res.json();

    if (data.teams) setSelectOptions(data.teams);
    if (data.selected_team) teamSelect.value = data.selected_team;
    updateLoginButtonState();

    state.myTeam = data.selected_team || null;
    state.currentPick = data.current || null;
    state.authedForSelected = !!data.authed_for_selected;
    state.authedEmail = data.authed_email || "";
    queueLink.style.display = (state.authedForSelected ? 'inline-block' : 'none');

    const loginStatus = document.getElementById('login-status');
    if (state.authedForSelected && state.myTeam) {
      loginStatus.textContent = `🔓 Logged in as ${state.authedEmail} for ${state.myTeam}`;
    } else {
      loginStatus.textContent = `🔒 Not logged in`;
    }

    if (!data.current) {
      currentPickSpan.textContent = 'Draft complete';
      picksProgress.textContent = `${data.picks_made}/${data.total_picks}`;
    } else {
      currentPickSpan.textContent = `Round ${data.current.round}, Pick ${data.current.pick} — ${data.current.team}`;
      picksProgress.textContent = `${data.picks_made}/${data.total_picks}`;
    }
  } catch (err) {
    console.error('fetchDraftStatus failed:', err);
    // Keep the UI usable even if status fails
    state.currentPick = null;
    state.authedForSelected = false;
    state.authedEmail = "";
    const loginStatus = document.getElementById('login-status');
    loginStatus.textContent = `🔒 Not logged in`;
    currentPickSpan.textContent = 'Status unavailable';
    picksProgress.textContent = `—`;
  }
}


async function fetchPlayers() {
  const params = new URLSearchParams({
    search: state.search,
    hide_owned: state.hideOwned ? '1' : '0',
  });
  const res = await fetch('/draft/api/players?' + params.toString());
  const data = await res.json();
  renderPlayers(data.players);
}


function normalizeStr(x) { return (x ?? '').toString().toLowerCase(); }
function asNumber(x) {
  if (x === null || x === undefined) return null;

  if (typeof x === 'string') {
    const s = x.trim();
    if (s === '') return null;                  // ← key change: empty string stays empty
    const n = Number(s);
    return Number.isFinite(n) ? n : null;
  }

  if (typeof x === 'number') {
    return Number.isFinite(x) ? x : null;
  }

  return null;
}


function asDate(x) { // expects YYYY-MM-DD or empty
  if (!x) return null;
  const t = Date.parse(x);
  return Number.isFinite(t) ? t : null;
}

const SORTERS = {
  mlbamid:  (p) => asNumber(p.mlbamid),
  name:     (p) => normalizeStr(p.name),
  bats:     (p) => normalizeStr(p.bats),
  throws:   (p) => normalizeStr(p.throws),
  position: (p) => normalizeStr(p.position),
  dob:      (p) => asDate(p.dob) ?? asDate(
              (p.dob_year && p.dob_month && p.dob_day)
                ? `${String(p.dob_year).padStart(4,'0')}-${String(p.dob_month).padStart(2,'0')}-${String(p.dob_day).padStart(2,'0')}`
                : ''
            ),
  mlb_org:  (p) => normalizeStr(p.mlb_org),
  franchise:(p) => normalizeStr(p.franchise),
  fg_30:    (p) => asNumber(p.fg_30),
  fg_fv:    (p) => asNumber(p.fg_fv),
  mlb_30:   (p) => asNumber(p.mlb_30),
  mlb_fv:   (p) => asNumber(p.mlb_fv),
  fg100:    (p) => asNumber(p.fg100),
  mlb100:   (p) => asNumber(p.mlb100),
};


function isEmptyVal(v) {
  // treat null/undefined/''/whitespace/NaN as empty
  return (
    v === null ||
    v === undefined ||
    (typeof v === 'number' && !Number.isFinite(v)) ||
    (typeof v === 'string' && v.trim() === '')
  );
}

function applySort(arr) {
  const key = state.sort.key;
  if (!key || !SORTERS[key]) return arr;

  const dir = state.sort.dir === 'desc' ? -1 : 1;
  const getter = SORTERS[key];

  return [...arr].sort((p1, p2) => {
    const a = getter(p1);
    const b = getter(p2);

    const aEmpty = isEmptyVal(a);
    const bEmpty = isEmptyVal(b);

    // Always push empties to the bottom (for both asc & desc)
    if (aEmpty && !bEmpty) return 1;
    if (!aEmpty && bEmpty) return -1;
    if (aEmpty && bEmpty)  return 0; // keep relative order of blanks

    // Both non-empty → compare properly
    if (typeof a === 'number' && typeof b === 'number') {
      const cmp = a - b;
      return dir * (cmp === 0 ? 0 : (cmp < 0 ? -1 : 1));
    }

    // Strings / mixed: case-insensitive, numeric-aware
    const cmp = String(a).localeCompare(String(b), undefined, {
      numeric: true,
      sensitivity: 'base'
    });
    return dir * (cmp === 0 ? 0 : (cmp < 0 ? -1 : 1));
  });
}



function renderPlayers(players) {
  playersBody.innerHTML = '';
  players = applySort(players);

  const canDraftNow = state.currentPick 
    && state.myTeam 
    && state.currentPick.team === state.myTeam
    && state.authedForSelected; // must be logged in for that team




  for (const p of players) {
    const tr = document.createElement('tr');
    tr.className = 'row-hover' + (p.franchise ? ' owned' : '');

    const eligible = !!p.eligible;
    const alreadyOwned = !!p.franchise;

    const actionCell = document.createElement('td');
    if (canDraftNow && !alreadyOwned && eligible) {
      const btn = document.createElement('button');
      btn.className = 'btn';
      btn.textContent = 'Draft';
      btn.onclick = async () => {
        const team = state.myTeam || "your team";
        const ok = window.confirm(`Are you sure you want to draft ${p.name} for ${team}?`);
        if (!ok) return;
        btn.disabled = true;
        try {
          const resp = await fetch('/draft/api/draft', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ player_id: p.id })
          });
          if (resp.ok) {
            await fetchDraftStatus();
            await fetchPlayers();
          } else {
            const msg = await resp.text();
            alert('Draft failed: ' + msg);
            btn.disabled = false;
          }
        } catch (e) {
          alert('Network error while drafting. Please try again.');
          btn.disabled = false;
        }
      };
      actionCell.appendChild(btn);

    } else if (!alreadyOwned && eligible && state.authedForSelected) {
      // Not on the clock but logged in → queue controls
      if (p.in_queue) {
        actionCell.innerHTML = '<span class="muted">Queued</span>';
      } else {
        const qbtn = document.createElement('button');
        qbtn.className = 'btn';
        qbtn.textContent = 'Add to queue';
        qbtn.onclick = async () => {
          qbtn.disabled = true;
          const resp = await fetch('/draft/api/queue/add', {
            method: 'POST',
            headers: { 'Content-Type':'application/json' },
            body: JSON.stringify({ player_id: p.id })
          });
          if (!resp.ok) {
            const msg = await resp.text();
            alert('Could not add to queue: ' + msg);
          }
          await fetchPlayers(); // refresh "Queued" badges
        };
        actionCell.appendChild(qbtn);
      }

    } else if (alreadyOwned) {
      actionCell.innerHTML = '<span class="muted">Owned</span>';
    } else if (!eligible) {
      actionCell.innerHTML = '<span class="danger">Ineligible</span>';
    } else {
      actionCell.innerHTML = '<span class="muted">—</span>';
    }

const dobText = (p.dob && p.dob.length) ? p.dob :
  ((p.dob_year && p.dob_month && p.dob_day)
    ? `${String(p.dob_year).padStart(4,'0')}-${String(p.dob_month).padStart(2,'0')}-${String(p.dob_day).padStart(2,'0')}`
    : '');

function show(x){ return (x === null || x === undefined) ? '' : x; }

tr.innerHTML = `
  <td>${p.mlbamid ?? ''}</td>
  <td>${p.name || ''}</td>
  <td>${p.bats || ''}</td>
  <td>${p.throws || ''}</td>
  <td>${p.position || ''}</td>
  <td>${dobText}</td>
  <td>${p.mlb_org || ''}</td>
  <td>${show(p.fg_30)}</td>
  <td>${show(p.fg_fv)}</td>
  <td>${show(p.mlb_30)}</td>
  <td>${show(p.mlb_fv)}</td>
  <td>${show(p.fg100)}</td>
  <td>${show(p.mlb100)}</td>
`;


    tr.appendChild(actionCell);
    playersBody.appendChild(tr);
  }
}

// Debounce helper
function debounce(fn, ms) {
  let t; return function(...args){ clearTimeout(t); t = setTimeout(() => fn.apply(this, args), ms); };
}

const debouncedFetch = debounce(() => { state.search = searchInput.value; fetchPlayers(); }, 120);
searchInput.addEventListener('input', debouncedFetch);

hideOwned.addEventListener('change', () => { state.hideOwned = hideOwned.checked; fetchPlayers(); });

teamSelect.addEventListener('change', async () => {
  const t = teamSelect.value || '';
  // Always tell backend which team is selected; this clears any previous auth if different
  await fetch('/draft/api/select_team', { 
    method: 'POST', 
    headers: {'Content-Type':'application/json'}, 
    body: JSON.stringify({ team: t }) 
  });

  updateLoginButtonState();
  await fetchDraftStatus();
  await fetchPlayers();
});

loginBtn.addEventListener('click', async () => {
  const t = teamSelect.value || '';
  if (!t) {
    alert('Please select a team first.');
    return;
  }
  const email = window.prompt(`Enter the manager email for ${t} to unlock drafting:`);
  if (!email || !email.trim()) return;

  const resp = await fetch('/draft/api/login_team', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ team: t, email: email.trim() })
  });
  if (!resp.ok) {
    const msg = await resp.text();
    alert('Login failed: ' + msg);
  }
  await fetchDraftStatus();
  await fetchPlayers();
});

async function boot() {
  await fetchDraftStatus().catch(() => {});
  updateLoginButtonState();
  await fetchPlayers().catch((e) => console.error('fetchPlayers failed:', e));
  setHeaderIndicators();
}



boot();
</script>
</body>
</html>
"""


@draft_bp.route("/")
def index():
    return render_template_string(INDEX_HTML)

def _require_authed_team():
    team = session.get("authed_team")
    if not team:
        abort(401, "Not logged in")
    return team

@draft_bp.get("/api/queue")
def api_queue_get():
    team = _require_authed_team()
    rows = get_team_queue(team)
    items = []
    for r in rows:
        items.append({
            "player_id":   int(r["player_id"]),
            "qpos":        int(r["qpos"] or 0),
            "name":        r["name"],
            "dob":         r["dob"],
            "position":    r["position"],
            "franchise":   r["franchise"],
            "eligible":    int(r["eligible"] or 1),
            "mlbamid":     r["mlbamid"],
            "bats":        r["bats"],
            "throws":      r["throws"],
            "dob_month":   r["dob_month"],
            "dob_day":     r["dob_day"],
            "dob_year":    r["dob_year"],
            "mlb_org":     r["mlb_org"],
            "fg_30":       r["fg_30"],
            "fg_fv":       r["fg_fv"],
            "mlb_30":      r["mlb_30"],
            "mlb_fv":      r["mlb_fv"],
            "fg100":       r["fg100"],
            "mlb100":      r["mlb100"],
        })
    return jsonify({
        "team": team,
        "use_at_start": get_queue_mode(team),
        "items": items,
    })



@draft_bp.post("/api/queue/add")
def api_queue_add():
    team = _require_authed_team()
    data = request.get_json(force=True, silent=True) or {}
    pid = int(data.get("player_id") or 0)
    if pid <= 0:
        return ("missing player_id", 400)

    conn = get_conn()
    cur = conn.cursor()
    # Already owned or ineligible? block adding
    cur.execute("SELECT franchise, COALESCE(eligible,1) FROM players WHERE id=?", (pid,))
    pr = cur.fetchone()
    if not pr:
        conn.close(); return ("player not found", 404)
    if pr[0] or int(pr[1]) != 1:
        conn.close(); return ("player not addable", 409)

    # Next position = max + 1
    cur.execute("SELECT COALESCE(MAX(position), 0) FROM draft_queue WHERE team=?", (team,))
    next_pos = int(cur.fetchone()[0]) + 1

    cur.execute("""
        INSERT OR IGNORE INTO draft_queue(team, player_id, position, created_at)
        VALUES(?,?,?,?)
    """, (team, pid, next_pos, datetime.utcnow().isoformat(timespec='seconds')))
    conn.commit()
    conn.close()
    return ("", 204)

@draft_bp.post("/api/queue/remove")
def api_queue_remove():
    team = _require_authed_team()
    data = request.get_json(force=True, silent=True) or {}
    pid = int(data.get("player_id") or 0)
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("DELETE FROM draft_queue WHERE team=? AND player_id=?", (team, pid))
    conn.commit()
    conn.close()
    return ("", 204)

@draft_bp.post("/api/queue/reorder")
def api_queue_reorder():
    team = _require_authed_team()
    data = request.get_json(force=True, silent=True) or {}
    order = data.get("order") or []
    if not isinstance(order, list) or not all(isinstance(x, int) for x in order):
        return ("invalid order", 400)

    conn = get_conn()
    cur = conn.cursor()
    # ensure only items from this team get reordered
    for idx, pid in enumerate(order, start=1):
        cur.execute("UPDATE draft_queue SET position=? WHERE team=? AND player_id=?", (idx, team, pid))
    conn.commit()
    conn.close()
    return ("", 204)

@draft_bp.post("/api/queue/mode")
def api_queue_mode():
    team = _require_authed_team()
    data = request.get_json(force=True, silent=True) or {}
    use_at_start = bool(data.get("use_at_start"))
    set_queue_mode(team, use_at_start)
    return ("", 204)



@draft_bp.get("/api/players")
def api_players():
    search = (request.args.get("search") or "").strip().lower()
    hide_owned = request.args.get("hide_owned") == "1"

    conn = get_conn()
    cur = conn.cursor()
    authed_team = session.get("authed_team", "")
    in_queue = set()
    if authed_team:
        cur.execute("SELECT player_id FROM draft_queue WHERE team=?", (authed_team,))
        in_queue = {int(r[0]) for r in cur.fetchall()}

    cols = ("id, name, dob, position, franchise, eligible, "
        "mlbamid, bats, throws, dob_month, dob_day, dob_year, mlb_org, "
        "fg_30, fg_fv, mlb_30, mlb_fv, fg100, mlb100")

    q = f"SELECT {cols} FROM players"
    params: List[Any] = []

    clauses = []
    if search:
        # accent-insensitive search across name/first/last
        clauses.append("(LOWER(unaccent(name)) LIKE ? OR LOWER(unaccent(first)) LIKE ? OR LOWER(unaccent(last)) LIKE ?)")
        s = "".join(ch for ch in unicodedata.normalize("NFKD", search) if not unicodedata.combining(ch))
        params += [f"%{s.lower()}%", f"%{s.lower()}%", f"%{s.lower()}%"]

    if hide_owned:
        clauses.append("(franchise IS NULL OR franchise = '')")

    if clauses:
        q += " WHERE " + " AND ".join(clauses)
    q += " ORDER BY unaccent(name) COLLATE NOCASE ASC"

    cur.execute(q, params)
    rows = cur.fetchall()
    conn.close()

    players = []
    for r in rows:
        players.append({
            "id": r["id"],
            "name": r["name"],
            "dob": r["dob"],
            "position": r["position"],
            "franchise": r["franchise"],
            "eligible": int(r["eligible"] or 0),
            "mlbamid": r["mlbamid"],
            "bats": r["bats"],
            "throws": r["throws"],
            "dob_month": r["dob_month"],
            "dob_day": r["dob_day"],
            "dob_year": r["dob_year"],
            "mlb_org": r["mlb_org"],
            "fg_30":  r["fg_30"],
            "fg_fv":  r["fg_fv"],
            "mlb_30": r["mlb_30"],
            "mlb_fv": r["mlb_fv"],
            "fg100":  r["fg100"],
            "mlb100": r["mlb100"],
            "in_queue": (r["id"] in in_queue),
        })
    return jsonify({"players": players})

@draft_bp.get("/api/debug/schedule_snapshot")
def debug_schedule_snapshot():
    from draft_order_page import _load_picks_overrides_and_designated, _compute_scheduled_times, EASTERN
    now = datetime.now(tz=EASTERN)
    picks, designated = _load_picks_overrides_and_designated()
    scheduled = _compute_scheduled_times(now)
    out = []
    for i, r in enumerate(picks):
        if r["player_id"]:
            continue
        out.append({
            "id": r["id"],
            "label": r["label"] or f"{r['round']}.{r['pick']:02d}",
            "team": r["team"],
            "designated": designated[i].isoformat(),
            "scheduled": scheduled.get(i, designated[i]).isoformat(),
        })
    return jsonify({"now": now.isoformat(), "undrafted": out})


@draft_bp.post("/tasks/enforce_queue")
def task_enforce_queue():
    try:
        enforce_queue_actions()
        return ("", 204)
    except Exception as e:
        return (f"enforce failed: {e}", 500)


@draft_bp.get("/api/draft_status")
def api_draft_status():
    try:
        # Opportunistic queue enforcement on status poll
        try:
            enforce_queue_actions()
        except Exception as _e:
            pass

        cur_pick = get_current_pick()

        # totals
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM draft_order WHERE player_id IS NOT NULL")
        made = int(cur.fetchone()[0] or 0)
        cur.execute("SELECT COUNT(*) FROM draft_order")
        total = int(cur.fetchone()[0] or 0)
        conn.close()

        selected_team = session.get("selected_team", "") or ""
        authed_team = session.get("authed_team", "") or ""
        authed_email = session.get("authed_email", "") or ""

        # Ensure cur_pick is either None or a plain dict (already is),
        # but *never* something non-serializable.
        payload = {
            "current": cur_pick if cur_pick else None,
            "selected_team": selected_team,
            "teams": MLB_TEAMS,
            "picks_made": made,
            "total_picks": total,
            "authed_team": authed_team,
            "authed_email": authed_email,
            "authed_for_selected": bool(selected_team) and (authed_team == selected_team),
        }
        return jsonify(payload)
    except Exception as e:
        # Never break the UI: return a safe default payload the frontend can handle.
        current_app.logger.exception("api/draft_status failed: %s", e)
        return jsonify({
            "current": None,
            "selected_team": session.get("selected_team", "") or "",
            "teams": MLB_TEAMS,
            "picks_made": 0,
            "total_picks": 0,
            "authed_team": "",
            "authed_email": "",
            "authed_for_selected": False,
        }), 200

@draft_bp.post("/tasks/test_discord")
def test_discord():
    url = os.environ.get("DISCORD_WEBHOOK_URL")
    if not url:
        return jsonify({"ok": False, "error": "DISCORD_WEBHOOK_URL not set in this process"}), 500
    try:
        _discord_post("Test from BNSL Draft webhook")
        return jsonify({"ok": True}), 200
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@draft_bp.post("/api/login_team")
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
        # also set selected team if not set
        if session.get("selected_team") != team:
            session["selected_team"] = team
        return jsonify({"ok": True}), 200
    else:
        return ("Invalid email for this team", 401)


@draft_bp.post("/api/select_team")
def api_select_team():
    data = request.get_json(force=True, silent=True) or {}
    team = (data.get("team") or "").strip()
    if team and team not in MLB_TEAMS:
        return ("Unknown team", 400)
    # selecting a new team clears any previous auth
    session["selected_team"] = team
    if session.get("authed_team") != team:
        session.pop("authed_team", None)
        session.pop("authed_email", None)
    return ("", 204)

@draft_bp.post("/api/draft")
def api_draft():
    data = request.get_json(force=True, silent=True) or {}
    player_id = data.get("player_id")
    if not isinstance(player_id, int):
        return ("Missing or invalid player_id", 400)

    # Load current pick
    current = get_current_pick()
    if current is None:
        return ("Draft is complete", 400)

    my_team = session.get("selected_team")
    if session.get("authed_team") != my_team:
        return ("Not logged in for this team", 401)
    if not my_team:
        return ("Select your team first", 400)
    if my_team != current["team"]:
        return ("Not your pick", 403)

    conn = get_conn()
    cur = conn.cursor()

    # Validate player availability & eligibility
    cur.execute("SELECT id, name, franchise, eligible FROM players WHERE id=?", (player_id,))
    prow = cur.fetchone()
    if not prow:
        conn.close()
        return ("Player not found", 404)
    if prow[2]:
        conn.close()
        return ("Player already owned", 409)
    if int(prow[3]) != 1:
        conn.close()
        return ("Player is not draft-eligible", 409)

    # Assign player to franchise and mark draft pick
    try:
        cur.execute("UPDATE players SET franchise=? WHERE id=?", (my_team, player_id))
        cur.execute(
            "UPDATE draft_order SET player_id=?, drafted_at=? WHERE id=?",
            (player_id, datetime.utcnow().isoformat(timespec='seconds'), current["id"])
        )
        conn.commit()
        try:
            notify_discord_pick(int(current["id"]))   # <-- NEW
        except Exception as e:
            print(f"[discord] failed: {e}")
        try:
            notify_if_new_on_clock()
        except Exception as e:
            print(f"[notify] failed: {e}")
        remove_player_from_all_queues(player_id)

        try:
            enforce_queue_actions()
        except Exception as e:
            print(f"[queue] enforce after draft failed: {e}")

    except Exception as e:
        conn.rollback()
        conn.close()
        return (f"Failed to draft: {e}", 500)

    conn.close()
    return ("", 204)

@draft_bp.post("/tasks/scan_on_clock")
def scan_on_clock():
    try:
        notify_if_new_on_clock()
        return ("", 204)
    except Exception as e:
        return (f"scan failed: {e}", 500)

@draft_bp.get("/healthz")
def healthz():
    return {"ok": True}

if __name__ == "__main__":
    print("\n*** Flask Draft App ***")
    print("If players.csv and draft_order.csv are not present, sample files were generated.")
    print("To reset: delete draft.db and restart.\n")

    port = int(os.environ.get("PORT", "5000"))  # Render sets PORT
    app.run(host="0.0.0.0", port=port, debug=False)  # bind to all interfaces

