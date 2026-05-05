from __future__ import annotations
from pathlib import Path
from datetime import datetime
from typing import Any, Dict, List
import csv
import os
import logging
import sqlite3
import unicodedata
import re

from flask import (
    Blueprint, current_app, jsonify, render_template_string,
    request, session, abort
)

from ui_skin import BNSL_GAME_CSS

roster_bp = Blueprint("roster", __name__)

from team_config import TEAM_EMAILS_BY_ABBR as TEAM_EMAILS, TEAM_ABBRS, canonical_team_abbr, emails_equal

POSITIONS = ["P","C","1B","2B","3B","SS","LF","CF","RF","DH","IF","OF"]
CONTRACT_TYPES = ["R","A","X","FA"]
STATUS_TYPES = ["Active","40-man","Reserve"]
FA_CLASSES = ["2026","2027","2028","2029","2030"]
CURRENT_SEASON = 2025
CURRENT_FA_CLASS = str(CURRENT_SEASON + 1)
DRAFT_YEAR = 2025
DRAFT_ROOKIE_OPTIONS_REMAINING = 3

DRAFT_TEAM_ABBR = {
    "Arizona Diamondbacks": "ARI",
    "Atlanta Braves": "ATL",
    "Baltimore Orioles": "BAL",
    "Boston Red Sox": "BOS",
    "Chicago Cubs": "CHC",
    "Chicago White Sox": "CHW",
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
    "Washington Nationals": "WAS",
}


def as_int(val, default=None):
    """Convert CSV strings or SQLite numeric values to int safely."""
    if val is None:
        return default
    if isinstance(val, str):
        s = val.strip()
        if s == "":
            return default
    else:
        s = val
    try:
        return int(float(s))
    except Exception:
        return default


def as_float(val, default=0.0):
    """Convert CSV strings or SQLite numeric values to float safely."""
    if val is None:
        return default
    if isinstance(val, str):
        s = val.strip()
        if s == "":
            return default
    else:
        s = val
    try:
        return float(s)
    except Exception:
        return default


def as_bool(val) -> bool:
    return str(val).strip().lower() in ("true", "1", "yes", "y")


def row_value(row: Any, key: str, default: Any = None) -> Any:
    """Safely read either a sqlite Row or a dict-like CSV row."""
    try:
        if isinstance(row, sqlite3.Row):
            return row[key] if key in row.keys() else default
        return row.get(key, default)
    except Exception:
        return default


def year_from_value(val: Any) -> int | None:
    """Extract a leading year from values like 2025, "2025", or "2025-10-01"."""
    s = str(val or "").strip()
    if not s:
        return None
    try:
        return int(s[:4])
    except Exception:
        return None


def fa_class_from_contract_parts(
    contract_type: str,
    contract_expires: Any,
    contract_initial_season: Any,
    contract_length: Any,
    contract_option: Any,
    service_time: Any,
    previous_service_time: Any = None,
    service_time_2025: Any = None,
) -> str:
    """
    Compute the FA class displayed in the roster tab.

    Important OOTP/BNSL convention: for FA/X contracts, contract_expires is
    the final contract season, including any option year if present. Therefore
    every FA/X player reaches free agency in contract_expires + 1.
    Example: contract_expires=2025 means FA class 2026, option flag or not.
    """
    ctype = (contract_type or "").strip().upper()
    if ctype in ("R", "A", ""):
        # service_time is the total after the imported season. Fall back to
        # previous + current-season service if needed.
        total_service = as_float(service_time, None)
        if total_service is None:
            total_service = as_float(previous_service_time, 0.0) + as_float(service_time_2025, 0.0)

        # Arbitration players at 6.000+ years are current free agents.
        if ctype == "A" and total_service >= 6.0:
            return CURRENT_FA_CLASS

        for year in range(CURRENT_SEASON + 1, CURRENT_SEASON + 11):
            projected = total_service + (year - (CURRENT_SEASON + 1))
            if projected > 6.0:
                return str(year)
        return ""

    if ctype in ("X", "FA"):
        # For FA/X contracts, contract_expires is the authoritative final
        # contract season.  This includes option years once they are used.
        # Therefore a 2025 expiry is always a 2026 FA class, regardless of
        # contract_option or contract_initial_season/contract_length.
        exp_year = year_from_value(contract_expires)
        if exp_year is not None:
            return str(exp_year + 1)

        # Fallback only for rows missing contract_expires.  In the BNSL/OOTP
        # export, contract_length should then indicate years after the initial
        # season, so initial + length is the FA class.
        initial = as_int(contract_initial_season)
        length = as_int(contract_length)
        if initial is not None and length is not None:
            return str(initial + length)

    return ""


def csv_fa_class(r: Dict[str, Any]) -> str:
    return fa_class_from_contract_parts(
        r.get("contract_type"),
        r.get("contract_expires"),
        r.get("contract_initial_season"),
        r.get("contract_length"),
        r.get("contract_option"),
        r.get("service_time"),
        r.get("previous_service_time"),
        r.get("service_time_2025"),
    )


def is_current_free_agent_csv(r: Dict[str, Any]) -> bool:
    return csv_fa_class(r) == CURRENT_FA_CLASS



def get_conn():
    db_path = current_app.config["ROSTER_DB_PATH"]
    conn = sqlite3.connect(db_path)
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




def roster_status_from_csv(
    status: str,
    active_roster: Any = None,
    expanded_roster: Any = None,
) -> str:
    """
    Import the three editable roster buckets used by the roster tab.

    Active = on the active roster and salary counts for every contract type.
    40-man = on the 40-man/expanded roster but not active.
    Reserve = owned by the franchise but not on the 40-man roster.
    """
    if as_bool(active_roster):
        return "Active"
    if as_bool(expanded_roster):
        return "40-man"

    s = (status or "").strip().lower()
    if s == "active":
        return "Active"
    if s == "expanded":
        return "40-man"
    return "Reserve"


def bt_text(bats: str, throws: str) -> str:
    b = (bats or "").strip().upper() or "?"
    t = (throws or "").strip().upper() or "?"
    return f"{b}/{t}"


def service_to_display(x: Any) -> float:
    try:
        return round(float(x), 2)
    except Exception:
        return 0.0


def infer_two_digit_birth_year(yy: int) -> int:
    # Baseball DOBs with 00-26 are 2000-2026; everything else is 1900s.
    return 2000 + yy if yy <= 26 else 1900 + yy


def parse_roster_dob_parts(dob: Any) -> tuple[int, int | None, int | None] | None:
    """Parse YYYY-MM-DD, DD-MM-YY, or DD-MM-YYYY roster DOBs."""
    text = str(dob or "").strip()
    if not text:
        return None

    parts = [p for p in re.split(r"\D+", text) if p]
    if len(parts) >= 3:
        # YYYY-MM-DD / YYYY/MM/DD
        if len(parts[0]) == 4:
            return int(parts[0]), int(parts[1]), int(parts[2])

        # DD-MM-YYYY
        if len(parts[2]) == 4:
            return int(parts[2]), int(parts[1]), int(parts[0])

        # DD-MM-YY
        if len(parts[2]) == 2:
            return infer_two_digit_birth_year(int(parts[2])), int(parts[1]), int(parts[0])

    # Last fallback: any explicit 19xx/20xx year embedded in the string.
    m = re.search(r"(19\d{2}|20\d{2})", text)
    if m:
        return int(m.group(1)), None, None

    return None


def birth_year_from_roster_dob(dob: Any) -> int | None:
    parsed = parse_roster_dob_parts(dob)
    return parsed[0] if parsed else None


def normalize_roster_dob(dob: Any) -> str:
    parsed = parse_roster_dob_parts(dob)
    if not parsed:
        return str(dob or "").strip()
    year, month, day = parsed
    if month is None or day is None:
        return f"{year:04d}"
    if not (1 <= month <= 12 and 1 <= day <= 31):
        return str(dob or "").strip()
    return f"{year:04d}-{month:02d}-{day:02d}"


def normalize_roster_birthdates(conn: sqlite3.Connection) -> None:
    cur = conn.cursor()
    cur.execute("""
        SELECT id, date_of_birth
        FROM roster_players
        WHERE COALESCE(date_of_birth, '') != ''
    """)
    for row in cur.fetchall():
        old_dob = str(row["date_of_birth"] or "").strip()
        new_dob = normalize_roster_dob(old_dob)
        if new_dob and new_dob != old_dob:
            cur.execute(
                "UPDATE roster_players SET date_of_birth=? WHERE id=?",
                (new_dob, int(row["id"])),
            )


def rulev_eligible(
    status: str,
    dob: str,
    contract_type: str | None = None,
    franchise: str | None = None,
    signed: Any = None,
    draft_year: Any = None,
) -> bool:
    """
    Rule V eligibility used by the roster tab.

    Eligible players are rostered to an org, are not protected on the 40-man
    roster, and were born in or before 2001.  In the roster tab's three-bucket
    model, Reserve means not on the 40-man.

    Players drafted in the current 2025 BNSL draft are explicitly protected
    from Rule V eligibility regardless of age.

    Contract type and signed are intentionally ignored here.  Franchise/status
    are the authoritative rostered-state fields for Rule V.
    """
    if as_int(draft_year, None) == DRAFT_YEAR:
        return False
    if (status or "").strip() != "Reserve":
        return False
    if not (franchise or "").strip():
        return False

    year = birth_year_from_roster_dob(dob)
    return year is not None and year <= 2001


def compute_fa_class(row: sqlite3.Row) -> str:
    # Once a current FA has had its contract fields cleared, we still need to
    # remember which FA class it belongs to for the roster tab filter/display.
    stored_fa_class = row_value(row, "fa_class", "")
    if stored_fa_class:
        return str(stored_fa_class)

    return fa_class_from_contract_parts(
        row_value(row, "contract_type"),
        row_value(row, "contract_expires"),
        row_value(row, "contract_initial_season"),
        row_value(row, "contract_length"),
        row_value(row, "contract_option"),
        row_value(row, "service_time"),
        row_value(row, "previous_service_time"),
        row_value(row, "service_time_2025"),
    )





def now_utc_iso() -> str:
    return datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


def ensure_roster_decision_columns(conn: sqlite3.Connection) -> None:
    cur = conn.cursor()
    cur.execute("PRAGMA table_info(roster_players)")
    cols = {row[1] for row in cur.fetchall()}
    additions = {
        "option_decision": "ALTER TABLE roster_players ADD COLUMN option_decision TEXT",
        "option_decision_at": "ALTER TABLE roster_players ADD COLUMN option_decision_at TEXT",
        "arbitration_decision": "ALTER TABLE roster_players ADD COLUMN arbitration_decision TEXT",
        "arbitration_decision_at": "ALTER TABLE roster_players ADD COLUMN arbitration_decision_at TEXT",
    }
    for col, ddl in additions.items():
        if col not in cols:
            cur.execute(ddl)


def row_service_time(row: sqlite3.Row) -> float:
    return as_float(row_value(row, "service_time"), 0.0)


def normalize_roster_contract_types(conn: sqlite3.Connection) -> None:
    """Promote pre-arb R contracts to arbitration A contracts at 3.000+ service."""
    conn.execute("UPDATE roster_players SET franchise='WAS' WHERE franchise='WSH'")
    conn.execute("""
        UPDATE roster_players
        SET contract_type='A'
        WHERE UPPER(COALESCE(contract_type, ''))='R'
          AND COALESCE(service_time, 0) >= 3.0
          AND COALESCE(franchise, '') != ''
    """)


def pending_option_decision(row: sqlite3.Row) -> bool:
    return bool(row_value(row, "franchise", "")) and bool(int(row_value(row, "contract_option", 0) or 0))


def pending_arbitration_decision(row: sqlite3.Row) -> bool:
    decision = str(row_value(row, "arbitration_decision", "") or "").strip()
    return (
        bool(row_value(row, "franchise", ""))
        and str(row_value(row, "contract_type", "") or "").strip().upper() == "A"
        and row_service_time(row) >= 3.0
        and decision == ""
    )


def sync_after_roster_mutation() -> None:
    try:
        from rulev_app import sync_rulev_from_roster_db
        sync_rulev_from_roster_db()
    except Exception:
        current_app.logger.exception("Rule V sync failed after roster mutation")

    try:
        from fa_app import sync_free_agents_from_roster_db
        sync_free_agents_from_roster_db()
    except Exception:
        current_app.logger.exception("FA sync failed after roster mutation")


def set_player_free_agent(
    cur: sqlite3.Cursor,
    player_id: int,
    fa_class: str = CURRENT_FA_CLASS,
    option_decision: str | None = None,
    arbitration_decision: str | None = None,
) -> None:
    stamp = now_utc_iso()
    cur.execute("""
        UPDATE roster_players
        SET signed=0,
            contract_type='FA',
            salary=0,
            contract_initial_season=NULL,
            contract_length=NULL,
            contract_option=0,
            contract_expires='',
            franchise='',
            affiliate_team='',
            roster_status='',
            active_roster=0,
            fa_class=?,
            option_decision=COALESCE(?, option_decision),
            option_decision_at=CASE WHEN ? IS NOT NULL THEN ? ELSE option_decision_at END,
            arbitration_decision=COALESCE(?, arbitration_decision),
            arbitration_decision_at=CASE WHEN ? IS NOT NULL THEN ? ELSE arbitration_decision_at END
        WHERE id=?
    """, (
        fa_class,
        option_decision,
        option_decision,
        stamp,
        arbitration_decision,
        arbitration_decision,
        stamp,
        player_id,
    ))


def update_fa_last_team(player_id: int, team_abbr: str) -> None:
    if not team_abbr:
        return
    try:
        from fa_app import get_conn as get_fa_conn, roster_code_to_team
        fa_conn = get_fa_conn()
        fa_cur = fa_conn.cursor()
        fa_cur.execute(
            "UPDATE free_agents SET last_team=COALESCE(NULLIF(?, ''), last_team) WHERE roster_player_id=?",
            (roster_code_to_team(team_abbr), player_id),
        )
        fa_conn.commit()
        fa_conn.close()
    except Exception:
        current_app.logger.exception("Failed to update FA last_team after roster action")


def ensure_roster_draft_columns(conn: sqlite3.Connection) -> None:
    cur = conn.cursor()
    cur.execute("PRAGMA table_info(roster_players)")
    cols = {row[1] for row in cur.fetchall()}
    additions = {
        "draft_player_id": "ALTER TABLE roster_players ADD COLUMN draft_player_id INTEGER",
        "draft_year": "ALTER TABLE roster_players ADD COLUMN draft_year INTEGER",
        "draft_round": "ALTER TABLE roster_players ADD COLUMN draft_round INTEGER",
        "draft_pick": "ALTER TABLE roster_players ADD COLUMN draft_pick INTEGER",
        "draft_label": "ALTER TABLE roster_players ADD COLUMN draft_label TEXT",
        "drafted_at": "ALTER TABLE roster_players ADD COLUMN drafted_at TEXT",
        "draft_team": "ALTER TABLE roster_players ADD COLUMN draft_team TEXT",
    }
    for col, ddl in additions.items():
        if col not in cols:
            cur.execute(ddl)
    cur.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS roster_players_draft_player_id_uq
        ON roster_players(draft_player_id)
        WHERE draft_player_id IS NOT NULL
    """)


def get_draft_db_path() -> Path:
    configured = current_app.config.get("DRAFT_DB_PATH")
    if configured:
        return Path(configured)
    return Path(__file__).resolve().parent / "draft.db"


def draft_team_to_abbr(team: Any) -> str:
    text = str(team or "").strip()
    if not text:
        return ""
    if text in DRAFT_TEAM_ABBR:
        return DRAFT_TEAM_ABBR[text]
    return canonical_team_abbr(text)


def split_draft_name(name: str) -> tuple[str, str]:
    parts = [p for p in re.split(r"\s+", str(name or "").strip()) if p]
    if not parts:
        return "", ""
    if len(parts) == 1:
        return "", parts[0]
    return parts[0], parts[-1]


def draft_dob_from_row(row: sqlite3.Row) -> str:
    dob = str(row["dob"] or "").strip()
    if dob:
        return normalize_roster_dob(dob)
    y = as_int(row["dob_year"] if "dob_year" in row.keys() else None)
    m = as_int(row["dob_month"] if "dob_month" in row.keys() else None)
    d = as_int(row["dob_day"] if "dob_day" in row.keys() else None)
    if y and m and d:
        return f"{y:04d}-{m:02d}-{d:02d}"
    return ""


def normalized_identity_name(name: Any) -> str:
    text = "".join(
        ch for ch in unicodedata.normalize("NFKD", str(name or ""))
        if not unicodedata.combining(ch)
    ).lower()
    return re.sub(r"[^a-z0-9]", "", text)


def sync_drafted_players_from_draft_db(conn: sqlite3.Connection | None = None) -> tuple[int, int, int]:
    """
    Mirror completed 2025 draft picks into roster.db.

    Drafted players become signed players on rookie contracts, assigned to the
    drafting franchise's Reserve roster.  The sync is idempotent and stores the
    source draft player id so repeated startup syncs or manual draft picks do
    not create duplicates.
    """
    draft_db = get_draft_db_path()
    if not draft_db.exists():
        current_app.logger.info("Draft-to-roster sync skipped: draft DB does not exist: %s", draft_db)
        return (0, 0, 0)

    own_conn = conn is None
    if conn is None:
        conn = get_conn()
    ensure_roster_draft_columns(conn)
    cur = conn.cursor()

    dconn = sqlite3.connect(str(draft_db))
    dconn.row_factory = sqlite3.Row
    try:
        dcur = dconn.cursor()
        dcur.execute("""
            SELECT
                d.id AS draft_order_id,
                d.round AS draft_round,
                d.pick AS draft_pick,
                COALESCE(NULLIF(d.label, ''), printf('%d.%02d', d.round, d.pick)) AS draft_label,
                d.team AS draft_team,
                d.drafted_at,
                p.id AS draft_player_id,
                p.name,
                p.dob,
                p.position,
                p.mlbamid,
                p.first,
                p.last,
                p.bats,
                p.throws,
                p.dob_month,
                p.dob_day,
                p.dob_year
            FROM draft_order d
            JOIN players p ON p.id = d.player_id
            WHERE d.player_id IS NOT NULL
            ORDER BY d.round ASC, d.pick ASC, d.id ASC
        """)
        drafted_rows = dcur.fetchall()
    except sqlite3.Error:
        current_app.logger.exception("Draft-to-roster sync failed while reading %s", draft_db)
        dconn.close()
        if own_conn:
            conn.close()
        return (0, 0, 0)
    finally:
        try:
            dconn.close()
        except Exception:
            pass

    cur.execute("SELECT COALESCE(MAX(id), 0) FROM roster_players")
    next_id = int(cur.fetchone()[0] or 0) + 1

    seen = 0
    inserted = 0
    updated = 0
    processed_draft_ids: set[int] = set()

    for drow in drafted_rows:
        draft_player_id = as_int(drow["draft_player_id"])
        if not draft_player_id or draft_player_id in processed_draft_ids:
            continue
        processed_draft_ids.add(draft_player_id)

        team_abbr = draft_team_to_abbr(drow["draft_team"])
        if not team_abbr:
            continue

        name = str(drow["name"] or "").strip()
        if not name:
            continue

        seen += 1
        dob = draft_dob_from_row(drow)
        mlbam_id = as_int(drow["mlbamid"], None)
        first = str(drow["first"] or "").strip()
        last = str(drow["last"] or "").strip()
        if not first or not last:
            f2, l2 = split_draft_name(name)
            first = first or f2
            last = last or l2

        match_id = None
        cur.execute("SELECT id FROM roster_players WHERE draft_player_id=?", (draft_player_id,))
        match = cur.fetchone()
        if match:
            match_id = int(match["id"] if isinstance(match, sqlite3.Row) else match[0])

        if match_id is None and mlbam_id:
            cur.execute("SELECT id FROM roster_players WHERE mlbam_id=?", (mlbam_id,))
            match = cur.fetchone()
            if match:
                match_id = int(match["id"] if isinstance(match, sqlite3.Row) else match[0])

        if match_id is None and dob:
            cur.execute("SELECT id, name FROM roster_players WHERE date_of_birth=?", (dob,))
            target_name = normalized_identity_name(name)
            for candidate in cur.fetchall():
                if normalized_identity_name(candidate["name"]) == target_name:
                    match_id = int(candidate["id"])
                    break

        values = (
            1, "R", 0.0, None, None, 0, "",
            0.0, 0.0, 0.0,
            team_abbr, "", "Reserve", 0, DRAFT_ROOKIE_OPTIONS_REMAINING, "",
            draft_player_id, DRAFT_YEAR,
            as_int(drow["draft_round"], None),
            as_int(drow["draft_pick"], None),
            str(drow["draft_label"] or ""),
            str(drow["drafted_at"] or ""),
            team_abbr,
        )

        if match_id is None:
            insert_id = next_id
            next_id += 1
            cur.execute("""
                INSERT INTO roster_players (
                    id, name, last_name, first_name, suffix, nickname,
                    position, date_of_birth, bats, throws, signed,
                    contract_type, salary, contract_initial_season,
                    contract_length, contract_option, contract_expires,
                    service_time, previous_service_time, service_time_2025,
                    franchise, affiliate_team, roster_status, active_roster,
                    options_remaining, fa_class, fangraphs_id, mlbam_id,
                    draft_player_id, draft_year, draft_round, draft_pick,
                    draft_label, drafted_at, draft_team
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """, (
                insert_id, name, last, first, "", "",
                str(drow["position"] or "").strip(), dob,
                str(drow["bats"] or "").strip(), str(drow["throws"] or "").strip(),
                1, "R", 0.0, None, None, 0, "",
                0.0, 0.0, 0.0,
                team_abbr, "", "Reserve", 0,
                DRAFT_ROOKIE_OPTIONS_REMAINING, "", "", mlbam_id,
                draft_player_id, DRAFT_YEAR,
                as_int(drow["draft_round"], None),
                as_int(drow["draft_pick"], None),
                str(drow["draft_label"] or ""),
                str(drow["drafted_at"] or ""),
                team_abbr,
            ))
            inserted += 1
        else:
            cur.execute("""
                UPDATE roster_players
                SET name=COALESCE(NULLIF(?, ''), name),
                    last_name=COALESCE(NULLIF(?, ''), last_name),
                    first_name=COALESCE(NULLIF(?, ''), first_name),
                    position=COALESCE(NULLIF(?, ''), position),
                    date_of_birth=COALESCE(NULLIF(?, ''), date_of_birth),
                    bats=COALESCE(NULLIF(?, ''), bats),
                    throws=COALESCE(NULLIF(?, ''), throws),
                    signed=?,
                    contract_type=?,
                    salary=?,
                    contract_initial_season=?,
                    contract_length=?,
                    contract_option=?,
                    contract_expires=?,
                    service_time=?,
                    previous_service_time=?,
                    service_time_2025=?,
                    franchise=?,
                    affiliate_team=?,
                    roster_status=?,
                    active_roster=?,
                    options_remaining=?,
                    fa_class=?,
                    draft_player_id=?,
                    draft_year=?,
                    draft_round=?,
                    draft_pick=?,
                    draft_label=?,
                    drafted_at=?,
                    draft_team=?,
                    mlbam_id=COALESCE(?, mlbam_id)
                WHERE id=?
            """, (
                name, last, first, str(drow["position"] or "").strip(), dob,
                str(drow["bats"] or "").strip(), str(drow["throws"] or "").strip(),
                *values, mlbam_id, match_id,
            ))
            updated += 1

    if own_conn:
        conn.commit()
        conn.close()

    current_app.logger.info(
        "Draft-to-roster sync complete: %s drafted players, %s inserted, %s updated",
        seen, inserted, updated,
    )
    return (seen, inserted, updated)


def bootstrap_roster():
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS roster_players (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            last_name TEXT,
            first_name TEXT,
            suffix TEXT,
            nickname TEXT,
            position TEXT,
            date_of_birth TEXT,
            bats TEXT,
            throws TEXT,
            signed INTEGER DEFAULT 1,
            contract_type TEXT,
            salary REAL,
            contract_initial_season INTEGER,
            contract_length INTEGER,
            contract_option INTEGER,
            contract_expires TEXT,
            service_time REAL,
            previous_service_time REAL,
            service_time_2025 REAL,
            franchise TEXT,
            affiliate_team TEXT,
            roster_status TEXT,
            active_roster INTEGER DEFAULT 0,
            options_remaining INTEGER,
            fa_class TEXT,
            fangraphs_id TEXT,
            mlbam_id INTEGER
        )
    """)

    # Existing DBs created before this change need this column so that 2026 FAs
    # still show as 2026 after their contract fields are cleared.
    cur.execute("PRAGMA table_info(roster_players)")
    existing_cols = {row[1] for row in cur.fetchall()}
    added_active_roster_col = False
    if "active_roster" not in existing_cols:
        cur.execute("ALTER TABLE roster_players ADD COLUMN active_roster INTEGER DEFAULT 0")
        added_active_roster_col = True

    if "fa_class" not in existing_cols:
        cur.execute("ALTER TABLE roster_players ADD COLUMN fa_class TEXT")

    ensure_roster_decision_columns(conn)
    ensure_roster_draft_columns(conn)

    cur.execute("SELECT COUNT(*) FROM roster_players")
    count = int(cur.fetchone()[0] or 0)

    if count == 0:
        csv_path = Path(current_app.config["ROSTER_CSV_PATH"])
        if csv_path.exists():
            with csv_path.open(newline="", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                for r in reader:
                    raw_id = (r.get("id") or "").strip()
                    raw_name = (r.get("name") or "").strip()

                    # skip blank / malformed rows
                    if not raw_id or not raw_name:
                        continue

                    try:
                        player_id = int(raw_id)
                    except ValueError:
                        continue

                    raw_contract_type = (r.get("contract_type") or "").strip().upper()
                    raw_franchise = canonical_team_abbr(r.get("franchise"))
                    raw_contract_expires = (r.get("contract_expires") or "").strip()
                    raw_contract_option = as_bool(r.get("contract_option"))

                    raw_contract_initial_season = as_int(r.get("contract_initial_season"))
                    raw_contract_length = as_int(r.get("contract_length"))
                    raw_service_time = as_float(r.get("service_time"), 0.0)
                    raw_service_time_2025 = as_float(r.get("service_time_2025"), 0.0)

                    raw_fa_class = csv_fa_class(r)

                    # Determine whether this player is effectively a current FA and therefore
                    # should no longer be tied to a team or to old contract details in the live DB.
                    is_2026_fa = raw_fa_class == CURRENT_FA_CLASS

                    stored_contract_type = "FA" if is_2026_fa else raw_contract_type
                    if not is_2026_fa and stored_contract_type == "R" and raw_service_time >= 3.0:
                        stored_contract_type = "A"
                    stored_franchise = "" if is_2026_fa else raw_franchise
                    stored_affiliate_team = "" if is_2026_fa else (r.get("team") or "").strip()
                    stored_roster_status = "" if is_2026_fa else roster_status_from_csv(r.get("status"), r.get("active_roster"), r.get("expanded_roster"))
                    stored_contract_option = 0 if is_2026_fa else (1 if raw_contract_option else 0)
                    stored_contract_initial_season = None if is_2026_fa else raw_contract_initial_season
                    stored_contract_length = None if is_2026_fa else raw_contract_length
                    stored_contract_expires = "" if is_2026_fa else raw_contract_expires
                    stored_salary = 0.0 if is_2026_fa else as_float(r.get("salary"), 0.0)
                    stored_fa_class = raw_fa_class

                    raw_options_remaining = as_int(r.get("options_remaining"), 0) or 0
                    optioned_this_season = as_bool(r.get("optioned_current_season"))

                    stored_options_remaining = raw_options_remaining - (1 if optioned_this_season else 0)
                    if stored_options_remaining < 0:
                        stored_options_remaining = 0

                    cur.execute("""
                        INSERT INTO roster_players (
                            id, name, last_name, first_name, suffix, nickname,
                            position, date_of_birth, bats, throws, signed,
                            contract_type, salary, contract_initial_season,
                            contract_length, contract_option, contract_expires,
                            service_time, previous_service_time, service_time_2025,
                            franchise, affiliate_team, roster_status, active_roster,
                            options_remaining, fa_class, fangraphs_id, mlbam_id
                        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                    """, (
                        player_id,
                        raw_name,
                        (r.get("last_name") or "").strip(),
                        (r.get("first_name") or "").strip(),
                        (r.get("suffix") or "").strip(),
                        (r.get("nickname") or "").strip(),
                        (r.get("position") or "").strip(),
                        (r.get("date_of_birth") or "").strip(),
                        (r.get("bats") or "").strip(),
                        (r.get("throws") or "").strip(),
                        1 if as_bool(r.get("signed")) else 0,
                        stored_contract_type,
                        stored_salary,
                        stored_contract_initial_season,
                        stored_contract_length,
                        stored_contract_option,
                        stored_contract_expires,
                        raw_service_time,
                        as_float(r.get("previous_service_time"), 0.0),
                        raw_service_time_2025,
                        stored_franchise,
                        stored_affiliate_team,
                        stored_roster_status,
                        0 if is_2026_fa else (1 if as_bool(r.get("active_roster")) else 0),
                        stored_options_remaining,
                        stored_fa_class,
                        (r.get("fangraphs_id") or "").strip(),
                        as_int(r.get("mlbam_id")),
                    ))


    # If this DB existed before active_roster was tracked, backfill it once from
    # the roster CSV. After that, financials read the live DB value so direct
    # DB changes are reflected without another import layer.
    if added_active_roster_col:
        csv_path = Path(current_app.config["ROSTER_CSV_PATH"])
        if csv_path.exists():
            with csv_path.open(newline="", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                for r in reader:
                    raw_id = (r.get("id") or "").strip()
                    if not raw_id:
                        continue
                    try:
                        player_id = int(raw_id)
                    except ValueError:
                        continue
                    cur.execute("""
                        UPDATE roster_players
                        SET active_roster=?
                        WHERE id=?
                    """, (0 if is_current_free_agent_csv(r) else (1 if as_bool(r.get("active_roster")) else 0), player_id))

    # Normalize older two-bucket DBs into the new three-bucket model.
    # Previous versions stored active players as roster_status='40-man' with
    # active_roster=1.  The UI now treats roster_status as the source of truth.
    cur.execute("""
        UPDATE roster_players
        SET roster_status='Active'
        WHERE COALESCE(active_roster, 0) = 1
          AND COALESCE(franchise, '') != ''
          AND roster_status = '40-man'
    """)
    cur.execute("""
        UPDATE roster_players
        SET roster_status='40-man'
        WHERE COALESCE(active_roster, 0) != 1
          AND LOWER(COALESCE(roster_status, '')) IN ('active', 'expanded')
          AND COALESCE(franchise, '') != ''
    """)
    cur.execute("""
        UPDATE roster_players
        SET active_roster = CASE WHEN roster_status='Active' THEN 1 ELSE 0 END
    """)

    # The initial import above only runs for an empty DB.  For an existing DB,
    # still apply the 2026 FA release logic from the current CSV so option-year
    # players do not remain attached to last year's club.
    csv_path = Path(current_app.config["ROSTER_CSV_PATH"])
    if csv_path.exists():
        with csv_path.open(newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for r in reader:
                raw_id = (r.get("id") or "").strip()
                if not raw_id or not is_current_free_agent_csv(r):
                    continue
                try:
                    player_id = int(raw_id)
                except ValueError:
                    continue

                cur.execute("""
                    UPDATE roster_players
                    SET signed=0,
                        contract_type='FA',
                        salary=0,
                        contract_initial_season=NULL,
                        contract_length=NULL,
                        contract_option=0,
                        contract_expires='',
                        franchise='',
                        affiliate_team='',
                        roster_status='',
                        active_roster=0,
                        fa_class=?
                    WHERE id=?
                """, (CURRENT_FA_CLASS, player_id))

    # Bring completed 2025 draft picks into roster.db as reserve-roster rookies.
    sync_drafted_players_from_draft_db(conn)

    # Keep arbitration status in sync with service-time rules for existing DBs.
    normalize_roster_contract_types(conn)

    # Keep DOBs in one canonical format for display and eligibility checks.
    # This also converts older DD-MM-YY roster imports to YYYY-MM-DD.
    normalize_roster_birthdates(conn)

    conn.commit()
    conn.close()



@roster_bp.get("/api/players")
def api_players():
    conn = get_conn()
    cur = conn.cursor()

    search = (request.args.get("search") or "").strip().lower()
    team = canonical_team_abbr(request.args.get("team"))
    contract_type = (request.args.get("contract_type") or "").strip().upper()
    position = (request.args.get("position") or "").strip().upper()
    roster_status = (request.args.get("roster_status") or "").strip()
    fa_class_filter = (request.args.get("fa_class") or "").strip()
    rulev_only = (request.args.get("rulev_only") == "1")
    contract_decisions_only = (request.args.get("contract_decisions_only") == "1")
    forty_man_count = None
    active_count = None
    
    q = "SELECT * FROM roster_players"
    clauses = []
    params: List[Any] = []

    if search:
        s2 = "".join(ch for ch in unicodedata.normalize("NFKD", search) if not unicodedata.combining(ch))
        clauses.append("LOWER(unaccent(name)) LIKE ?")
        params.append(f"%{s2}%")

    if team:
        clauses.append("franchise = ?")
        params.append(team)

    if contract_type:
        clauses.append("contract_type = ?")
        params.append(contract_type)

    if position:
        if position in ("IF", "OF"):
            clauses.append("position = ?")
            params.append(position)
        else:
            clauses.append("(position = ? OR position LIKE ?)")
            params.extend([position, f"%{position}%"])

    if roster_status:
        clauses.append("roster_status = ?")
        params.append(roster_status)

    if clauses:
        q += " WHERE " + " AND ".join(clauses)

    q += " ORDER BY unaccent(name) COLLATE NOCASE ASC"

    cur.execute(q, params)
    rows = cur.fetchall()

    if team:
        cur.execute("""
            SELECT
                COALESCE(SUM(CASE WHEN roster_status = 'Active' THEN 1 ELSE 0 END), 0) AS active_count,
                COALESCE(SUM(CASE WHEN roster_status IN ('Active', '40-man') THEN 1 ELSE 0 END), 0) AS forty_man_count
            FROM roster_players
            WHERE franchise = ?
        """, (team,))
        count_row = cur.fetchone()
        active_count = int(count_row["active_count"] or 0)
        forty_man_count = int(count_row["forty_man_count"] or 0)

    conn.close()

    out = []
    for r in rows:
        row_fa_class = compute_fa_class(r)
        row_rulev = rulev_eligible(
            r["roster_status"],
            r["date_of_birth"],
            r["contract_type"],
            r["franchise"],
            r["signed"],
            row_value(r, "draft_year", None),
        )

        if rulev_only and not row_rulev:
            continue
        if fa_class_filter and row_fa_class != fa_class_filter:
            continue

        pending_option = pending_option_decision(r)
        pending_arb = pending_arbitration_decision(r)
        if contract_decisions_only and not (pending_option or pending_arb):
            continue

        is_current_fa = row_fa_class == CURRENT_FA_CLASS and not (r["franchise"] or "")
        can_edit = (
            canonical_team_abbr(session.get("roster_authed_team", "")) != "" and
            canonical_team_abbr(r["franchise"] or "") == canonical_team_abbr(session.get("roster_authed_team", ""))
        )

        out.append({
            "id": r["id"],
            "name": r["name"],
            "position": r["position"],
            "team": r["franchise"] or "",
            "dob": normalize_roster_dob(r["date_of_birth"]),
            "bt": bt_text(r["bats"], r["throws"]),
            "roster_status": r["roster_status"] or "",
            "active_roster": (r["roster_status"] or "") == "Active",
            "contract_type": "" if is_current_fa else (r["contract_type"] or ""),
            "service_time": round(float(r["service_time"] or 0), 2),
            "salary_m": None if is_current_fa else round(float(r["salary"] or 0) / 1_000_000.0, 3),
            "team_opt": False if is_current_fa else bool(int(r["contract_option"] or 0)),
            "fa_class": row_fa_class,
            "options_remaining": int(r["options_remaining"] or 0),
            "rulev_eligible": row_rulev,
            "pending_option_decision": pending_option,
            "pending_arbitration_decision": pending_arb,
            "contract_decision_pending": pending_option or pending_arb,
            "arbitration_decision": row_value(r, "arbitration_decision", "") or "",
            "option_decision": row_value(r, "option_decision", "") or "",
            "can_edit_status": can_edit,
            "can_release": can_edit and not (pending_option or pending_arb),
            "can_exercise_option": can_edit and pending_option,
            "can_decline_option": can_edit and pending_option,
            "can_tender_arbitration": can_edit and pending_arb,
            "can_decline_arbitration": can_edit and pending_arb,
        })

    return jsonify({
        "players": out,
        "teams": TEAM_ABBRS,
        "contract_types": CONTRACT_TYPES,
        "positions": POSITIONS,
        "status_types": STATUS_TYPES,
        "fa_classes": FA_CLASSES,
        "forty_man_count": forty_man_count,
        "active_count": active_count,

    })

@roster_bp.post("/api/login_team")
def api_login_team():
    data = request.get_json(force=True, silent=True) or {}
    team = canonical_team_abbr(data.get("team"))
    email = (data.get("email") or "").strip()

    expected = TEAM_EMAILS.get(team)
    if not expected:
        return ("Unknown team", 400)

    if emails_equal(email, expected):
        session["roster_authed_team"] = team
        session["roster_authed_email"] = email
        return jsonify({"ok": True})
    return ("Invalid email", 401)


@roster_bp.get("/api/status")
def api_status():
    return jsonify({
        "authed_team": canonical_team_abbr(session.get("roster_authed_team", "")),
        "authed_email": session.get("roster_authed_email", ""),
        "teams": TEAM_ABBRS,
    })


def require_roster_team() -> str:
    team = canonical_team_abbr(session.get("roster_authed_team"))
    if not team:
        abort(401, "Not logged in")
    session["roster_authed_team"] = team
    return team

@roster_bp.post("/api/update_player")
def api_update_player():
    team = require_roster_team()
    data = request.get_json(force=True, silent=True) or {}

    player_id = int(data.get("id") or 0)
    new_status = (data.get("roster_status") or "").strip()

    if player_id <= 0:
        return ("Missing player id", 400)

    if new_status not in ("Active", "40-man", "Reserve"):
        return ("Invalid roster status", 400)

    conn = get_conn()
    cur = conn.cursor()
    ensure_roster_decision_columns(conn)

    cur.execute("SELECT * FROM roster_players WHERE id=?", (player_id,))
    row = cur.fetchone()

    if not row:
        conn.close()
        return ("Player not found", 404)

    if (row["franchise"] or "") != team:
        conn.close()
        return ("You can only edit your own team", 403)

    old_status = row["roster_status"] or ""
    old_on_40 = old_status in ("Active", "40-man")
    new_on_40 = new_status in ("Active", "40-man")

    # Moving a reserve player onto either Active or 40-man consumes a 40-man slot.
    if not old_on_40 and new_on_40:
        cur.execute("""
            SELECT COUNT(*)
            FROM roster_players
            WHERE franchise = ? AND roster_status IN ('Active', '40-man')
        """, (team,))
        forty_count = int(cur.fetchone()[0] or 0)

        if forty_count >= 40:
            conn.close()
            return ("Cannot add player to 40-man: team already has 40 players on the 40-man roster", 409)

    waiver_reason = ""
    should_waive = False
    options_remaining = int(row["options_remaining"] or 0)

    if old_status == "Active" and new_status == "40-man" and options_remaining <= 0:
        should_waive = True
        waiver_reason = "Out of options: active player sent to minors/40-man"
    elif old_on_40 and not new_on_40:
        should_waive = True
        waiver_reason = "Removed from 40-man roster"

    if should_waive:
        from waivers_app import create_waiver_from_roster_row
        create_waiver_from_roster_row(
            conn,
            row,
            waived_from_team=team,
            pre_waiver_status=old_status,
            desired_status=new_status,
            waiver_reason=waiver_reason,
        )

    cur.execute("""
        UPDATE roster_players
        SET roster_status=?, active_roster=?
        WHERE id=?
    """, (new_status, 1 if new_status == "Active" else 0, player_id))

    conn.commit()
    conn.close()

    sync_after_roster_mutation()
    return jsonify({"ok": True, "waiver_created": should_waive})


@roster_bp.post("/api/player_action")
def api_player_action():
    team = require_roster_team()
    data = request.get_json(force=True, silent=True) or {}
    player_id = int(data.get("id") or 0)
    action = (data.get("action") or "").strip().lower()

    if player_id <= 0:
        return ("Missing player id", 400)
    if action not in {
        "release", "exercise_option", "decline_option",
        "tender_arbitration", "decline_arbitration",
    }:
        return ("Invalid action", 400)

    conn = get_conn()
    cur = conn.cursor()
    ensure_roster_decision_columns(conn)
    cur.execute("SELECT * FROM roster_players WHERE id=?", (player_id,))
    row = cur.fetchone()
    if not row:
        conn.close()
        return ("Player not found", 404)
    if (row["franchise"] or "") != team:
        conn.close()
        return ("You can only edit your own team", 403)

    old_team = row["franchise"] or ""
    old_status = row["roster_status"] or ""
    msg = ""

    if action == "release":
        if pending_option_decision(row) or pending_arbitration_decision(row):
            conn.close()
            return ("This player has a pending contract decision. Use tender/non-tender or exercise/decline option instead.", 409)
        from waivers_app import create_waiver_from_roster_row
        create_waiver_from_roster_row(
            conn,
            row,
            waived_from_team=team,
            pre_waiver_status=old_status,
            desired_status="Released",
            waiver_reason="Released by team",
        )
        set_player_free_agent(cur, player_id, CURRENT_FA_CLASS)
        msg = "Player released and placed on waivers."

    elif action == "exercise_option":
        if not pending_option_decision(row):
            conn.close()
            return ("This player does not have a pending option decision", 409)
        stamp = now_utc_iso()
        cur.execute("""
            UPDATE roster_players
            SET contract_option=0,
                option_decision='exercised',
                option_decision_at=?
            WHERE id=?
        """, (stamp, player_id))
        msg = "Option exercised."

    elif action == "decline_option":
        if not pending_option_decision(row):
            conn.close()
            return ("This player does not have a pending option decision", 409)
        salary = float(row["salary"] or 0.0)
        buyout = round(salary * 0.10, 2)
        set_player_free_agent(cur, player_id, CURRENT_FA_CLASS, option_decision="declined")
        msg = f"Option declined. Buyout posted: ${buyout:,.0f}."
        conn.commit()
        conn.close()
        try:
            from financials_app import record_finance_payment
            record_finance_payment(
                source_type="contract_option_buyout",
                source_id=player_id,
                payer_team_abbr=old_team,
                receiver_team_abbr="LEAGUE",
                amount=buyout,
                description=f"10% option buyout for {row['name']}",
            )
        except Exception:
            current_app.logger.exception("Failed to post option buyout")
        sync_after_roster_mutation()
        update_fa_last_team(player_id, old_team)
        return jsonify({"ok": True, "message": msg})

    elif action == "tender_arbitration":
        if not pending_arbitration_decision(row):
            conn.close()
            return ("This player does not have a pending arbitration decision", 409)
        stamp = now_utc_iso()
        cur.execute("""
            UPDATE roster_players
            SET arbitration_decision='tendered',
                arbitration_decision_at=?
            WHERE id=?
        """, (stamp, player_id))
        msg = "Arbitration salary tendered."

    elif action == "decline_arbitration":
        if not pending_arbitration_decision(row):
            conn.close()
            return ("This player does not have a pending arbitration decision", 409)
        set_player_free_agent(cur, player_id, CURRENT_FA_CLASS, arbitration_decision="declined")
        msg = "Arbitration declined; player released to free agency."

    conn.commit()
    conn.close()

    sync_after_roster_mutation()
    if action in {"release", "decline_arbitration"}:
        update_fa_last_team(player_id, old_team)
    return jsonify({"ok": True, "message": msg})

ROSTER_HTML = """
<!doctype html>
<html>
<head>
  <base href="/roster/">
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Rosters</title>
  __BNSL_GAME_CSS__
  <style>
    .wrap { max-width: 1550px; margin: 0 auto; padding: 18px; }
    .topbar { display:flex; flex-wrap:wrap; gap:10px; align-items:center; margin-bottom:12px; }
    .pill { padding:8px 10px; border-radius:999px; }
    .btn[disabled] { opacity:.5; cursor:not-allowed; }
    table { width:100%; border-collapse: collapse; }
    th, td { padding:8px 10px; border-bottom:1px solid rgba(255,255,255,.08); vertical-align:top; }
    th { text-align:left; }
    .actions { display:flex; gap:6px; flex-wrap:wrap; min-width:210px; }
    .mini { font-size: 12px; padding: 5px 8px; border-radius:999px; }
    .danger { border-color: rgba(255,77,109,.45); }
    .decision { color: var(--gold); font-weight: 700; }
    .muted { opacity:.72; }
  </style>
</head>
<body>
  <div class="wrap">
    <h1>Roster Database</h1>

    <div class="topbar">
      <select id="team-filter"></select>
      <select id="contract-filter"></select>
      <select id="position-filter"></select>
      <select id="status-filter"></select>
      <select id="fa-class-filter"></select>
      <label><input type="checkbox" id="rulev-only"> Rule V eligible only</label>
      <label><input type="checkbox" id="contract-decisions-only"> Contract Decisions</label>
      <input id="search" type="text" placeholder="Search players...">
      <button id="login-btn">Login</button>
      <span id="login-status">Not logged in</span>
      <span id="forty-man-count" style="display:none;"></span>
    </div>

    <table>
      <thead>
        <tr>
          <th>Pos</th>
          <th>Name</th>
          <th>Team</th>
          <th>DOB</th>
          <th>B/T</th>
          <th>Status</th>
          <th>Contract</th>
          <th>Service</th>
          <th>Salary</th>
          <th>Team Opt?</th>
          <th>FA Class</th>
          <th>Options</th>
          <th>Actions</th>
        </tr>
      </thead>
      <tbody id="roster-body"></tbody>
    </table>
  </div>

<script>
const body = document.getElementById("roster-body");
const search = document.getElementById("search");
const teamFilter = document.getElementById("team-filter");
const contractFilter = document.getElementById("contract-filter");
const positionFilter = document.getElementById("position-filter");
const statusFilter = document.getElementById("status-filter");
const faClassFilter = document.getElementById("fa-class-filter");
const rulevOnly = document.getElementById("rulev-only");
const contractDecisionsOnly = document.getElementById("contract-decisions-only");
const loginBtn = document.getElementById("login-btn");
const loginStatus = document.getElementById("login-status");
const fortyManCount = document.getElementById("forty-man-count");

let state = {
  authedTeam: "",
  search: "",
  team: "",
  contractType: "",
  position: "",
  rosterStatus: "",
  faClass: "",
  rulevOnly: false,
  contractDecisionsOnly: false,
};

function esc(s) {
  return String(s ?? "").replace(/[&<>\"]/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;'}[c]));
}

function moneyM(x) {
  return `$${Number(x).toFixed(3)}M`;
}

function makeOptions(el, values, label) {
  el.innerHTML = `<option value="">${label}</option>` +
    values.map(v => `<option value="${v}">${v}</option>`).join("");
}

async function fetchStatus() {
  const res = await fetch("api/status");
  const data = await res.json();
  state.authedTeam = data.authed_team || "";
  loginStatus.textContent = state.authedTeam
    ? `Logged in for ${state.authedTeam}`
    : "Not logged in";
}

function actionButton(label, action, playerId, enabled, danger=false) {
  if (!enabled) return "";
  const cls = danger ? "btn mini danger player-action" : "btn mini player-action";
  return `<button class="${cls}" data-action="${action}" data-player-id="${playerId}">${label}</button>`;
}

function actionHtml(p) {
  const bits = [];
  if (p.contract_decision_pending) {
    bits.push(`<span class="decision">Decision pending</span>`);
  }
  bits.push(actionButton("Release", "release", p.id, p.can_release, true));
  bits.push(actionButton("Exercise option", "exercise_option", p.id, p.can_exercise_option));
  bits.push(actionButton("Decline option", "decline_option", p.id, p.can_decline_option, true));
  bits.push(actionButton("Tender arb", "tender_arbitration", p.id, p.can_tender_arbitration));
  bits.push(actionButton("Non-tender", "decline_arbitration", p.id, p.can_decline_arbitration, true));
  return bits.filter(Boolean).join(" ") || `<span class="muted">—</span>`;
}

async function fetchPlayers() {
  const params = new URLSearchParams({
    search: state.search,
    team: state.team,
    contract_type: state.contractType,
    position: state.position,
    roster_status: state.rosterStatus,
    fa_class: state.faClass,
    rulev_only: state.rulevOnly ? "1" : "0",
    contract_decisions_only: state.contractDecisionsOnly ? "1" : "0",
  });

  const res = await fetch("api/players?" + params.toString());
  const data = await res.json();

  makeOptions(teamFilter, data.teams || [], "All teams");
  makeOptions(contractFilter, data.contract_types || [], "All contracts");
  makeOptions(positionFilter, data.positions || [], "All positions");
  makeOptions(statusFilter, data.status_types || [], "All statuses");
  makeOptions(faClassFilter, data.fa_classes || [], "FA class");

  teamFilter.value = state.team;
  contractFilter.value = state.contractType;
  positionFilter.value = state.position;
  statusFilter.value = state.rosterStatus;
  faClassFilter.value = state.faClass;

  if (state.team && data.forty_man_count !== null && data.forty_man_count !== undefined) {
    const activeText = data.active_count !== null && data.active_count !== undefined
      ? `${data.active_count} active • `
      : "";
    fortyManCount.textContent = `${activeText}${data.forty_man_count}/40 on 40-man`;
    fortyManCount.style.display = "inline";
  } else {
    fortyManCount.textContent = "";
    fortyManCount.style.display = "none";
  }

  body.innerHTML = "";

  for (const p of data.players || []) {
    const tr = document.createElement("tr");

    let statusHtml = esc(p.roster_status || "");
    if (p.can_edit_status) {
      const selActive = p.roster_status === "Active" ? "selected" : "";
      const sel40 = p.roster_status === "40-man" ? "selected" : "";
      const selRes = p.roster_status === "Reserve" ? "selected" : "";
      statusHtml =
        '<select class="status-edit" data-player-id="' + p.id + '" data-current-status="' + esc(p.roster_status || "") + '" data-options-remaining="' + p.options_remaining + '">' +
          '<option value="Active" ' + selActive + '>Active</option>' +
          '<option value="40-man" ' + sel40 + '>40-man</option>' +
          '<option value="Reserve" ' + selRes + '>Reserve</option>' +
        '</select>';
    }

    tr.innerHTML = `
      <td>${esc(p.position || "")}</td>
      <td><b>${esc(p.name)}</b></td>
      <td>${esc(p.team || "")}</td>
      <td>${esc(p.dob || "")}</td>
      <td>${esc(p.bt || "")}</td>
      <td>${statusHtml}</td>
      <td>${esc(p.contract_type || "")}</td>
      <td>${Number(p.service_time).toFixed(2)}</td>
      <td>${p.salary_m === null || p.salary_m === undefined ? "" : moneyM(p.salary_m)}</td>
      <td>${p.team_opt ? "✓" : ""}</td>
      <td>${esc(p.fa_class || "")}</td>
      <td>${p.options_remaining}</td>
      <td><div class="actions">${actionHtml(p)}</div></td>
    `;
    body.appendChild(tr);
  }

  document.querySelectorAll(".status-edit").forEach(sel => {
    sel.onchange = async () => {
      const playerId = sel.dataset.playerId;
      const oldStatus = sel.dataset.currentStatus || "";
      const optionsRemaining = Number(sel.dataset.optionsRemaining || 0);
      const newStatus = sel.value;
      const oldOn40 = oldStatus === "Active" || oldStatus === "40-man";
      const newOn40 = newStatus === "Active" || newStatus === "40-man";
      let confirmMessage = "";
      if (oldStatus === "Active" && newStatus === "40-man" && optionsRemaining <= 0) {
        confirmMessage = "This player is out of options, so this move will place him on waivers. Continue?";
      } else if (oldOn40 && !newOn40) {
        confirmMessage = "Removing this player from the 40-man roster will place him on waivers. Continue?";
      }
      if (confirmMessage && !confirm(confirmMessage)) {
        sel.value = oldStatus;
        return;
      }
      sel.disabled = true;
      const resp = await fetch("api/update_player", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ id: Number(playerId), roster_status: newStatus })
      });
      if (!resp.ok) {
        alert(await resp.text());
      } else {
        const data = await resp.json();
        if (data.waiver_created) alert("This roster move placed the player on waivers.");
      }
      await fetchPlayers();
    };
  });

  document.querySelectorAll(".player-action").forEach(btn => {
    btn.onclick = async () => {
      const action = btn.dataset.action;
      const playerId = Number(btn.dataset.playerId);
      const prompts = {
        release: "Release this player? They will become a free agent and enter waivers.",
        decline_option: "Decline this option? The player becomes a free agent and a 10% buyout is posted.",
        decline_arbitration: "Non-tender this arbitration player? The player becomes a free agent with no waiver.",
      };
      if (prompts[action] && !confirm(prompts[action])) return;
      btn.disabled = true;
      const resp = await fetch("api/player_action", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ id: playerId, action })
      });
      if (!resp.ok) {
        alert(await resp.text());
      } else {
        const data = await resp.json();
        if (data.message) alert(data.message);
      }
      await fetchPlayers();
    };
  });
}

function debounce(fn, ms) {
  let t;
  return (...args) => {
    clearTimeout(t);
    t = setTimeout(() => fn(...args), ms);
  };
}

search.addEventListener("input", debounce(() => {
  state.search = search.value;
  fetchPlayers();
}, 120));

teamFilter.onchange = () => { state.team = teamFilter.value; fetchPlayers(); };
contractFilter.onchange = () => { state.contractType = contractFilter.value; fetchPlayers(); };
positionFilter.onchange = () => { state.position = positionFilter.value; fetchPlayers(); };
statusFilter.onchange = () => { state.rosterStatus = statusFilter.value; fetchPlayers(); };
faClassFilter.onchange = () => { state.faClass = faClassFilter.value; fetchPlayers(); };
rulevOnly.onchange = () => { state.rulevOnly = rulevOnly.checked; fetchPlayers(); };
contractDecisionsOnly.onchange = () => { state.contractDecisionsOnly = contractDecisionsOnly.checked; fetchPlayers(); };

loginBtn.onclick = async () => {
  const team = prompt("Team abbreviation?");
  if (!team) return;
  const email = prompt("Manager email?");
  if (!email) return;

  const resp = await fetch("api/login_team", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ team: team.trim().toUpperCase(), email: email.trim() })
  });

  if (!resp.ok) {
    alert(await resp.text());
    return;
  }

  await fetchStatus();
  await fetchPlayers();
};

(async function boot() {
  await fetchStatus();
  await fetchPlayers();
})();
</script>
</body>
</html>
""".replace("__BNSL_GAME_CSS__", BNSL_GAME_CSS)

@roster_bp.get("/")
def roster_index():
    return render_template_string(ROSTER_HTML)
