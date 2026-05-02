from __future__ import annotations
from pathlib import Path
from datetime import datetime
from typing import Any, Dict, List
import csv
import os
import sqlite3
import unicodedata

from flask import (
    Blueprint, current_app, jsonify, render_template_string,
    request, session, abort
)

from ui_skin import BNSL_GAME_CSS

roster_bp = Blueprint("roster", __name__)

TEAM_EMAILS = {
    "TOR": "daniele.defeo@gmail.com",
    "NYY": "dmsund66@gmail.com",
    "BOS": "chris_lawrence@sbcglobal.net",
    "TB": "smith.mark.louis@gmail.com",
    "BAL": "bsweis@ptd.net",
    "DET": "manconley@gmail.com",
    "KC": "jim@timhafer.com",
    "MIN": "jonathan.adelman@gmail.com",
    "CHW": "bglover6@gmail.com",
    "CLE": "bonfanti20@gmail.com",
    "LAA": "dsucoff@gmail.com",
    "SEA": "daniel_a_fisher@yahoo.com",
    "OAK": "bspropp@hotmail.com",
    "HOU": "golk624@protonmail.com",
    "TEX": "Brianorr@live.com",
    "WSH": "smsetnor@gmail.com",
    "NYM": "kerkhoffc@gmail.com",
    "PHI": "jdcarney26@gmail.com",
    "ATL": "stevegaston@yahoo.com",
    "MIA": "schmitz@ucsb.edu",
    "STL": "parkbench@mac.com",
    "CHC": "bryanhartman@gmail.com",
    "PIT": "jseiner24@gmail.com",
    "MIL": "tsurratt@hiaspire.com",
    "CIN": "jpmile@yahoo.com",
    "LAD": "jr92@comcast.net",
    "COL": "GypsySon@gmail.com",
    "ARI": "mhr4240@gmail.com",
    "SF": "jasonmallet@gmail.com",
    "SD": "mattaca77@gmail.com",
}

TEAM_ABBRS = sorted(TEAM_EMAILS.keys())

POSITIONS = ["P","C","1B","2B","3B","SS","LF","CF","RF","DH","IF","OF"]
CONTRACT_TYPES = ["R","A","X","FA"]
STATUS_TYPES = ["Active","40-man","Reserve"]
FA_CLASSES = ["2026","2027","2028","2029","2030"]
CURRENT_SEASON = 2025
CURRENT_FA_CLASS = str(CURRENT_SEASON + 1)


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


def emails_equal(a: str | None, b: str | None) -> bool:
    if not a or not b:
        return False
    return a.strip().lower() == b.strip().lower()


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


def rulev_eligible(status: str, dob: str) -> bool:
    if status != "Reserve":
        return False
    try:
        year = int((dob or "")[:4])
        return year <= 2001
    except Exception:
        return False


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
                    raw_franchise = (r.get("franchise") or "").strip()
                    raw_contract_expires = (r.get("contract_expires") or "").strip()
                    raw_contract_option = as_bool(r.get("contract_option"))

                    raw_contract_initial_season = as_int(r.get("contract_initial_season"))
                    raw_contract_length = as_int(r.get("contract_length"))
                    raw_service_time_2025 = as_float(r.get("service_time_2025"), 0.0)

                    raw_fa_class = csv_fa_class(r)

                    # Determine whether this player is effectively a current FA and therefore
                    # should no longer be tied to a team or to old contract details in the live DB.
                    is_2026_fa = raw_fa_class == CURRENT_FA_CLASS

                    stored_contract_type = "FA" if is_2026_fa else raw_contract_type
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
                        as_float(r.get("service_time"), 0.0),
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

    conn.commit()
    conn.close()



@roster_bp.get("/api/players")
def api_players():
    conn = get_conn()
    cur = conn.cursor()

    search = (request.args.get("search") or "").strip().lower()
    team = (request.args.get("team") or "").strip().upper()
    contract_type = (request.args.get("contract_type") or "").strip().upper()
    position = (request.args.get("position") or "").strip().upper()
    roster_status = (request.args.get("roster_status") or "").strip()
    fa_class_filter = (request.args.get("fa_class") or "").strip()
    rulev_only = (request.args.get("rulev_only") == "1")
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
        row_rulev = rulev_eligible(r["roster_status"], r["date_of_birth"])

        if rulev_only and not row_rulev:
            continue
        if fa_class_filter and row_fa_class != fa_class_filter:
            continue

        is_current_fa = row_fa_class == CURRENT_FA_CLASS and not (r["franchise"] or "")

        out.append({
            "id": r["id"],
            "name": r["name"],
            "position": r["position"],
            "team": r["franchise"] or "",
            "dob": r["date_of_birth"],
            "bt": bt_text(r["bats"], r["throws"]),
            "roster_status": r["roster_status"] or "",
            "active_roster": (r["roster_status"] or "") == "Active",
            "contract_type": "" if is_current_fa else (r["contract_type"] or ""),
            "service_time": round(float(r["service_time"] or 0), 2),
            "salary_m": None if is_current_fa else round(float(r["salary"] or 0) / 1_000_000.0, 2),
            "team_opt": False if is_current_fa else bool(int(r["contract_option"] or 0)),
            "fa_class": row_fa_class,
            "options_remaining": int(r["options_remaining"] or 0),
            "rulev_eligible": row_rulev,
            "can_edit_status": (
              session.get("roster_authed_team", "") != "" and
              (r["franchise"] or "") == session.get("roster_authed_team", "")),
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
    team = (data.get("team") or "").strip().upper()
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
        "authed_team": session.get("roster_authed_team", ""),
        "authed_email": session.get("roster_authed_email", ""),
        "teams": TEAM_ABBRS,
    })


def require_roster_team() -> str:
    team = session.get("roster_authed_team")
    if not team:
        abort(401, "Not logged in")
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

    cur.execute("""
        SELECT id, franchise, roster_status
        FROM roster_players
        WHERE id=?
    """, (player_id,))
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

    cur.execute("""
        UPDATE roster_players
        SET roster_status=?, active_roster=?
        WHERE id=?
    """, (new_status, 1 if new_status == "Active" else 0, player_id))

    conn.commit()
    conn.close()
    return ("", 204)

ROSTER_HTML = f"""
<!doctype html>
<html>
<head>
  <base href="/roster/">
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Rosters</title>
  {BNSL_GAME_CSS}
  <style>
    .wrap {{ max-width: 1450px; margin: 0 auto; padding: 18px; }}
    .topbar {{ display:flex; flex-wrap:wrap; gap:10px; align-items:center; margin-bottom:12px; }}
    .pill {{ padding:8px 10px; border-radius:999px; }}
    .btn[disabled] {{ opacity:.5; cursor:not-allowed; }}
    table {{ width:100%; border-collapse: collapse; }}
    th, td {{ padding:8px 10px; border-bottom:1px solid rgba(255,255,255,.08); }}
    th {{ text-align:left; }}
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
const loginBtn = document.getElementById("login-btn");
const loginStatus = document.getElementById("login-status");
const fortyManCount = document.getElementById("forty-man-count");

let state = {{
  authedTeam: "",
  search: "",
  team: "",
  contractType: "",
  position: "",
  rosterStatus: "",
  faClass: "",
  rulevOnly: false,
}};

function moneyM(x) {{
  return `$${{Number(x).toFixed(2)}}M`;
}}

function makeOptions(el, values, label) {{
  el.innerHTML = `<option value="">${{label}}</option>` +
    values.map(v => `<option value="${{v}}">${{v}}</option>`).join("");
}}

async function fetchStatus() {{
  const res = await fetch("api/status");
  const data = await res.json();
  state.authedTeam = data.authed_team || "";
  loginStatus.textContent = state.authedTeam
    ? `Logged in for ${{state.authedTeam}}`
    : "Not logged in";
}}

async function fetchPlayers() {{
  const params = new URLSearchParams({{
    search: state.search,
    team: state.team,
    contract_type: state.contractType,
    position: state.position,
    roster_status: state.rosterStatus,
    fa_class: state.faClass,
    rulev_only: state.rulevOnly ? "1" : "0",
  }});

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

if (state.team && data.forty_man_count !== null && data.forty_man_count !== undefined) {{
  const activeText = data.active_count !== null && data.active_count !== undefined
    ? `${{data.active_count}} active • `
    : "";
  fortyManCount.textContent = `${{activeText}}${{data.forty_man_count}}/40 on 40-man`;
  fortyManCount.style.display = "inline";
}} else {{
  fortyManCount.textContent = "";
  fortyManCount.style.display = "none";
}}

  body.innerHTML = "";

  for (const p of data.players || []) {{
    const tr = document.createElement("tr");

    let statusHtml = (p.roster_status || "");
    if (p.can_edit_status) {{
      const selActive = p.roster_status === "Active" ? "selected" : "";
      const sel40 = p.roster_status === "40-man" ? "selected" : "";
      const selRes = p.roster_status === "Reserve" ? "selected" : "";
      statusHtml =
        '<select class="status-edit" data-player-id="' + p.id + '">' +
          '<option value="Active" ' + selActive + '>Active</option>' +
          '<option value="40-man" ' + sel40 + '>40-man</option>' +
          '<option value="Reserve" ' + selRes + '>Reserve</option>' +
        '</select>';
    }}

    tr.innerHTML = `
      <td>${{p.position || ""}}</td>
      <td><b>${{p.name}}</b></td>
      <td>${{p.team || ""}}</td>
      <td>${{p.dob || ""}}</td>
      <td>${{p.bt || ""}}</td>
      <td>${{statusHtml}}</td>
      <td>${{p.contract_type || ""}}</td>
      <td>${{Number(p.service_time).toFixed(2)}}</td>
      <td>${{p.salary_m === null || p.salary_m === undefined ? "" : moneyM(p.salary_m)}}</td>
      <td>${{p.team_opt ? "✓" : ""}}</td>
      <td>${{p.fa_class || ""}}</td>
      <td>${{p.options_remaining}}</td>
    `;
    body.appendChild(tr);
  }}

  document.querySelectorAll(".status-edit").forEach(sel => {{
    sel.onchange = async () => {{
      const playerId = sel.dataset.playerId;
      const newStatus = sel.value;

      sel.disabled = true;

      const resp = await fetch("api/update_player", {{
        method: "POST",
        headers: {{ "Content-Type": "application/json" }},
        body: JSON.stringify({{
          id: Number(playerId),
          roster_status: newStatus
        }})
      }});

      if (!resp.ok) {{
        alert(await resp.text());
      }}

      await fetchPlayers();
    }};
  }});
}}

function debounce(fn, ms) {{
  let t;
  return (...args) => {{
    clearTimeout(t);
    t = setTimeout(() => fn(...args), ms);
  }};
}}

search.addEventListener("input", debounce(() => {{
  state.search = search.value;
  fetchPlayers();
}}, 120));

teamFilter.onchange = () => {{ state.team = teamFilter.value; fetchPlayers(); }};
contractFilter.onchange = () => {{ state.contractType = contractFilter.value; fetchPlayers(); }};
positionFilter.onchange = () => {{ state.position = positionFilter.value; fetchPlayers(); }};
statusFilter.onchange = () => {{ state.rosterStatus = statusFilter.value; fetchPlayers(); }};
faClassFilter.onchange = () => {{ state.faClass = faClassFilter.value; fetchPlayers(); }};
rulevOnly.onchange = () => {{ state.rulevOnly = rulevOnly.checked; fetchPlayers(); }};

loginBtn.onclick = async () => {{
  const team = prompt("Team abbreviation?");
  if (!team) return;
  const email = prompt("Manager email?");
  if (!email) return;

  const resp = await fetch("api/login_team", {{
    method: "POST",
    headers: {{ "Content-Type": "application/json" }},
    body: JSON.stringify({{ team: team.trim().toUpperCase(), email: email.trim() }})
  }});

  if (!resp.ok) {{
    alert(await resp.text());
    return;
  }}

  await fetchStatus();
}};

(async function boot() {{
  await fetchStatus();
  await fetchPlayers();
}})();
</script>
</body>
</html>
"""

@roster_bp.get("/")
def roster_index():
    return render_template_string(ROSTER_HTML)
