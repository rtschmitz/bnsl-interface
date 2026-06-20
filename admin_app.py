from __future__ import annotations

from datetime import date, datetime, time, timezone
from pathlib import Path
from typing import Any
import contextlib
import csv
import io
import json
import os
import re
import sqlite3
import tempfile
import unicodedata

from flask import (
    Blueprint, abort, current_app, jsonify, redirect, render_template_string,
    request, session, url_for,
)

from bnsl_paths import db_path
from ui_skin import BNSL_GAME_CSS

try:
    from team_config import TEAM_ABBRS as CONFIG_TEAM_ABBRS, canonical_team_abbr as config_canonical_team_abbr
except Exception:  # pragma: no cover - fallback only for standalone parsing/tests
    CONFIG_TEAM_ABBRS = []
    def config_canonical_team_abbr(team: Any) -> str:
        return str(team or "").strip().upper()

try:
    from trades_app import ABBR_TO_FULL, TEAM_ORDER as TRADES_TEAM_ORDER
except Exception:  # pragma: no cover
    ABBR_TO_FULL = {
        "ARI": "Arizona Diamondbacks", "ATL": "Atlanta Braves", "BAL": "Baltimore Orioles",
        "BOS": "Boston Red Sox", "CHC": "Chicago Cubs", "CHW": "Chicago White Sox",
        "CIN": "Cincinnati Reds", "CLE": "Cleveland Guardians", "COL": "Colorado Rockies",
        "DET": "Detroit Tigers", "HOU": "Houston Astros", "KC": "Kansas City Royals",
        "LAA": "Los Angeles Angels", "LAD": "Los Angeles Dodgers", "MIA": "Miami Marlins",
        "MIL": "Milwaukee Brewers", "MIN": "Minnesota Twins", "NYM": "New York Mets",
        "NYY": "New York Yankees", "OAK": "Oakland Athletics", "PHI": "Philadelphia Phillies",
        "PIT": "Pittsburgh Pirates", "SD": "San Diego Padres", "SEA": "Seattle Mariners",
        "SF": "San Francisco Giants", "STL": "St. Louis Cardinals", "TB": "Tampa Bay Rays",
        "TEX": "Texas Rangers", "TOR": "Toronto Blue Jays", "WAS": "Washington Nationals",
    }
    TRADES_TEAM_ORDER = list(ABBR_TO_FULL.keys())

admin_bp = Blueprint("admin", __name__)

ADMIN_PASSWORD = os.environ.get("BNSL_ADMIN_PASSWORD", "bnsladminpass")
ADMIN_SESSION_KEY = "bnsl_admin_authed"
CUSTOM_WAIVER_META_KEY = "next_waiver_run_at"
ROSTER_LOCK_META_KEY = "roster_locked"
CONTRACT_TYPES = ["R", "A", "FA", "X"]
ROSTER_STATUS_TYPES = ["Reserve", "40-man", "Active"]

TEAM_ABBRS = [t for t in (TRADES_TEAM_ORDER or CONFIG_TEAM_ABBRS or sorted(ABBR_TO_FULL)) if t]
TEAM_ABBRS = ["CHW" if t == "CWS" else "WAS" if t == "WSH" else t for t in TEAM_ABBRS]
TEAM_ABBRS = list(dict.fromkeys(TEAM_ABBRS))
def canonical_team_abbr(team: Any) -> str:
    code = config_canonical_team_abbr(team)
    code = "CHW" if code == "CWS" else "WAS" if code == "WSH" else code
    return code if code in TEAM_ABBRS else code


def display_team(abbr: str | None) -> str:
    code = canonical_team_abbr(abbr)
    return ABBR_TO_FULL.get(code, code or "")


def team_label(abbr: str | None) -> str:
    code = canonical_team_abbr(abbr)
    full = display_team(code)
    return f"{code} — {full}" if full and full != code else code


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def get_admin_db_path() -> Path:
    cfg = current_app.config.get("ADMIN_DB_PATH")
    return Path(cfg) if cfg else db_path("admin.db")


def get_admin_conn() -> sqlite3.Connection:
    path = get_admin_db_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(path), timeout=30)
    conn.execute("PRAGMA busy_timeout=30000")
    conn.row_factory = sqlite3.Row
    ensure_admin_schema(conn)
    return conn


def ensure_admin_schema(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS admin_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            created_at TEXT NOT NULL,
            action TEXT NOT NULL,
            actor TEXT,
            summary TEXT NOT NULL,
            payload_json TEXT
        )
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_admin_log_created ON admin_log(created_at DESC)")
    conn.commit()


def bootstrap_admin() -> None:
    conn = get_admin_conn()
    conn.close()


def log_action(action: str, summary: str, payload: dict[str, Any] | None = None) -> int:
    conn = get_admin_conn()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO admin_log(created_at, action, actor, summary, payload_json)
        VALUES (?, ?, ?, ?, ?)
        """,
        (
            now_iso(),
            action,
            "admin",
            summary,
            json.dumps(payload or {}, sort_keys=True, default=str),
        ),
    )
    conn.commit()
    log_id = int(cur.lastrowid)
    conn.close()
    return log_id


def admin_logs(limit: int = 200) -> list[dict[str, Any]]:
    conn = get_admin_conn()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT id, created_at, action, actor, summary, payload_json
        FROM admin_log
        ORDER BY id DESC
        LIMIT ?
        """,
        (int(limit),),
    )
    rows = [dict(r) for r in cur.fetchall()]
    conn.close()
    return rows


def is_admin_authed() -> bool:
    return bool(session.get(ADMIN_SESSION_KEY))


def require_admin() -> None:
    if not is_admin_authed():
        abort(401, "Admin login required")


def redirect_with(
    message: str | None = None,
    error: str | None = None,
    refresh: str | list[str] | tuple[str, ...] | None = None,
):
    args: dict[str, str] = {}
    if message:
        args["msg"] = message
    if error:
        args["err"] = error
    if refresh:
        if isinstance(refresh, str):
            args["refresh"] = refresh
        else:
            args["refresh"] = ",".join(str(x) for x in refresh if str(x).strip())
    return redirect(url_for("admin.index", **args))


def get_roster_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(current_app.config["ROSTER_DB_PATH"], timeout=30)
    conn.execute("PRAGMA busy_timeout=30000")
    conn.row_factory = sqlite3.Row

    def _unaccent(s: Any) -> str:
        if s is None:
            return ""
        return "".join(
            ch for ch in unicodedata.normalize("NFKD", str(s))
            if not unicodedata.combining(ch)
        )
    conn.create_function("unaccent", 1, _unaccent)
    return conn




def _ootp27_dir() -> Path:
    """Directory containing fixed OOTP27 input files for the BNSL import."""
    cfg = current_app.config.get("OOTP27_DIR") or current_app.config.get("BNSL_OOTP27_DIR")
    if cfg:
        return Path(cfg)
    roster_db = Path(current_app.config["ROSTER_DB_PATH"])
    return roster_db.parent / "ootp27"


def _require_ootp_input(path: Path, label: str) -> None:
    if not path.exists():
        raise FileNotFoundError(f"{label} not found at {path}")
    if not path.is_file():
        raise FileNotFoundError(f"{label} is not a regular file: {path}")


def _write_roster_players_export_csv(output_path: Path) -> dict[str, Any]:
    """
    Write the same raw roster_players CSV produced by the roster app's
    /roster/export.csv endpoint, but to a server-side temporary path.
    """
    conn = get_roster_conn()
    try:
        try:
            from roster_app import ensure_roster_identifier_columns
            ensure_roster_identifier_columns(conn)
            conn.commit()
        except Exception:
            current_app.logger.exception("Could not ensure roster identifier columns before OOTP export")
            raise

        cur = conn.cursor()
        cur.execute("PRAGMA table_info(roster_players)")
        columns = [row[1] for row in cur.fetchall()]
        if not columns:
            raise RuntimeError("roster_players table was not found in roster.db")

        order_clause = " ORDER BY id" if "id" in columns else ""
        cur.execute(f"SELECT * FROM roster_players{order_clause}")
        rows = cur.fetchall()

        with output_path.open("w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(columns)
            for row in rows:
                writer.writerow([row[col] for col in columns])

        return {"row_count": len(rows), "columns": columns}
    finally:
        conn.close()


def _build_bnsl_ootp_import_download() -> dict[str, Any]:
    """Run bnsl_ootp_roster_import.py from fixed server-side inputs."""
    ootp_dir = _ootp27_dir()
    input_id_map = ootp_dir / "bns_ootp_id_map.csv"
    ootp_export = ootp_dir / "mlb_rosters.csv"
    league_structure = ootp_dir / "league_structure.xml"

    _require_ootp_input(input_id_map, "BNS/OOTP id map")
    _require_ootp_input(ootp_export, "OOTP roster export")
    _require_ootp_input(league_structure, "OOTP league structure XML")

    try:
        from bnsl_ootp_roster_import import main as ootp_import_main
    except Exception as exc:
        raise RuntimeError(
            "Could not import bnsl_ootp_roster_import.py. "
            "Add that script to the same GitHub repo/deploy as admin_app.py."
        ) from exc

    with tempfile.TemporaryDirectory(prefix="bnsl_ootp_import_") as tmpdir_text:
        tmpdir = Path(tmpdir_text)
        bnsl_export = tmpdir / "roster_players.csv"
        output_csv = tmpdir / "ootp_player_import_updated.csv"
        id_map_output = tmpdir / "bns_ootp_id_map_generated.csv"
        audit_output = tmpdir / "bns_ootp_audit_report.csv"

        export_info = _write_roster_players_export_csv(bnsl_export)

        argv = [
            "--use-id-map",
            "--input-id-map", str(input_id_map),
            "--ootp-export", str(ootp_export),
            "--bnsl-export", str(bnsl_export),
            "--league-structure", str(league_structure),
            "--output", str(output_csv),
            "--id-map-output", str(id_map_output),
            "--audit-output", str(audit_output),
        ]

        stdout = io.StringIO()
        stderr = io.StringIO()
        with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
            return_code = ootp_import_main(argv)

        if return_code not in (0, None):
            raise RuntimeError(f"BNSL→OOTP importer exited with code {return_code}: {stderr.getvalue() or stdout.getvalue()}")
        if not output_csv.exists():
            raise RuntimeError("BNSL→OOTP importer finished but did not create the OOTP import CSV")

        audit_rows = 0
        if audit_output.exists():
            with audit_output.open("r", newline="", encoding="utf-8", errors="replace") as f:
                audit_rows = max(sum(1 for _ in f) - 1, 0)

        stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        return {
            "filename": f"ootp_player_import_updated_{stamp}.csv",
            "data": output_csv.read_bytes(),
            "row_count": export_info["row_count"],
            "audit_rows": audit_rows,
            "stdout": stdout.getvalue(),
            "stderr": stderr.getvalue(),
            "ootp_dir": str(ootp_dir),
        }

def get_draft_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(current_app.config["DRAFT_DB_PATH"], timeout=30)
    conn.execute("PRAGMA busy_timeout=30000")
    conn.row_factory = sqlite3.Row
    return conn


def get_rulev_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(current_app.config["RULEV_DB_PATH"], timeout=30)
    conn.execute("PRAGMA busy_timeout=30000")
    conn.row_factory = sqlite3.Row
    return conn


def _parse_admin_draft_datetime(date_text: str, time_text: str) -> datetime:
    from draft_order_page import EASTERN, validate_regular_pick_time
    try:
        d = datetime.strptime((date_text or "").strip(), "%Y-%m-%d").date()
    except Exception:
        raise ValueError("Draft pick date must use YYYY-MM-DD")
    try:
        t = datetime.strptime((time_text or "09:00").strip(), "%H:%M").time()
    except Exception:
        raise ValueError("Draft pick time must use HH:MM")
    return validate_regular_pick_time(datetime.combine(d, t, tzinfo=EASTERN))


def _pick_label(row: sqlite3.Row, *, rulev: bool = False) -> str:
    if not rulev:
        label = str(row["label"] or "").strip() if "label" in row.keys() else ""
        if label:
            return label
    return f"{int(row['round'])}.{int(row['pick']):02d}"


def _ensure_draft_skip_schema(conn: sqlite3.Connection) -> None:
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS pick_overrides (
            draft_order_id INTEGER PRIMARY KEY,
            scheduled_time TEXT NOT NULL,
            missed INTEGER DEFAULT 0,
            skipped_at TEXT
        )
    """)
    cur.execute("PRAGMA table_info(pick_overrides)")
    cols = {r[1] for r in cur.fetchall()}
    if "missed" not in cols:
        cur.execute("ALTER TABLE pick_overrides ADD COLUMN missed INTEGER DEFAULT 0")
    if "skipped_at" not in cols:
        cur.execute("ALTER TABLE pick_overrides ADD COLUMN skipped_at TEXT")
    conn.commit()


def _ensure_rulev_skip_schema(conn: sqlite3.Connection) -> None:
    cur = conn.cursor()
    cur.execute("""
      CREATE TABLE IF NOT EXISTS rulev_pick_miss_state (
        rulev_order_id INTEGER PRIMARY KEY,
        first_missed_at TEXT,
        rescheduled_time TEXT,
        skipped_at TEXT
      )
    """)
    conn.commit()


def _draft_pick_choices() -> dict[str, list[dict[str, str]]]:
    choices: dict[str, list[dict[str, str]]] = {"draft": [], "rulev": []}

    try:
        conn = get_draft_conn()
        _ensure_draft_skip_schema(conn)
        cur = conn.cursor()
        cur.execute("""
            SELECT d.id, d.round, d.pick, d.team, d.player_id, d.drafted_at, d.label, po.skipped_at
            FROM draft_order d
            LEFT JOIN pick_overrides po ON po.draft_order_id = d.id
            ORDER BY d.round ASC, d.pick ASC
        """)
        for row in cur.fetchall():
            if row["player_id"]:
                status = "selected"
            elif row["skipped_at"]:
                status = "skipped"
            else:
                status = "open"
            label = _pick_label(row)
            choices["draft"].append({
                "value": f"{int(row['round'])}|{int(row['pick'])}",
                "label": f"{label} — {row['team']} ({status})",
            })
        conn.close()
    except Exception:
        current_app.logger.exception("Unable to load amateur draft pick choices for admin panel")

    try:
        conn = get_rulev_conn()
        _ensure_rulev_skip_schema(conn)
        cur = conn.cursor()
        cur.execute("""
            SELECT o.id, o.round, o.pick, o.team, o.player_id, o.drafted_at, ms.skipped_at
            FROM rulev_order o
            LEFT JOIN rulev_pick_miss_state ms ON ms.rulev_order_id = o.id
            ORDER BY o.round ASC, o.pick ASC
        """)
        for row in cur.fetchall():
            if row["player_id"]:
                status = "selected"
            elif row["skipped_at"]:
                status = "skipped"
            else:
                status = "open"
            label = _pick_label(row, rulev=True)
            choices["rulev"].append({
                "value": f"{int(row['round'])}|{int(row['pick'])}",
                "label": f"{label} — {row['team']} ({status})",
            })
        conn.close()
    except Exception:
        current_app.logger.exception("Unable to load Rule V pick choices for admin panel")

    return choices


def _parse_pick_key(value: Any) -> tuple[int, int]:
    parts = str(value or "").split("|", 1)
    if len(parts) != 2:
        raise ValueError("Select a valid draft pick")
    try:
        round_num = int(parts[0])
        pick_num = int(parts[1])
    except Exception:
        raise ValueError("Select a valid draft pick")
    if round_num <= 0 or pick_num <= 0:
        raise ValueError("Select a valid draft pick")
    return round_num, pick_num



def _parse_money(value: Any) -> float:
    text = str(value or "").strip().replace("$", "").replace(",", "")
    if not text:
        raise ValueError("Amount is required")
    amount = float(text)
    if amount == 0:
        raise ValueError("Amount cannot be zero")
    return amount


def _parse_effective_date(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return date.today().isoformat()
    try:
        return datetime.strptime(text, "%Y-%m-%d").date().isoformat()
    except Exception:
        raise ValueError("Effective date must use YYYY-MM-DD")



def _parse_waiver_datetime(date_text: str, time_text: str) -> datetime:
    from waivers_app import EASTERN
    try:
        d = datetime.strptime((date_text or "").strip(), "%Y-%m-%d").date()
    except Exception:
        raise ValueError("Waiver date must use YYYY-MM-DD")
    try:
        t = datetime.strptime((time_text or "12:00").strip(), "%H:%M").time()
    except Exception:
        raise ValueError("Waiver time must use HH:MM")
    dt = datetime.combine(d, t, tzinfo=EASTERN)
    return dt.astimezone(timezone.utc)


def _ensure_roster_meta(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS app_meta (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        )
        """
    )


def _get_roster_meta(key: str, default: str = "") -> str:
    conn = get_roster_conn()
    _ensure_roster_meta(conn)
    cur = conn.cursor()
    cur.execute("SELECT value FROM app_meta WHERE key=?", (key,))
    row = cur.fetchone()
    conn.close()
    return str(row["value"] if row else default)


def _set_roster_meta(key: str, value: Any) -> None:
    conn = get_roster_conn()
    _ensure_roster_meta(conn)
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


def _is_roster_locked() -> bool:
    return _get_roster_meta(ROSTER_LOCK_META_KEY, "0").strip().lower() in {"1", "true", "yes", "on"}


def _set_roster_locked(locked: bool) -> bool:
    _set_roster_meta(ROSTER_LOCK_META_KEY, "1" if locked else "0")
    return locked


def _set_custom_waiver_date(run_at_utc: datetime) -> dict[str, Any]:
    from waivers_app import ensure_waiver_schema, iso

    conn = get_roster_conn()
    ensure_waiver_schema(conn)
    _ensure_roster_meta(conn)
    run_iso = iso(run_at_utc)
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO app_meta(key, value)
        VALUES (?, ?)
        ON CONFLICT(key) DO UPDATE SET value=excluded.value
        """,
        (CUSTOM_WAIVER_META_KEY, run_iso),
    )
    cur.execute("""
        UPDATE waiver_entries
        SET run_at=?
        WHERE status='active' OR processed_at IS NULL
    """, (run_iso,))
    active_updated = int(cur.rowcount or 0)
    conn.commit()
    conn.close()
    return {"run_at": run_iso, "active_waivers_updated": active_updated}


def _parse_nonnegative_int(value: Any, *, field: str, default: int = 0) -> int:
    text = str(value if value is not None else "").strip()
    if text == "":
        return default
    try:
        out = int(float(text))
    except Exception:
        raise ValueError(f"{field} must be a whole number")
    if out < 0:
        raise ValueError(f"{field} cannot be negative")
    return out


def _parse_nonnegative_money(value: Any, *, field: str = "Salary", default: float = 0.0) -> float:
    text = str(value if value is not None else "").strip().replace("$", "").replace(",", "")
    if text == "":
        return float(default)
    try:
        out = float(text)
    except Exception:
        raise ValueError(f"{field} must be numeric")
    if out < 0:
        raise ValueError(f"{field} cannot be negative")
    return out


def _parse_nonnegative_float(value: Any, *, field: str, default: float = 0.0) -> float:
    text = str(value if value is not None else "").strip().replace(",", "")
    if text == "":
        return float(default)
    try:
        out = float(text)
    except Exception:
        raise ValueError(f"{field} must be numeric")
    if out < 0:
        raise ValueError(f"{field} cannot be negative")
    return out


def _bool_from_form(value: Any) -> int:
    return 1 if str(value or "").strip().lower() in {"1", "true", "yes", "y", "on"} else 0


def _normalize_roster_status(value: Any) -> str:
    raw = str(value or "").strip()
    lookup = {"reserve": "Reserve", "40-man": "40-man", "40man": "40-man", "active": "Active"}
    normalized = lookup.get(raw.lower(), raw)
    if normalized not in ROSTER_STATUS_TYPES:
        raise ValueError("Roster status must be Reserve, 40-man, or Active")
    return normalized


def _normalize_contract_type(value: Any) -> str:
    ctype = str(value or "").strip().upper()
    if ctype not in CONTRACT_TYPES:
        raise ValueError("Contract type must be R, A, FA, or X")
    return ctype


def _normalize_contract_expires(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    if re.match(r"^20\d{2}(-\d{2}-\d{2})?$", text):
        return text
    raise ValueError("Year of expiry must be blank, YYYY, or YYYY-MM-DD")


def _compute_fa_class(row: sqlite3.Row, updates: dict[str, Any]) -> str:
    try:
        from roster_app import fa_class_from_contract_parts
        def val(name: str) -> Any:
            return updates[name] if name in updates else (row[name] if name in row.keys() else None)
        return fa_class_from_contract_parts(
            val("contract_type"),
            val("contract_expires"),
            val("contract_initial_season"),
            val("contract_length"),
            val("contract_option"),
            val("service_time"),
            val("previous_service_time"),
            val("service_time_2025"),
        )
    except Exception:
        return str(row["fa_class"] if "fa_class" in row.keys() else "")


def _update_draft_source_franchise(row: sqlite3.Row, team: str) -> int:
    draft_player_id = row["draft_player_id"] if "draft_player_id" in row.keys() else None
    if not draft_player_id:
        return 0
    updated = 0
    try:
        dconn = get_draft_conn()
        dcur = dconn.cursor()
        dcur.execute("UPDATE draft_order SET team=? WHERE player_id=?", (team, int(draft_player_id)))
        updated += int(dcur.rowcount or 0)
        dcur.execute("UPDATE players SET franchise=? WHERE id=?", (team, int(draft_player_id)))
        updated += int(dcur.rowcount or 0)
        dconn.commit()
        dconn.close()
    except Exception:
        current_app.logger.exception("Admin player update could not update draft source rows")
    return updated


def _update_active_waiver_snapshot(player_id: int, updates: dict[str, Any]) -> int:
    waiver_cols = {"salary", "contract_type", "contract_option", "contract_expires", "options_remaining"}
    relevant = {k: v for k, v in updates.items() if k in waiver_cols}
    if "roster_status" in updates:
        relevant["pre_waiver_status"] = updates["roster_status"]
    if not relevant:
        return 0
    assignments = ", ".join(f"{k}=?" for k in relevant)
    try:
        conn = get_roster_conn()
        cur = conn.cursor()
        cur.execute(
            f"UPDATE waiver_entries SET {assignments} WHERE player_id=? AND status='active'",
            (*relevant.values(), int(player_id)),
        )
        changed = int(cur.rowcount or 0)
        conn.commit()
        conn.close()
        return changed
    except Exception:
        current_app.logger.exception("Admin player update could not update active waiver snapshot")
        return 0


def _update_player_fields(player_id: int, updates: dict[str, Any]) -> dict[str, Any]:
    conn = get_roster_conn()
    cur = conn.cursor()
    cur.execute("SELECT * FROM roster_players WHERE id=?", (int(player_id),))
    row = cur.fetchone()
    if not row:
        conn.close()
        raise ValueError("Player not found in roster_players")

    old = {k: row[k] for k in row.keys()}
    clean: dict[str, Any] = {}

    if "franchise" in updates:
        team = canonical_team_abbr(updates.get("franchise"))
        if team and team not in TEAM_ABBRS:
            raise ValueError("Unknown destination franchise")
        clean["franchise"] = team
        clean["signed"] = 1 if team else 0

    release_to_fa = "franchise" in clean and not clean["franchise"]

    if "contract_type" in updates:
        # A blank contract type is only valid when explicitly releasing the
        # player to the unrostered/FA pool.  Otherwise keep the normal admin
        # validation strict.
        if release_to_fa and not str(updates.get("contract_type") or "").strip():
            clean["contract_type"] = ""
        else:
            clean["contract_type"] = _normalize_contract_type(updates.get("contract_type"))
    if "salary" in updates:
        clean["salary"] = _parse_nonnegative_money(updates.get("salary"), default=float(row["salary"] or 0.0))
    if "service_time" in updates:
        clean["service_time"] = _parse_nonnegative_float(updates.get("service_time"), field="Service time", default=float(row["service_time"] or 0.0))
    if "options_remaining" in updates:
        clean["options_remaining"] = _parse_nonnegative_int(updates.get("options_remaining"), field="Player options", default=int(row["options_remaining"] or 0))
    if "contract_option" in updates:
        clean["contract_option"] = _bool_from_form(updates.get("contract_option"))
    if "contract_expires" in updates:
        clean["contract_expires"] = _normalize_contract_expires(updates.get("contract_expires"))
    if "roster_status" in updates:
        status = _normalize_roster_status(updates.get("roster_status"))
        clean["roster_status"] = status
        clean["active_roster"] = 1 if status == "Active" else 0

    # Empty franchise means explicitly unrostered/free agent.  Treat this like
    # a release, not just a franchise edit: remove team ownership, clear salary
    # and contract metadata, clear option fields, and keep the player off any
    # active roster counts.
    if release_to_fa:
        clean["signed"] = 0
        if "active_roster" in row.keys():
            clean["active_roster"] = 0
        if "roster_status" in row.keys():
            clean["roster_status"] = "Reserve"
        if "contract_type" in row.keys():
            clean["contract_type"] = ""
        if "salary" in row.keys():
            clean["salary"] = 0.0
        if "contract_initial_season" in row.keys():
            clean["contract_initial_season"] = None
        if "contract_length" in row.keys():
            clean["contract_length"] = None
        if "contract_option" in row.keys():
            clean["contract_option"] = 0
        if "contract_expires" in row.keys():
            clean["contract_expires"] = ""
        if "options_remaining" in row.keys():
            clean["options_remaining"] = 0
        if "option_decision" in row.keys():
            clean["option_decision"] = ""
        if "option_decision_at" in row.keys():
            clean["option_decision_at"] = ""
        if "fa_class" in row.keys():
            clean["fa_class"] = ""

    if not clean:
        conn.close()
        raise ValueError("No player fields were supplied")

    if "fa_class" in row.keys() and not release_to_fa:
        clean["fa_class"] = _compute_fa_class(row, clean)

    assignments = ", ".join(f"{field}=?" for field in clean)
    cur.execute(
        f"UPDATE roster_players SET {assignments} WHERE id=?",
        (*clean.values(), int(player_id)),
    )
    conn.commit()
    conn.close()

    draft_rows_updated = 0
    if "franchise" in clean:
        draft_rows_updated = _update_draft_source_franchise(row, clean["franchise"])

    waiver_rows_updated = _update_active_waiver_snapshot(player_id, clean)

    try:
        from roster_app import sync_after_roster_mutation
        sync_after_roster_mutation(player_id=player_id, sync_fa=True, sync_rulev=False)
    except Exception:
        current_app.logger.exception("Admin player update sync failed")

    changed = {
        key: {"old": old.get(key), "new": value}
        for key, value in clean.items()
        if key not in {"active_roster", "signed", "fa_class"} and str(old.get(key) or "") != str(value or "")
    }

    return {
        "player_id": int(player_id),
        "player_name": row["name"],
        "old_team": canonical_team_abbr(old.get("franchise") or ""),
        "new_team": canonical_team_abbr(clean.get("franchise", old.get("franchise") or "")),
        "changes": changed,
        "applied_fields": clean,
        "draft_rows_updated": draft_rows_updated,
        "active_waiver_rows_updated": waiver_rows_updated,
    }


def _change_player_franchise(player_id: int, new_team: str) -> dict[str, Any]:
    return _update_player_fields(player_id, {"franchise": new_team})


def _search_roster_players(q: str, limit: int = 25) -> list[dict[str, Any]]:
    q = (q or "").strip()
    conn = get_roster_conn()
    cur = conn.cursor()
    params: list[Any] = []
    where = ""
    if q:
        normalized = "".join(
            ch for ch in unicodedata.normalize("NFKD", q.lower())
            if not unicodedata.combining(ch)
        )
        where = "WHERE LOWER(unaccent(name)) LIKE ? OR CAST(id AS TEXT)=?"
        params.extend([f"%{normalized}%", q])
    cur.execute(
        f"""
        SELECT
            id, name, position, franchise, roster_status, contract_type, salary,
            service_time, options_remaining, contract_option, contract_expires, fa_class
        FROM roster_players
        {where}
        ORDER BY unaccent(name) COLLATE NOCASE ASC
        LIMIT ?
        """,
        (*params, int(limit)),
    )
    rows = [dict(r) for r in cur.fetchall()]
    conn.close()
    return rows

def _fa_locked() -> bool:
    try:
        from fa_app import is_fa_locked
        return is_fa_locked()
    except Exception:
        current_app.logger.exception("Unable to read FA lock state")
        return False


def _search_fa_players(q: str, limit: int = 25) -> list[dict[str, Any]]:
    """Substring search over unsigned free agents for the admin QO form."""
    q = (q or "").strip()
    if not q or (len(q) < 2 and not q.isdigit()):
        return []
    normalized = "".join(
        ch for ch in unicodedata.normalize("NFKD", q.lower())
        if not unicodedata.combining(ch)
    )
    try:
        from fa_app import ensure_fa_blacklist_schema, get_conn as get_fa_conn
        ensure_fa_blacklist_schema()
        conn = get_fa_conn()
        cur = conn.cursor()
        cur.execute(
            """
            SELECT
              p.id, p.name, p.position, p.last_team,
              b.team AS current_bid_team, b.aav_m AS current_aav_m
            FROM free_agents p
            LEFT JOIN bids b ON b.player_id=p.id AND b.status='ACTIVE'
            WHERE (COALESCE(p.is_roster_unrostered,0)=1 OR p.roster_player_id IS NULL)
              AND COALESCE(p.is_blacklisted,0)=0
              AND (p.signed_team IS NULL OR p.signed_team='')
              AND (
                LOWER(unaccent(p.name)) LIKE ?
                OR CAST(p.id AS TEXT)=?
              )
            ORDER BY
              CASE
                WHEN LOWER(unaccent(p.name))=? THEN 0
                WHEN LOWER(unaccent(p.name)) LIKE ? THEN 1
                ELSE 2
              END,
              unaccent(p.name) COLLATE NOCASE ASC
            LIMIT ?
            """,
            (
                f"%{normalized}%",
                q,
                normalized,
                f"{normalized}%",
                int(limit),
            ),
        )
        rows = [dict(r) for r in cur.fetchall()]
        conn.close()
        return rows
    except Exception:
        current_app.logger.exception("Unable to search FA choices for admin QO form")
        return []


def _search_fa_bid_players(q: str, limit: int = 25) -> list[dict[str, Any]]:
    """Substring search over unsigned free agents that currently have an active bid."""
    q = (q or "").strip()
    if not q or (len(q) < 2 and not q.isdigit()):
        return []
    normalized = "".join(
        ch for ch in unicodedata.normalize("NFKD", q.lower())
        if not unicodedata.combining(ch)
    )
    try:
        from fa_app import ensure_fa_blacklist_schema, get_conn as get_fa_conn
        ensure_fa_blacklist_schema()
        conn = get_fa_conn()
        cur = conn.cursor()
        cur.execute(
            """
            SELECT
              p.id, p.name, p.position, p.last_team,
              b.id AS current_bid_id,
              b.team AS current_bid_team,
              b.years AS current_years,
              b.has_option AS current_has_option,
              b.aav_m AS current_aav_m,
              b.bid_value_m AS current_bid_value_m,
              b.created_at AS current_created_at,
              b.expires_at AS current_expires_at,
              (
                SELECT pb.team
                FROM bids pb
                WHERE pb.player_id=p.id AND pb.status='OUTBID' AND pb.id<>b.id
                ORDER BY datetime(pb.created_at) DESC, pb.id DESC
                LIMIT 1
              ) AS previous_bid_team,
              (
                SELECT pb.aav_m
                FROM bids pb
                WHERE pb.player_id=p.id AND pb.status='OUTBID' AND pb.id<>b.id
                ORDER BY datetime(pb.created_at) DESC, pb.id DESC
                LIMIT 1
              ) AS previous_aav_m,
              (
                SELECT pb.bid_value_m
                FROM bids pb
                WHERE pb.player_id=p.id AND pb.status='OUTBID' AND pb.id<>b.id
                ORDER BY datetime(pb.created_at) DESC, pb.id DESC
                LIMIT 1
              ) AS previous_bid_value_m
            FROM free_agents p
            JOIN bids b ON b.player_id=p.id AND b.status='ACTIVE'
            WHERE (p.signed_team IS NULL OR p.signed_team='')
              AND COALESCE(p.is_blacklisted,0)=0
              AND (
                LOWER(unaccent(p.name)) LIKE ?
                OR CAST(p.id AS TEXT)=?
              )
            ORDER BY
              CASE
                WHEN LOWER(unaccent(p.name))=? THEN 0
                WHEN LOWER(unaccent(p.name)) LIKE ? THEN 1
                ELSE 2
              END,
              unaccent(p.name) COLLATE NOCASE ASC
            LIMIT ?
            """,
            (
                f"%{normalized}%",
                q,
                normalized,
                f"{normalized}%",
                int(limit),
            ),
        )
        rows = [dict(r) for r in cur.fetchall()]
        conn.close()
        return rows
    except Exception:
        current_app.logger.exception("Unable to search active FA bid choices for admin reset form")
        return []

def _search_fa_blacklist_candidates(q: str, limit: int = 25) -> list[dict[str, Any]]:
    """Substring search over unsigned, unblacklisted FA rows for the admin blacklist form."""
    q = (q or "").strip()
    if not q or (len(q) < 2 and not q.isdigit()):
        return []
    normalized = "".join(
        ch for ch in unicodedata.normalize("NFKD", q.lower())
        if not unicodedata.combining(ch)
    )
    try:
        from fa_app import ensure_fa_blacklist_schema, get_conn as get_fa_conn
        ensure_fa_blacklist_schema()
        conn = get_fa_conn()
        cur = conn.cursor()
        cur.execute(
            """
            SELECT
              p.id, p.name, p.position, p.last_team,
              b.team AS current_bid_team, b.aav_m AS current_aav_m
            FROM free_agents p
            LEFT JOIN bids b ON b.player_id=p.id AND b.status='ACTIVE'
            WHERE (COALESCE(p.is_roster_unrostered,0)=1 OR p.roster_player_id IS NULL)
              AND COALESCE(p.is_blacklisted,0)=0
              AND (p.signed_team IS NULL OR p.signed_team='')
              AND (
                LOWER(unaccent(p.name)) LIKE ?
                OR CAST(p.id AS TEXT)=?
              )
            ORDER BY
              CASE
                WHEN LOWER(unaccent(p.name))=? THEN 0
                WHEN LOWER(unaccent(p.name)) LIKE ? THEN 1
                ELSE 2
              END,
              unaccent(p.name) COLLATE NOCASE ASC
            LIMIT ?
            """,
            (
                f"%{normalized}%",
                q,
                normalized,
                f"{normalized}%",
                int(limit),
            ),
        )
        rows = [dict(r) for r in cur.fetchall()]
        conn.close()
        return rows
    except Exception:
        current_app.logger.exception("Unable to search FA blacklist candidates")
        return []


def _active_blacklisted_fa_players(limit: int = 100) -> list[dict[str, Any]]:
    try:
        from fa_app import list_blacklisted_free_agents
        return list_blacklisted_free_agents(limit=limit)
    except Exception:
        current_app.logger.exception("Unable to load FA blacklist rows")
        return []


ADMIN_HTML = """
<!doctype html>
<html>
<head>
  <base href="/admin/">
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>BNSL Admin</title>
  __BNSL_GAME_CSS__
  <style>
    .wrap { max-width: 1500px; margin: 0 auto; padding: 18px; }
    .grid { display:grid; grid-template-columns: repeat(2, minmax(320px, 1fr)); gap: 14px; align-items:start; }
    .card { background: rgba(0,0,0,.18); border: 1px solid rgba(140,170,255,.16); border-radius: 18px; padding: 14px; }
    .controls { display:flex; flex-wrap:wrap; gap:10px; align-items:end; }
    label { display:block; font-size: 13px; opacity: .92; }
    input, select, textarea { width: 100%; box-sizing: border-box; margin-top: 5px; }
    textarea { min-height: 180px; font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace; }
    .row { display:grid; grid-template-columns: repeat(2, minmax(140px, 1fr)); gap:10px; }
    .row3 { display:grid; grid-template-columns: repeat(3, minmax(120px, 1fr)); gap:10px; }
    .msg { margin: 0 0 12px; border-radius: 14px; padding:10px 12px; border:1px solid rgba(140,170,255,.18); }
    .ok { color: var(--good); background: rgba(54,249,162,.07); }
    .err { color: var(--warn); background: rgba(255,77,109,.07); }
    .muted { opacity: .72; }
    table { width:100%; border-collapse: collapse; }
    th, td { padding:8px 10px; border-bottom:1px solid rgba(255,255,255,.08); text-align:left; vertical-align:top; }
    .search-results button { margin-top: 4px; }
    .wide { grid-column: 1 / -1; }
    @media(max-width: 900px){ .grid,.row,.row3{grid-template-columns:1fr;} }
  </style>
</head>
<body>
  <div class="wrap">
    <div class="brand">
      <div>
        <h1>ADMIN PANEL</h1>
        <div class="sub">Bonus payments, roster corrections, waiver scheduling, and audit log.</div>
      </div>
      <div class="right">
        {% if authed %}<form method="post" action="logout"><button class="btn" type="submit">Logout</button></form>{% endif %}
        <span class="badge">ADMIN</span>
      </div>
    </div>

    {% if msg %}<div class="msg ok">{{ msg }}</div>{% endif %}
    {% if err %}<div class="msg err">{{ err }}</div>{% endif %}

    {% if not authed %}
      <div class="panel pad" style="max-width:520px;">
        <h2>Admin Login</h2>
        <form method="post" action="login">
          <label>Password<input name="password" type="password" autofocus></label>
          <div style="margin-top:12px;"><button class="btn primary" type="submit">Login</button></div>
        </form>
      </div>
    {% else %}
      <div class="grid">
        <div class="card">
          <h2>Bonus Payment</h2>
          <p class="muted">Positive amounts add revenue/cap space to the selected team. Negative amounts remove it. These post as <b>admin_bonus</b> payments.</p>
          <form method="post" action="bonus-payment">
            <div class="row3">
              <label>Team
                <select name="team">{% for t in teams %}<option value="{{ t }}">{{ team_label(t) }}</option>{% endfor %}</select>
              </label>
              <label>Amount
                <input name="amount" inputmode="decimal" placeholder="250000">
              </label>
              <label>Effective date
                <input name="effective_date" type="date" value="{{ today }}">
              </label>
            </div>
            <label style="margin-top:10px;">Description
              <input name="description" placeholder="Admin revenue correction">
            </label>
            <div style="margin-top:12px;"><button class="btn primary" type="submit">Post bonus</button></div>
          </form>
        </div>

        <div class="card">
          <h2>Roster Lock</h2>
          <p class="muted">When locked, roster-page transactions are blocked: status moves, releases, options, and arbitration decisions. Admin edits still work here.</p>
          <form method="post" action="toggle-roster-lock">
            <input type="hidden" name="locked" value="{{ '0' if roster_locked else '1' }}">
            <div class="pill" style="display:inline-block; margin-bottom:10px;">Status: <b>{{ 'LOCKED' if roster_locked else 'UNLOCKED' }}</b></div>
            <div><button class="btn {{ '' if roster_locked else 'primary' }}" type="submit">{{ 'Unlock roster' if roster_locked else 'Lock roster' }}</button></div>
          </form>
        </div>

        <div class="card">
          <h2>Free Agency Lock</h2>
          <p class="muted">When locked, teams cannot submit FA bids from the FA page and active bids/QOs have no signing deadline. Unlocking FA gives every active unsigned bid a fresh 48-hour clock.</p>
          <form method="post" action="toggle-fa-lock">
            <input type="hidden" name="locked" value="{{ '0' if fa_locked else '1' }}">
            <div class="pill" style="display:inline-block; margin-bottom:10px;">Status: <b>{{ 'LOCKED' if fa_locked else 'UNLOCKED' }}</b></div>
            <div><button class="btn {{ '' if fa_locked else 'primary' }}" type="submit">{{ 'Unlock FA' if fa_locked else 'Lock FA' }}</button></div>
          </form>
        </div>

        <div class="card">
          <h2>Manual Syncs</h2>
          <p class="muted">These are intentionally not run during normal page loads or search. Use them only when you want a full maintenance refresh.</p>
          <div class="controls">
            <form method="post" action="sync-draft-roster" onsubmit="return confirm('Sync completed draft picks into roster.db now?');">
              <button class="btn" type="submit">Sync draft → roster</button>
            </form>
            <form method="post" action="sync-rulev-roster" onsubmit="return confirm('Refresh Rule V eligible pool from roster.db now?');">
              <button class="btn" type="submit">Sync Rule V pool</button>
            </form>
            <form method="post" action="sync-fa-roster" onsubmit="return confirm('Run full FA maintenance sync, including OOTP/rating imports?');">
              <button class="btn" type="submit">Full FA maintenance sync</button>
            </form>
          </div>
        </div>

        <div class="card">
          <h2>OOTP27 Roster Import</h2>
          <p class="muted">Builds an OOTP player import CSV from the live <b>roster.db</b> table plus the fixed files in <b>{{ ootp27_dir }}</b>: <code>bns_ootp_id_map.csv</code>, <code>mlb_rosters.csv</code>, and <code>league_structure.xml</code>.</p>
          <form method="post" action="generate-ootp-import" onsubmit="return confirm('Generate a fresh OOTP import CSV from the current roster.db?');">
            <button class="btn primary" type="submit">Download BNSL → OOTP import CSV</button>
          </form>
        </div>

        <div class="card">
          <h2>Set QO</h2>
          <p class="muted">Creates a standard one-year qualifying-offer FA bid at <b>$22.773M</b>. If FA is locked, the bid is created immediately but will not start its 48-hour clock until FA is unlocked.</p>
          <form method="post" action="set-qo">
            <div class="row">
              <label>Team
                <select name="team">{% for t in teams %}<option value="{{ t }}">{{ team_label(t) }}</option>{% endfor %}</select>
              </label>
              <label>Search free agent
                <input id="qo-player-search" placeholder="Type at least 2 characters of a free agent name">
                <input id="qo-player-id" name="player_id" type="hidden">
              </label>
            </div>
            <div id="qo-player-selected" class="muted" style="margin-top:8px;">No free agent selected.</div>
            <div id="qo-player-results" class="search-results muted" style="margin:8px 0 12px;">Type at least 2 characters to search unsigned free agents.</div>
            <div style="margin-top:12px;"><button id="qo-submit" class="btn primary" type="submit" disabled>Set QO</button></div>
          </form>
        </div>

        <div class="card">
          <h2>Reset FA Bid</h2>
          <p class="muted">Use this when the current high FA bid was entered incorrectly. The current active bid is marked <b>VOIDED</b>. If there was a previous bid, it becomes the new active high bid with a fresh 48-hour clock; otherwise bidding is cleared for that player.</p>
          <form method="post" action="reset-fa-bid" onsubmit="return confirm('Void the current active FA bid for this player and restore the previous high bid if one exists?');">
            <label>Search active FA bid
              <input id="reset-fa-player-search" placeholder="Type at least 2 characters of a player with an active bid">
              <input id="reset-fa-player-id" name="player_id" type="hidden">
            </label>
            <div id="reset-fa-player-selected" class="muted" style="margin-top:8px;">No active bid selected.</div>
            <div id="reset-fa-player-results" class="search-results muted" style="margin:8px 0 12px;">Type at least 2 characters to search active FA bids.</div>
            <label>Admin note / reason
              <input name="note" placeholder="Example: Entered $15M instead of $1.5M">
            </label>
            <div style="margin-top:12px;"><button id="reset-fa-submit" class="btn primary" type="submit" disabled>Reset active bid</button></div>
          </form>
        </div>

        <div class="card">
          <h2>FA Blacklist</h2>
          <p class="muted">Use this for players who should be removed from the public FA pool. Blacklisting hides them from the FA page and watchlists, blocks new bids/QOs, and voids any existing active/outbid FA bids.</p>
          <form method="post" action="blacklist-fa-player" onsubmit="return confirm('Blacklist this player, hide them from FA, and void their existing FA bids?');">
            <label>Search free agent
              <input id="blacklist-fa-player-search" placeholder="Type at least 2 characters of a free agent name">
              <input id="blacklist-fa-player-id" name="player_id" type="hidden">
            </label>
            <div id="blacklist-fa-player-selected" class="muted" style="margin-top:8px;">No free agent selected.</div>
            <div id="blacklist-fa-player-results" class="search-results muted" style="margin:8px 0 12px;">Type at least 2 characters to search unsigned free agents.</div>
            <label>Reason / note
              <input name="note" placeholder="Optional reason shown in admin log">
            </label>
            <div style="margin-top:12px;"><button id="blacklist-fa-submit" class="btn primary" type="submit" disabled>Blacklist free agent</button></div>
          </form>

          <h3 style="margin-top:18px;">Currently blacklisted</h3>
          <div class="table-wrap">
            <table>
              <thead><tr><th>Player</th><th>Pos</th><th>Blacklisted</th><th>Note</th><th>Bids</th><th></th></tr></thead>
              <tbody>
                {% for p in blacklisted_fa_players %}
                <tr>
                  <td><b>{{ p.name }}</b><div class="muted">#{{ p.id }}{% if p.roster_player_id %} • roster #{{ p.roster_player_id }}{% endif %}</div></td>
                  <td>{{ p.position or '—' }}</td>
                  <td>{{ p.blacklisted_at or '—' }}</td>
                  <td>{{ p.blacklist_note or '—' }}</td>
                  <td>{{ p.bid_rows or 0 }}</td>
                  <td>
                    <form method="post" action="unblacklist-fa-player" onsubmit="return confirm('Restore this player to normal FA eligibility?');">
                      <input type="hidden" name="player_id" value="{{ p.id }}">
                      <button class="btn" type="submit">Remove</button>
                    </form>
                  </td>
                </tr>
                {% endfor %}
                {% if not blacklisted_fa_players %}<tr><td colspan="6" class="muted">No players are currently blacklisted.</td></tr>{% endif %}
              </tbody>
            </table>
          </div>
        </div>

        <div class="card wide">
          <h2>Modify Player</h2>
          <p class="muted">Search by name, click “Use”, then change franchise and contract/roster fields. These changes write directly to roster.db, refresh Financials/Rosters, and lightly refresh that player’s FA row when applicable. Full Draft/Rule V/FA maintenance syncs are manual below.</p>
          <div class="row">
            <label>Search player
              <input id="player-search" placeholder="Type a player name or id">
            </label>
            <label>Selected player id
              <input id="player-id" form="player-form" name="player_id" placeholder="Roster player id">
            </label>
          </div>
          <div id="player-results" class="search-results muted" style="margin:8px 0 12px;">Type at least 2 characters to search.</div>
          <form id="player-form" method="post" action="update-player">
            <div class="row3">
              <label>Franchise
                <select id="player-franchise" name="team"><option value="">Unrostered / FA</option>{% for t in teams %}<option value="{{ t }}">{{ team_label(t) }}</option>{% endfor %}</select>
              </label>
              <label>Contract type
                <select id="player-contract-type" name="contract_type"><option value="">—</option>{% for c in contract_types %}<option value="{{ c }}">{{ c }}</option>{% endfor %}</select>
              </label>
              <label>Roster status
                <select id="player-roster-status" name="roster_status">{% for s in roster_statuses %}<option value="{{ s }}">{{ s }}</option>{% endfor %}</select>
              </label>
            </div>
            <div class="row3" style="margin-top:10px;">
              <label>Salary
                <input id="player-salary" name="salary" inputmode="decimal" placeholder="673000">
              </label>
              <label>Player options remaining
                <input id="player-options" name="options_remaining" type="number" min="0" max="10" step="1" placeholder="0">
              </label>
              <label>Year of expiry
                <input id="player-expires" name="contract_expires" placeholder="2026">
              </label>
            </div>
            <div class="row3" style="margin-top:10px;">
              <label>Service time
                <input id="player-service-time" name="service_time" inputmode="decimal" placeholder="0.00">
              </label>
              <label style="display:flex; gap:8px; align-items:center; margin-top:22px;">
                <input id="player-contract-option" name="contract_option" type="checkbox" value="1" style="width:auto; margin:0;"> Contract has option year
              </label>
              <label>Reason / note
                <input name="note" placeholder="Optional admin note">
              </label>
            </div>
            <div style="margin-top:12px;"><button class="btn primary" type="submit">Save player changes</button></div>
          </form>
        </div>

        <div class="card">
          <h2>Set Next Waivers Date</h2>
          <p class="muted">Sets the custom next waiver run and rewrites every active waiver entry to that date/time. New waivers will use this custom run while it remains in the future.</p>
          <form method="post" action="set-waiver-date">
            <div class="row">
              <label>Date
                <input name="waiver_date" type="date" value="{{ today }}">
              </label>
              <label>Time ET
                <input name="waiver_time" type="time" value="12:00">
              </label>
            </div>
            <div style="margin-top:12px;"><button class="btn primary" type="submit">Set waiver run</button></div>
          </form>
        </div>

        <div class="card">
          <h2>Set Draft Pick Time</h2>
          <p class="muted">Choose Amateur Draft or Rule V, then either set a new ET start time or mark one pick as skipped. Time changes can apply to only the selected pick or to that pick plus every later pick.</p>
          <form method="post" action="set-draft-time">
            <div class="row">
              <label>Draft
                <select id="draft-time-kind" name="draft_kind">
                  <option value="draft">Amateur Draft</option>
                  <option value="rulev">Rule V Draft</option>
                </select>
              </label>
              <label>Pick
                <select id="draft-time-pick" name="pick_key"></select>
              </label>
            </div>
            <div class="row" style="margin-top:10px;">
              <label>Action
                <select id="draft-time-action" name="draft_time_action">
                  <option value="set_time">Set pick time</option>
                  <option value="skip">Mark pick skipped</option>
                </select>
              </label>
              <label id="draft-time-scope-wrap">Scope
                <select id="draft-time-scope" name="time_scope">
                  <option value="following">This pick and all following picks</option>
                  <option value="single">This pick only</option>
                </select>
              </label>
            </div>
            <div id="draft-time-fields" class="row" style="margin-top:10px;">
              <label>Date
                <input id="draft-time-date" name="pick_date" type="date" value="{{ today }}">
              </label>
              <label>Time ET
                <input id="draft-time-time" name="pick_time" type="time" value="09:00" step="3600">
              </label>
            </div>
            <div style="margin-top:12px;"><button id="draft-time-submit" class="btn primary" type="submit">Set draft time</button></div>
          </form>
        </div>

        <div class="card wide">
          <h2>Admin Log</h2>
          <div class="table-wrap">
            <table>
              <thead><tr><th style="width:12%;">ID</th><th style="width:20%;">Created</th><th style="width:18%;">Action</th><th>Summary</th></tr></thead>
              <tbody>
                {% for row in logs %}
                <tr><td>{{ row.id }}</td><td>{{ row.created_at }}</td><td>{{ row.action }}</td><td>{{ row.summary }}</td></tr>
                {% endfor %}
                {% if not logs %}<tr><td colspan="4" class="muted">No admin actions logged yet.</td></tr>{% endif %}
              </tbody>
            </table>
          </div>
        </div>
      </div>

<script>
const searchBox = document.getElementById('player-search');
const results = document.getElementById('player-results');
const playerId = document.getElementById('player-id');
const playerFranchise = document.getElementById('player-franchise');
const playerContractType = document.getElementById('player-contract-type');
const playerRosterStatus = document.getElementById('player-roster-status');
const playerSalary = document.getElementById('player-salary');
const playerOptions = document.getElementById('player-options');
const playerExpires = document.getElementById('player-expires');
const playerServiceTime = document.getElementById('player-service-time');
const playerContractOption = document.getElementById('player-contract-option');
function esc(s){ return String(s ?? '').replace(/[&<>\"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','\"':'&quot;',"'":'&#39;'}[c])); }
function setSelectValue(select, value){
  if (!select) return;
  const wanted = String(value || '');
  for (const opt of select.options) { if (opt.value === wanted) { select.value = wanted; return; } }
}
function clearContractFieldsForUnrostered(){
  setSelectValue(playerContractType, '');
  setSelectValue(playerRosterStatus, 'Reserve');
  if (playerSalary) playerSalary.value = '';
  if (playerOptions) playerOptions.value = '';
  if (playerExpires) playerExpires.value = '';
  if (playerContractOption) playerContractOption.checked = false;
}
if (playerFranchise) {
  playerFranchise.addEventListener('change', () => {
    if (!playerFranchise.value) clearContractFieldsForUnrostered();
  });
}
let playersById = {};
let timer = null;
searchBox.addEventListener('input', () => {
  clearTimeout(timer);
  const q = searchBox.value.trim();
  if (q.length < 2) { results.textContent = 'Type at least 2 characters to search.'; return; }
  timer = setTimeout(async () => {
    const res = await fetch('api/players?q=' + encodeURIComponent(q));
    if (!res.ok) { results.textContent = 'Search failed.'; return; }
    const data = await res.json();
    playersById = Object.fromEntries((data.players || []).map(p => [String(p.id), p]));
    if (!data.players.length) { results.textContent = 'No matching players.'; return; }
    results.innerHTML = data.players.map(p => `<div><button class="btn" type="button" data-id="${p.id}">Use</button> <b>${esc(p.name)}</b> <span class="muted">#${p.id} • ${esc(p.position || '')} • ${esc(p.franchise || 'Unrostered')} • ${esc(p.roster_status || '')} • ${esc(p.contract_type || '')} • svc ${Number(p.service_time || 0).toFixed(2)} • $${Number(p.salary || 0).toLocaleString()}</span></div>`).join('');
    results.querySelectorAll('button[data-id]').forEach(btn => btn.onclick = () => {
      const p = playersById[String(btn.dataset.id)] || {};
      playerId.value = p.id || '';
      setSelectValue(playerFranchise, p.franchise || '');
      if (!p.franchise) {
        clearContractFieldsForUnrostered();
      } else {
        setSelectValue(playerContractType, p.contract_type || 'R');
        setSelectValue(playerRosterStatus, p.roster_status || 'Reserve');
        playerSalary.value = p.salary ?? 0;
        playerOptions.value = p.options_remaining ?? 0;
        playerExpires.value = p.contract_expires || '';
        playerContractOption.checked = !!Number(p.contract_option || 0);
      }
      playerServiceTime.value = Number(p.service_time || 0).toFixed(2);
    });
  }, 200);
});


const qoSearchBox = document.getElementById('qo-player-search');
const qoResults = document.getElementById('qo-player-results');
const qoSelected = document.getElementById('qo-player-selected');
const qoPlayerId = document.getElementById('qo-player-id');
const qoSubmit = document.getElementById('qo-submit');
let qoPlayersById = {};
let qoTimer = null;
function resetQOSelection(message){
  if (qoPlayerId) qoPlayerId.value = '';
  if (qoSelected) qoSelected.textContent = message || 'No free agent selected.';
  if (qoSubmit) qoSubmit.disabled = true;
}
function qoPlayerSummary(p){
  const bits = [`#${esc(p.id)}`];
  if (p.position) bits.push(esc(p.position));
  if (p.last_team) bits.push(`last: ${esc(p.last_team)}`);
  if (p.current_bid_team) bits.push(`current: ${esc(p.current_bid_team)} $${Number(p.current_aav_m || 0).toFixed(3)}M`);
  return bits.join(' • ');
}
if (qoSearchBox) {
  qoSearchBox.addEventListener('input', () => {
    clearTimeout(qoTimer);
    const q = qoSearchBox.value.trim();
    resetQOSelection('No free agent selected.');
    if (q.length < 2 && !/^\\d+$/.test(q)) {
      qoResults.textContent = 'Type at least 2 characters to search unsigned free agents.';
      return;
    }
    qoTimer = setTimeout(async () => {
      const res = await fetch('api/fa-players?q=' + encodeURIComponent(q));
      if (!res.ok) { qoResults.textContent = 'Search failed.'; return; }
      const data = await res.json();
      qoPlayersById = Object.fromEntries((data.players || []).map(p => [String(p.id), p]));
      if (!data.players.length) { qoResults.textContent = 'No matching unsigned free agents.'; return; }
      qoResults.innerHTML = data.players.map(p => `<div><button class="btn" type="button" data-qo-id="${p.id}">Use</button> <b>${esc(p.name)}</b> <span class="muted">${qoPlayerSummary(p)}</span></div>`).join('');
      qoResults.querySelectorAll('button[data-qo-id]').forEach(btn => btn.onclick = () => {
        const p = qoPlayersById[String(btn.dataset.qoId)] || {};
        qoPlayerId.value = p.id || '';
        qoSearchBox.value = p.name || '';
        qoSelected.innerHTML = `Selected: <b>${esc(p.name || '')}</b> <span class="muted">${qoPlayerSummary(p)}</span>`;
        qoSubmit.disabled = !qoPlayerId.value;
      });
    }, 200);
  });
}

const resetFaSearchBox = document.getElementById('reset-fa-player-search');
const resetFaResults = document.getElementById('reset-fa-player-results');
const resetFaSelected = document.getElementById('reset-fa-player-selected');
const resetFaPlayerId = document.getElementById('reset-fa-player-id');
const resetFaSubmit = document.getElementById('reset-fa-submit');
let resetFaPlayersById = {};
let resetFaTimer = null;
function resetFaSelection(message){
  if (resetFaPlayerId) resetFaPlayerId.value = '';
  if (resetFaSelected) resetFaSelected.textContent = message || 'No active bid selected.';
  if (resetFaSubmit) resetFaSubmit.disabled = true;
}
function resetFaContract(p){
  const y = Number(p.current_years || 1);
  const opt = Number(p.current_has_option || 0) ? '+opt' : '';
  return `${y}y${opt}`;
}
function resetFaPlayerSummary(p){
  const bits = [`#${esc(p.id)}`];
  if (p.position) bits.push(esc(p.position));
  if (p.current_bid_team) bits.push(`void: ${esc(p.current_bid_team)} ${resetFaContract(p)} $${Number(p.current_aav_m || 0).toFixed(3)}M`);
  if (p.previous_bid_team) bits.push(`restore: ${esc(p.previous_bid_team)} $${Number(p.previous_aav_m || 0).toFixed(3)}M`);
  else bits.push('restore: none; clears bidding');
  return bits.join(' • ');
}
if (resetFaSearchBox) {
  resetFaSearchBox.addEventListener('input', () => {
    clearTimeout(resetFaTimer);
    const q = resetFaSearchBox.value.trim();
    resetFaSelection('No active bid selected.');
    if (q.length < 2 && !/^[0-9]+$/.test(q)) {
      resetFaResults.textContent = 'Type at least 2 characters to search active FA bids.';
      return;
    }
    resetFaTimer = setTimeout(async () => {
      const res = await fetch('api/fa-bid-players?q=' + encodeURIComponent(q));
      if (!res.ok) { resetFaResults.textContent = 'Search failed.'; return; }
      const data = await res.json();
      resetFaPlayersById = Object.fromEntries((data.players || []).map(p => [String(p.id), p]));
      if (!data.players.length) { resetFaResults.textContent = 'No matching active FA bids.'; return; }
      resetFaResults.innerHTML = data.players.map(p => `<div><button class="btn" type="button" data-reset-fa-id="${p.id}">Use</button> <b>${esc(p.name)}</b> <span class="muted">${resetFaPlayerSummary(p)}</span></div>`).join('');
      resetFaResults.querySelectorAll('button[data-reset-fa-id]').forEach(btn => btn.onclick = () => {
        const p = resetFaPlayersById[String(btn.dataset.resetFaId)] || {};
        resetFaPlayerId.value = p.id || '';
        resetFaSearchBox.value = p.name || '';
        resetFaSelected.innerHTML = `Selected: <b>${esc(p.name || '')}</b> <span class="muted">${resetFaPlayerSummary(p)}</span>`;
        resetFaSubmit.disabled = !resetFaPlayerId.value;
      });
    }, 200);
  });
}


const blacklistFaSearchBox = document.getElementById('blacklist-fa-player-search');
const blacklistFaResults = document.getElementById('blacklist-fa-player-results');
const blacklistFaSelected = document.getElementById('blacklist-fa-player-selected');
const blacklistFaPlayerId = document.getElementById('blacklist-fa-player-id');
const blacklistFaSubmit = document.getElementById('blacklist-fa-submit');
let blacklistFaPlayersById = {};
let blacklistFaTimer = null;
function resetBlacklistFaSelection(message){
  if (blacklistFaPlayerId) blacklistFaPlayerId.value = '';
  if (blacklistFaSelected) blacklistFaSelected.textContent = message || 'No free agent selected.';
  if (blacklistFaSubmit) blacklistFaSubmit.disabled = true;
}
function blacklistFaPlayerSummary(p){
  const bits = [`#${esc(p.id)}`];
  if (p.position) bits.push(esc(p.position));
  if (p.last_team) bits.push(`last: ${esc(p.last_team)}`);
  if (p.current_bid_team) bits.push(`active bid: ${esc(p.current_bid_team)} $${Number(p.current_aav_m || 0).toFixed(3)}M`);
  return bits.join(' • ');
}
if (blacklistFaSearchBox) {
  blacklistFaSearchBox.addEventListener('input', () => {
    clearTimeout(blacklistFaTimer);
    const q = blacklistFaSearchBox.value.trim();
    resetBlacklistFaSelection('No free agent selected.');
    if (q.length < 2 && !/^[0-9]+$/.test(q)) {
      blacklistFaResults.textContent = 'Type at least 2 characters to search unsigned free agents.';
      return;
    }
    blacklistFaTimer = setTimeout(async () => {
      const res = await fetch('api/fa-blacklist-candidates?q=' + encodeURIComponent(q));
      if (!res.ok) { blacklistFaResults.textContent = 'Search failed.'; return; }
      const data = await res.json();
      blacklistFaPlayersById = Object.fromEntries((data.players || []).map(p => [String(p.id), p]));
      if (!data.players.length) { blacklistFaResults.textContent = 'No matching unblacklisted free agents.'; return; }
      blacklistFaResults.innerHTML = data.players.map(p => `<div><button class="btn" type="button" data-blacklist-fa-id="${p.id}">Use</button> <b>${esc(p.name)}</b> <span class="muted">${blacklistFaPlayerSummary(p)}</span></div>`).join('');
      blacklistFaResults.querySelectorAll('button[data-blacklist-fa-id]').forEach(btn => btn.onclick = () => {
        const p = blacklistFaPlayersById[String(btn.dataset.blacklistFaId)] || {};
        blacklistFaPlayerId.value = p.id || '';
        blacklistFaSearchBox.value = p.name || '';
        blacklistFaSelected.innerHTML = `Selected: <b>${esc(p.name || '')}</b> <span class="muted">${blacklistFaPlayerSummary(p)}</span>`;
        blacklistFaSubmit.disabled = !blacklistFaPlayerId.value;
      });
    }, 200);
  });
}


const draftTimeChoices = {{ draft_time_choices|tojson }};
const draftTimeKind = document.getElementById('draft-time-kind');
const draftTimePick = document.getElementById('draft-time-pick');
const draftTimeAction = document.getElementById('draft-time-action');
const draftTimeScopeWrap = document.getElementById('draft-time-scope-wrap');
const draftTimeScope = document.getElementById('draft-time-scope');
const draftTimeFields = document.getElementById('draft-time-fields');
const draftTimeDate = document.getElementById('draft-time-date');
const draftTimeTime = document.getElementById('draft-time-time');
const draftTimeSubmit = document.getElementById('draft-time-submit');
function refreshDraftTimePickOptions(){
  if (!draftTimeKind || !draftTimePick) return;
  const rows = draftTimeChoices[draftTimeKind.value] || [];
  draftTimePick.innerHTML = '';
  if (!rows.length) {
    const opt = document.createElement('option');
    opt.value = '';
    opt.textContent = 'No picks found';
    draftTimePick.appendChild(opt);
    return;
  }
  for (const row of rows) {
    const opt = document.createElement('option');
    opt.value = row.value;
    opt.textContent = row.label;
    draftTimePick.appendChild(opt);
  }
}
function refreshDraftTimeActionUi(){
  if (!draftTimeAction) return;
  const skipping = draftTimeAction.value === 'skip';
  if (draftTimeFields) draftTimeFields.style.display = skipping ? 'none' : '';
  if (draftTimeScopeWrap) draftTimeScopeWrap.style.display = skipping ? 'none' : '';
  if (draftTimeDate) draftTimeDate.disabled = skipping;
  if (draftTimeTime) draftTimeTime.disabled = skipping;
  if (draftTimeScope) draftTimeScope.disabled = skipping;
  if (draftTimeSubmit) draftTimeSubmit.textContent = skipping ? 'Mark pick skipped' : 'Set draft time';
}
if (draftTimeKind) {
  draftTimeKind.addEventListener('change', refreshDraftTimePickOptions);
  refreshDraftTimePickOptions();
}
if (draftTimeAction) {
  draftTimeAction.addEventListener('change', refreshDraftTimeActionUi);
  refreshDraftTimeActionUi();
}

const adminRefreshTargets = {{ refresh_targets|tojson }};
if (adminRefreshTargets && window.parent && window.parent !== window) {
  const targets = String(adminRefreshTargets).split(',').map(s => s.trim()).filter(Boolean);
  if (targets.length) window.parent.postMessage({ type: 'bnsl-admin-refresh', targets }, window.location.origin);
}
</script>
    {% endif %}
  </div>
</body>
</html>
""".replace("__BNSL_GAME_CSS__", BNSL_GAME_CSS)


@admin_bp.get("/")
def index():
    today = date.today().isoformat()
    return render_template_string(
        ADMIN_HTML,
        authed=is_admin_authed(),
        teams=TEAM_ABBRS,
        team_label=team_label,
        contract_types=CONTRACT_TYPES,
        roster_statuses=ROSTER_STATUS_TYPES,
        today=today,
        roster_locked=_is_roster_locked() if is_admin_authed() else False,
        fa_locked=_fa_locked() if is_admin_authed() else False,
        ootp27_dir=str(_ootp27_dir()) if is_admin_authed() else "",

        draft_time_choices=_draft_pick_choices() if is_admin_authed() else {"draft": [], "rulev": []},
        blacklisted_fa_players=_active_blacklisted_fa_players() if is_admin_authed() else [],
        logs=admin_logs() if is_admin_authed() else [],
        msg=request.args.get("msg", ""),
        err=request.args.get("err", ""),
        refresh_targets=request.args.get("refresh", ""),
    )


@admin_bp.post("/login")
def login():
    password = request.form.get("password", "")
    if password == ADMIN_PASSWORD:
        session[ADMIN_SESSION_KEY] = True
        log_action("admin_login", "Admin logged in")
        return redirect_with("Logged in.")
    return redirect_with(error="Invalid admin password.")


@admin_bp.post("/logout")
def logout():
    session.pop(ADMIN_SESSION_KEY, None)
    return redirect_with("Logged out.")


@admin_bp.get("/api/players")
def api_players():
    require_admin()
    q = request.args.get("q", "")
    return jsonify({"players": _search_roster_players(q)})


@admin_bp.get("/api/fa-players")
def api_fa_players():
    require_admin()
    q = request.args.get("q", "")
    return jsonify({"players": _search_fa_players(q)})


@admin_bp.get("/api/fa-bid-players")
def api_fa_bid_players():
    require_admin()
    q = request.args.get("q", "")
    return jsonify({"players": _search_fa_bid_players(q)})


@admin_bp.get("/api/fa-blacklist-candidates")
def api_fa_blacklist_candidates():
    require_admin()
    q = request.args.get("q", "")
    return jsonify({"players": _search_fa_blacklist_candidates(q)})


@admin_bp.post("/bonus-payment")
def bonus_payment():
    require_admin()
    try:
        team = canonical_team_abbr(request.form.get("team"))
        if team not in TEAM_ABBRS:
            raise ValueError("Unknown team")
        amount = _parse_money(request.form.get("amount"))
        effective_date = _parse_effective_date(request.form.get("effective_date"))
        description = (request.form.get("description") or "Admin bonus payment").strip() or "Admin bonus payment"

        if amount > 0:
            payer, receiver, post_amount = "BNSL", team, amount
            direction = "added to"
        else:
            payer, receiver, post_amount = team, "BNSL", abs(amount)
            direction = "removed from"

        payload = {
            "team": team,
            "amount": amount,
            "effective_date": effective_date,
            "description": description,
            "payer": payer,
            "receiver": receiver,
        }
        log_id = log_action(
            "bonus_payment_requested",
            f"{direction.capitalize()} {team}: ${amount:,.0f} ({description})",
            payload,
        )

        from financials_app import record_finance_payment
        record_finance_payment(
            source_type="admin_bonus",
            source_id=log_id,
            payer_team_abbr=payer,
            receiver_team_abbr=receiver,
            amount=post_amount,
            description=description,
            effective_date=effective_date,
        )
        log_action(
            "bonus_payment_posted",
            f"Posted admin bonus #{log_id}: {direction} {team} ${abs(amount):,.0f}",
            {**payload, "source_id": log_id},
        )
        return redirect_with(f"Posted bonus payment for {team}.", refresh=["financials"])
    except Exception as e:
        current_app.logger.exception("Admin bonus payment failed")
        return redirect_with(error=str(e))


@admin_bp.post("/update-player")
def update_player():
    require_admin()
    try:
        player_id = int(request.form.get("player_id") or 0)
        if player_id <= 0:
            raise ValueError("Select a valid player id")
        updates = {
            "franchise": request.form.get("team"),
            "contract_type": request.form.get("contract_type"),
            "salary": request.form.get("salary"),
            "service_time": request.form.get("service_time"),
            "options_remaining": request.form.get("options_remaining"),
            "contract_option": request.form.get("contract_option"),
            "contract_expires": request.form.get("contract_expires"),
            "roster_status": request.form.get("roster_status"),
        }
        note = (request.form.get("note") or "").strip()
        result = _update_player_fields(player_id, updates)
        if note:
            result["note"] = note
        changed_fields = ", ".join(result["changes"].keys()) or "no visible field changes"
        log_action(
            "update_player",
            f"Updated {result['player_name']} #{player_id}: {changed_fields}",
            result,
        )
        return redirect_with(
            f"Updated {result['player_name']}.",
            refresh=["roster", "financials", "fa", "rulev", "waivers"],
        )
    except Exception as e:
        current_app.logger.exception("Admin player update failed")
        return redirect_with(error=str(e))


@admin_bp.post("/change-player-franchise")
def change_player_franchise():
    require_admin()
    try:
        player_id = int(request.form.get("player_id") or 0)
        if player_id <= 0:
            raise ValueError("Select a valid player id")
        new_team = canonical_team_abbr(request.form.get("team"))
        note = (request.form.get("note") or "").strip()
        result = _change_player_franchise(player_id, new_team)
        if note:
            result["note"] = note
        log_action(
            "change_player_franchise",
            f"Moved {result['player_name']} #{player_id} from {result['old_team'] or 'FA'} to {result['new_team']}",
            result,
        )
        return redirect_with(f"Moved {result['player_name']} to {result['new_team']}.", refresh=["roster", "financials", "fa", "rulev", "waivers"])
    except Exception as e:
        current_app.logger.exception("Admin player-franchise change failed")
        return redirect_with(error=str(e))


@admin_bp.post("/toggle-roster-lock")
def toggle_roster_lock():
    require_admin()
    try:
        locked = _bool_from_form(request.form.get("locked")) == 1
        _set_roster_locked(locked)
        log_action(
            "roster_lock",
            f"Roster transactions {'locked' if locked else 'unlocked'}",
            {"locked": locked},
        )
        return redirect_with(
            f"Roster transactions are now {'locked' if locked else 'unlocked'}.",
            refresh=["roster"],
        )
    except Exception as e:
        current_app.logger.exception("Admin roster-lock toggle failed")
        return redirect_with(error=str(e))


@admin_bp.post("/toggle-fa-lock")
def toggle_fa_lock():
    require_admin()
    try:
        locked = _bool_from_form(request.form.get("locked")) == 1
        from fa_app import set_fa_locked
        result = set_fa_locked(locked)
        log_action(
            "fa_lock",
            f"Free agency {'locked' if locked else 'unlocked'}; {result.get('active_bids_updated', 0)} active bids updated",
            result,
        )
        return redirect_with(
            f"Free agency is now {'locked' if locked else 'unlocked'}.",
            refresh=["fa", "financials"],
        )
    except Exception as e:
        current_app.logger.exception("Admin FA-lock toggle failed")
        return redirect_with(error=str(e))


@admin_bp.post("/sync-draft-roster")
def sync_draft_roster():
    require_admin()
    try:
        from roster_app import sync_drafted_players_from_draft_db
        seen, inserted, updated = sync_drafted_players_from_draft_db()
        log_action(
            "sync_draft_roster",
            f"Synced draft picks into roster: {seen} seen, {inserted} inserted, {updated} updated",
            {"seen": seen, "inserted": inserted, "updated": updated},
        )
        return redirect_with(f"Draft → roster sync complete: {seen} seen, {inserted} inserted, {updated} updated.", refresh="roster")
    except Exception as e:
        current_app.logger.exception("Manual draft → roster sync failed")
        return redirect_with(error=str(e))


@admin_bp.post("/sync-rulev-roster")
def sync_rulev_roster():
    require_admin()
    try:
        from rulev_app import sync_rulev_from_roster_db
        eligible, upserted, hidden = sync_rulev_from_roster_db()
        log_action(
            "sync_rulev_roster",
            f"Synced Rule V pool: {eligible} eligible, {upserted} upserted, {hidden} hidden/stale",
            {"eligible": eligible, "upserted": upserted, "hidden": hidden},
        )
        return redirect_with(f"Rule V pool sync complete: {eligible} eligible, {upserted} upserted, {hidden} hidden/stale.", refresh="rulev")
    except Exception as e:
        current_app.logger.exception("Manual Rule V roster sync failed")
        return redirect_with(error=str(e))


@admin_bp.post("/sync-fa-roster")
def sync_fa_roster():
    require_admin()
    try:
        from fa_app import sync_free_agents_from_roster_if_needed
        sync_free_agents_from_roster_if_needed(force=True)
        log_action("sync_fa_roster", "Ran full FA maintenance sync", {})
        return redirect_with("Full FA maintenance sync complete.", refresh=["fa", "financials"])
    except Exception as e:
        current_app.logger.exception("Manual FA roster sync failed")
        return redirect_with(error=str(e))




@admin_bp.post("/generate-ootp-import")
def generate_ootp_import():
    require_admin()
    try:
        result = _build_bnsl_ootp_import_download()
        log_action(
            "generate_ootp_import",
            f"Generated OOTP import CSV from {result['row_count']} roster rows; audit rows: {result['audit_rows']}",
            {
                "row_count": result["row_count"],
                "audit_rows": result["audit_rows"],
                "ootp_dir": result["ootp_dir"],
                "stdout": result["stdout"][-4000:],
                "stderr": result["stderr"][-4000:],
            },
        )
        return current_app.response_class(
            result["data"],
            mimetype="text/csv",
            headers={"Content-Disposition": f"attachment; filename={result['filename']}"},
        )
    except Exception as e:
        current_app.logger.exception("Admin BNSL→OOTP import generation failed")
        return redirect_with(error=str(e))

@admin_bp.post("/set-qo")
def set_qo():
    require_admin()
    try:
        team = canonical_team_abbr(request.form.get("team"))
        if team not in TEAM_ABBRS:
            raise ValueError("Unknown team")
        player_id = int(request.form.get("player_id") or 0)
        if player_id <= 0:
            raise ValueError("Select a valid free agent")
        from fa_app import set_qualifying_offer, QO_AAV_M
        result = set_qualifying_offer(team, player_id, QO_AAV_M)
        log_action(
            "set_qo",
            f"Set QO for {result['player_name']} by {team}: ${QO_AAV_M:.3f}M",
            {"team_abbr": team, **result},
        )
        clock_note = " clock frozen until FA is unlocked" if result.get("fa_locked") else " 48-hour clock started"
        return redirect_with(
            f"Set QO for {result['player_name']} by {team} at ${QO_AAV_M:.3f}M;{clock_note}.",
            refresh=["fa", "financials"],
        )
    except Exception as e:
        current_app.logger.exception("Admin QO creation failed")
        return redirect_with(error=str(e))


@admin_bp.post("/reset-fa-bid")
def reset_fa_bid():
    require_admin()
    try:
        player_id = int(request.form.get("player_id") or 0)
        if player_id <= 0:
            raise ValueError("Select a valid free agent with an active bid")
        note = (request.form.get("note") or "").strip()
        from fa_app import reset_active_bid_for_player, fmt_money_m
        result = reset_active_bid_for_player(player_id)
        if note:
            result["note"] = note
        voided = result.get("voided_bid") or {}
        restored = result.get("restored_bid") or {}
        if restored:
            summary = (
                f"Voided active FA bid for {result['player_name']} "
                f"({voided.get('team')} {fmt_money_m(float(voided.get('aav_m') or 0.0))}/yr); "
                f"restored {restored.get('team')} {fmt_money_m(float(restored.get('aav_m') or 0.0))}/yr as active high bid"
            )
            user_msg = f"Reset bid for {result['player_name']}; restored previous high bid by {restored.get('team')}."
        else:
            summary = (
                f"Voided only active FA bid for {result['player_name']} "
                f"({voided.get('team')} {fmt_money_m(float(voided.get('aav_m') or 0.0))}/yr); no previous bid to restore"
            )
            user_msg = f"Reset bid for {result['player_name']}; no previous bid existed, so bidding is now clear."
        log_action("reset_fa_bid", summary, result)
        return redirect_with(user_msg, refresh=["fa", "financials"])
    except Exception as e:
        current_app.logger.exception("Admin FA bid reset failed")
        return redirect_with(error=str(e))



@admin_bp.post("/blacklist-fa-player")
def blacklist_fa_player():
    require_admin()
    try:
        player_id = int(request.form.get("player_id") or 0)
        if player_id <= 0:
            raise ValueError("Select a valid free agent")
        note = (request.form.get("note") or "").strip()
        from fa_app import blacklist_free_agent
        result = blacklist_free_agent(player_id, note=note)
        log_action(
            "blacklist_fa_player",
            f"Blacklisted {result['player_name']} #{player_id}; voided {result.get('bids_voided', 0)} FA bids",
            result,
        )
        return redirect_with(
            f"Blacklisted {result['player_name']} and voided {result.get('bids_voided', 0)} FA bids.",
            refresh=["fa", "financials"],
        )
    except Exception as e:
        current_app.logger.exception("Admin FA blacklist failed")
        return redirect_with(error=str(e))


@admin_bp.post("/unblacklist-fa-player")
def unblacklist_fa_player():
    require_admin()
    try:
        player_id = int(request.form.get("player_id") or 0)
        if player_id <= 0:
            raise ValueError("Select a valid free agent")
        from fa_app import unblacklist_free_agent
        result = unblacklist_free_agent(player_id)
        log_action(
            "unblacklist_fa_player",
            f"Removed {result['player_name']} #{player_id} from FA blacklist",
            result,
        )
        return redirect_with(f"Removed {result['player_name']} from the FA blacklist.", refresh=["fa"])
    except Exception as e:
        current_app.logger.exception("Admin FA unblacklist failed")
        return redirect_with(error=str(e))


@admin_bp.post("/set-draft-time")
def set_draft_time():
    require_admin()
    try:
        draft_kind = (request.form.get("draft_kind") or "").strip().lower()
        action = (request.form.get("draft_time_action") or "set_time").strip().lower()
        round_num, pick_num = _parse_pick_key(request.form.get("pick_key"))

        if action in {"skip", "skipped", "mark_skipped"}:
            if draft_kind == "draft":
                from draft_order_page import mark_draft_pick_skipped
                result = mark_draft_pick_skipped(round_num, pick_num)
                refresh = ["draft"]
            elif draft_kind == "rulev":
                from rulev_order_page import mark_rulev_pick_skipped_by_round_pick
                result = mark_rulev_pick_skipped_by_round_pick(round_num, pick_num)
                refresh = ["rulev"]
            else:
                raise ValueError("Choose either Amateur Draft or Rule V Draft")

            log_action(
                "mark_draft_pick_skipped",
                f"Marked {result['draft_name']} {result['pick_label']} ({result['team']}) as skipped",
                result,
            )
            return redirect_with(
                f"Marked {result['draft_name']} {result['pick_label']} as skipped.",
                refresh=refresh,
            )

        if action not in {"set_time", "time", "set"}:
            raise ValueError("Choose a valid draft-time action")

        scope = (request.form.get("time_scope") or "following").strip().lower()
        include_following = scope not in {"single", "only", "one"}
        start_dt = _parse_admin_draft_datetime(
            request.form.get("pick_date", ""),
            request.form.get("pick_time", "09:00"),
        )

        if draft_kind == "draft":
            from draft_order_page import set_pick_and_following_times, fmt_est
            result = set_pick_and_following_times(round_num, pick_num, start_dt, include_following=include_following)
            refresh = ["draft"]
        elif draft_kind == "rulev":
            from rulev_order_page import set_pick_and_following_times, fmt_est
            result = set_pick_and_following_times(round_num, pick_num, start_dt, include_following=include_following)
            refresh = ["rulev"]
        else:
            raise ValueError("Choose either Amateur Draft or Rule V Draft")

        when = fmt_est(start_dt)
        scope_text = "this pick and all following picks" if include_following else "this pick only"
        log_action(
            "set_draft_pick_time",
            f"Set {result['draft_name']} {result['pick_label']} ({result['team']}) to {when}; updated {result['updated_count']} pick time(s), scope={scope_text}",
            result,
        )
        return redirect_with(
            f"Set {result['draft_name']} {result['pick_label']} to {when} ({scope_text}).",
            refresh=refresh,
        )
    except Exception as e:
        current_app.logger.exception("Admin set-draft-time failed")
        return redirect_with(error=str(e))


@admin_bp.post("/set-waiver-date")
def set_waiver_date():
    require_admin()
    try:
        run_at_utc = _parse_waiver_datetime(
            request.form.get("waiver_date", ""),
            request.form.get("waiver_time", "12:00"),
        )
        result = _set_custom_waiver_date(run_at_utc)
        from waivers_app import format_et
        log_action(
            "set_next_waiver_date",
            f"Set next waiver run to {format_et(result['run_at'])}; updated {result['active_waivers_updated']} active waiver(s)",
            result,
        )
        return redirect_with(f"Set next waiver run to {format_et(result['run_at'])}.", refresh=["waivers"])
    except Exception as e:
        current_app.logger.exception("Admin set-waiver-date failed")
        return redirect_with(error=str(e))
