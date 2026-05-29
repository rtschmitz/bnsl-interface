from __future__ import annotations

from datetime import date, datetime, time, timezone
from pathlib import Path
from typing import Any
import json
import os
import re
import sqlite3
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
    conn = sqlite3.connect(str(path))
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
    conn = sqlite3.connect(current_app.config["ROSTER_DB_PATH"])
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


def get_draft_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(current_app.config["DRAFT_DB_PATH"])
    conn.row_factory = sqlite3.Row
    return conn


def get_rulev_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(current_app.config["RULEV_DB_PATH"])
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


def _draft_pick_choices() -> dict[str, list[dict[str, str]]]:
    choices: dict[str, list[dict[str, str]]] = {"draft": [], "rulev": []}

    try:
        conn = get_draft_conn()
        cur = conn.cursor()
        cur.execute("""
            SELECT id, round, pick, team, player_id, drafted_at, label
            FROM draft_order
            ORDER BY round ASC, pick ASC
        """)
        for row in cur.fetchall():
            status = "selected" if row["player_id"] else "open"
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
        cur = conn.cursor()
        cur.execute("""
            SELECT id, round, pick, team, player_id, drafted_at
            FROM rulev_order
            ORDER BY round ASC, pick ASC
        """)
        for row in cur.fetchall():
            status = "selected" if row["player_id"] else "open"
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

    if "contract_type" in updates:
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

    if not clean:
        conn.close()
        raise ValueError("No player fields were supplied")

    if "fa_class" in row.keys():
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
        sync_after_roster_mutation()
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

        <div class="card wide">
          <h2>Modify Player</h2>
          <p class="muted">Search by name, click “Use”, then change franchise and contract/roster fields. These changes write directly to roster.db and refresh Financials, Rosters, FA/Rule V syncs, and active waiver snapshots.</p>
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
                <select id="player-franchise" name="team">{% for t in teams %}<option value="{{ t }}">{{ team_label(t) }}</option>{% endfor %}</select>
              </label>
              <label>Contract type
                <select id="player-contract-type" name="contract_type">{% for c in contract_types %}<option value="{{ c }}">{{ c }}</option>{% endfor %}</select>
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
          <p class="muted">Choose Amateur Draft or Rule V, choose a round/pick, then set the new ET start time. That pick and every later pick are regenerated hourly from 9 AM–6 PM ET, skipping Sundays. Rule V missed picks are skipped, not moved to an evening queue.</p>
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
              <label>Date
                <input name="pick_date" type="date" value="{{ today }}">
              </label>
              <label>Time ET
                <input name="pick_time" type="time" value="09:00" step="3600">
              </label>
            </div>
            <div style="margin-top:12px;"><button class="btn primary" type="submit">Set draft time</button></div>
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
    results.innerHTML = data.players.map(p => `<div><button class="btn" type="button" data-id="${p.id}">Use</button> <b>${esc(p.name)}</b> <span class="muted">#${p.id} • ${esc(p.position || '')} • ${esc(p.franchise || 'FA')} • ${esc(p.roster_status || '')} • ${esc(p.contract_type || '')} • svc ${Number(p.service_time || 0).toFixed(2)} • $${Number(p.salary || 0).toLocaleString()}</span></div>`).join('');
    results.querySelectorAll('button[data-id]').forEach(btn => btn.onclick = () => {
      const p = playersById[String(btn.dataset.id)] || {};
      playerId.value = p.id || '';
      setSelectValue(playerFranchise, p.franchise || '');
      setSelectValue(playerContractType, p.contract_type || 'R');
      setSelectValue(playerRosterStatus, p.roster_status || 'Reserve');
      playerSalary.value = p.salary ?? 0;
      playerOptions.value = p.options_remaining ?? 0;
      playerExpires.value = p.contract_expires || '';
      playerServiceTime.value = Number(p.service_time || 0).toFixed(2);
      playerContractOption.checked = !!Number(p.contract_option || 0);
    });
  }, 200);
});

const draftTimeChoices = {{ draft_time_choices|tojson }};
const draftTimeKind = document.getElementById('draft-time-kind');
const draftTimePick = document.getElementById('draft-time-pick');
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
if (draftTimeKind) {
  draftTimeKind.addEventListener('change', refreshDraftTimePickOptions);
  refreshDraftTimePickOptions();
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
        draft_time_choices=_draft_pick_choices() if is_admin_authed() else {"draft": [], "rulev": []},
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


@admin_bp.post("/set-draft-time")
def set_draft_time():
    require_admin()
    try:
        draft_kind = (request.form.get("draft_kind") or "").strip().lower()
        round_num, pick_num = _parse_pick_key(request.form.get("pick_key"))
        start_dt = _parse_admin_draft_datetime(
            request.form.get("pick_date", ""),
            request.form.get("pick_time", "09:00"),
        )

        if draft_kind == "draft":
            from draft_order_page import set_pick_and_following_times, fmt_est
            result = set_pick_and_following_times(round_num, pick_num, start_dt)
            refresh = ["draft"]
        elif draft_kind == "rulev":
            from rulev_order_page import set_pick_and_following_times, fmt_est
            result = set_pick_and_following_times(round_num, pick_num, start_dt)
            refresh = ["rulev"]
        else:
            raise ValueError("Choose either Amateur Draft or Rule V Draft")

        when = fmt_est(start_dt)
        log_action(
            "set_draft_pick_time",
            f"Set {result['draft_name']} {result['pick_label']} ({result['team']}) to {when}; regenerated {result['updated_count']} pick time(s)",
            result,
        )
        return redirect_with(
            f"Set {result['draft_name']} {result['pick_label']} to {when}.",
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
