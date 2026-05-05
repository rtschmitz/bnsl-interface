from __future__ import annotations

from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
from typing import Any, Dict, List
import sqlite3
import unicodedata

from flask import Blueprint, current_app, jsonify, render_template_string, request, session, abort

from ui_skin import BNSL_GAME_CSS

waivers_bp = Blueprint("waivers", __name__)

EASTERN = ZoneInfo("America/New_York")
WAIVER_CLAIM_FEE = 50_000.0

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

# User-provided finishing order, inverted for default waiver priority.
# CWS is normalized to the rest of this app's CHW code.
FINISHING_ORDER = [
    "MIA", "DET", "PHI", "CHC", "COL", "NYM", "ARI", "SD", "SF", "MIN",
    "OAK", "CHW", "CLE", "HOU", "NYY", "BOS", "LAD", "LAA", "CIN", "BAL",
    "WSH", "ATL", "TOR", "KC", "PIT", "SEA", "TEX", "MIL", "TB", "STL",
]
DEFAULT_WAIVER_PRIORITY = list(reversed(FINISHING_ORDER))


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat(timespec="seconds")


def parse_iso(value: str) -> datetime:
    dt = datetime.fromisoformat(value)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def emails_equal(a: str | None, b: str | None) -> bool:
    if not a or not b:
        return False
    return a.strip().lower() == b.strip().lower()


def get_roster_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(current_app.config["ROSTER_DB_PATH"])
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


def next_waiver_run_at(dt: datetime | None = None) -> datetime:
    """Sunday 12:00 America/New_York waiver run for a newly waived player.

    Newly waived players must be on waivers more than 48 hours before the
    Sunday noon ET deadline.  If the next deadline is within 48 hours, they
    roll to the following Sunday's run.
    """
    now_et = (dt or utcnow()).astimezone(EASTERN)
    days_until_sunday = (6 - now_et.weekday()) % 7  # Monday=0, Sunday=6
    run_et = (now_et + timedelta(days=days_until_sunday)).replace(
        hour=12, minute=0, second=0, microsecond=0
    )
    if run_et <= now_et:
        run_et = run_et + timedelta(days=7)

    # Waivers entered inside the 48-hour window are not eligible for this
    # deadline; they process at the following weekly run instead.
    if run_et - now_et <= timedelta(hours=48):
        run_et = run_et + timedelta(days=7)

    return run_et.astimezone(timezone.utc)


def format_et(value: str | None) -> str:
    if not value:
        return ""
    try:
        return parse_iso(value).astimezone(EASTERN).strftime("%Y-%m-%d %I:%M %p ET")
    except Exception:
        return str(value)


def ensure_table_column(conn: sqlite3.Connection, table: str, column: str, ddl: str) -> None:
    cur = conn.cursor()
    cur.execute(f"PRAGMA table_info({table})")
    cols = {row[1] for row in cur.fetchall()}
    if column not in cols:
        cur.execute(f"ALTER TABLE {table} ADD COLUMN {ddl}")


def compact_claim_priorities(cur: sqlite3.Cursor, team: str) -> None:
    cur.execute("""
        SELECT c.id
        FROM waiver_claims c
        JOIN waiver_entries w ON w.id = c.waiver_id
        WHERE c.team_abbr=?
          AND c.status='pending'
          AND w.status='active'
        ORDER BY COALESCE(c.claim_priority, 1000000), datetime(c.claimed_at) ASC, c.id ASC
    """, (team,))
    for idx, row in enumerate(cur.fetchall(), start=1):
        cur.execute("UPDATE waiver_claims SET claim_priority=? WHERE id=?", (idx, int(row["id"])))


def ensure_waiver_schema(conn: sqlite3.Connection) -> None:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS waiver_entries (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            player_id INTEGER NOT NULL,
            player_name TEXT NOT NULL,
            position TEXT,
            date_of_birth TEXT,
            bats TEXT,
            throws TEXT,
            salary REAL,
            contract_type TEXT,
            contract_initial_season INTEGER,
            contract_length INTEGER,
            contract_option INTEGER,
            contract_expires TEXT,
            service_time REAL,
            previous_service_time REAL,
            service_time_2025 REAL,
            options_remaining INTEGER,
            fa_class TEXT,
            fangraphs_id TEXT,
            mlbam_id INTEGER,
            waived_from_team TEXT NOT NULL,
            pre_waiver_status TEXT,
            desired_status TEXT,
            waiver_reason TEXT,
            waived_at TEXT NOT NULL,
            run_at TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'active',
            claimed_by_team TEXT,
            processed_at TEXT
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS waiver_claims (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            waiver_id INTEGER NOT NULL,
            team_abbr TEXT NOT NULL,
            claimed_at TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'pending',
            claim_priority INTEGER,
            UNIQUE(waiver_id, team_abbr) ON CONFLICT IGNORE
        )
    """)
    ensure_table_column(conn, "waiver_claims", "claim_priority", "claim_priority INTEGER")

    # Backfill priorities for claims created before claim-ordering existed.
    cur = conn.cursor()
    cur.execute("""
        SELECT DISTINCT team_abbr
        FROM waiver_claims
        WHERE status='pending'
    """)
    for row in cur.fetchall():
        compact_claim_priorities(cur, row["team_abbr"])

    conn.execute("CREATE INDEX IF NOT EXISTS idx_waiver_entries_status_run ON waiver_entries(status, run_at)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_waiver_claims_waiver ON waiver_claims(waiver_id, status)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_waiver_claims_team_priority ON waiver_claims(team_abbr, status, claim_priority)")
    conn.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS idx_waiver_entries_active_player
        ON waiver_entries(player_id)
        WHERE status='active'
    """)
    conn.commit()


def _row_value(row: Any, key: str, default: Any = None) -> Any:
    try:
        if isinstance(row, sqlite3.Row):
            return row[key] if key in row.keys() else default
        return row.get(key, default)
    except Exception:
        return default


def create_waiver_from_roster_row(
    conn: sqlite3.Connection,
    player_row: sqlite3.Row,
    waived_from_team: str,
    pre_waiver_status: str,
    desired_status: str,
    waiver_reason: str,
) -> int:
    """Create an active waiver entry using the pre-move roster snapshot."""
    ensure_waiver_schema(conn)
    cur = conn.cursor()
    player_id = int(_row_value(player_row, "id") or 0)
    cur.execute("SELECT id FROM waiver_entries WHERE player_id=? AND status='active'", (player_id,))
    existing = cur.fetchone()
    if existing:
        return int(existing["id"])

    now = utcnow()
    run_at = next_waiver_run_at(now)
    cur.execute("""
        INSERT INTO waiver_entries(
            player_id, player_name, position, date_of_birth, bats, throws,
            salary, contract_type, contract_initial_season, contract_length,
            contract_option, contract_expires, service_time, previous_service_time,
            service_time_2025, options_remaining, fa_class, fangraphs_id, mlbam_id,
            waived_from_team, pre_waiver_status, desired_status, waiver_reason,
            waived_at, run_at, status
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?, 'active')
    """, (
        player_id,
        str(_row_value(player_row, "name", "") or ""),
        _row_value(player_row, "position", ""),
        _row_value(player_row, "date_of_birth", ""),
        _row_value(player_row, "bats", ""),
        _row_value(player_row, "throws", ""),
        float(_row_value(player_row, "salary", 0.0) or 0.0),
        _row_value(player_row, "contract_type", ""),
        _row_value(player_row, "contract_initial_season", None),
        _row_value(player_row, "contract_length", None),
        int(_row_value(player_row, "contract_option", 0) or 0),
        _row_value(player_row, "contract_expires", ""),
        float(_row_value(player_row, "service_time", 0.0) or 0.0),
        float(_row_value(player_row, "previous_service_time", 0.0) or 0.0),
        float(_row_value(player_row, "service_time_2025", 0.0) or 0.0),
        int(_row_value(player_row, "options_remaining", 0) or 0),
        _row_value(player_row, "fa_class", ""),
        _row_value(player_row, "fangraphs_id", ""),
        _row_value(player_row, "mlbam_id", None),
        waived_from_team,
        pre_waiver_status,
        desired_status,
        waiver_reason,
        iso(now),
        iso(run_at),
    ))
    return int(cur.lastrowid)


def _priority_index(team: str, priority: list[str]) -> int:
    team = (team or "").upper()
    try:
        return priority.index(team)
    except ValueError:
        return 10_000


def _restore_claimed_player(cur: sqlite3.Cursor, waiver: sqlite3.Row, claiming_team: str) -> None:
    restore_status = waiver["pre_waiver_status"] or "Reserve"
    cur.execute("""
        UPDATE roster_players
        SET signed=1,
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
            affiliate_team='',
            roster_status=?,
            active_roster=?,
            options_remaining=?,
            fa_class=?,
            fangraphs_id=COALESCE(NULLIF(?, ''), fangraphs_id),
            mlbam_id=COALESCE(?, mlbam_id)
        WHERE id=?
    """, (
        waiver["contract_type"] or "",
        float(waiver["salary"] or 0.0),
        waiver["contract_initial_season"],
        waiver["contract_length"],
        int(waiver["contract_option"] or 0),
        waiver["contract_expires"] or "",
        float(waiver["service_time"] or 0.0),
        float(waiver["previous_service_time"] or 0.0),
        float(waiver["service_time_2025"] or 0.0),
        claiming_team,
        restore_status,
        1 if restore_status == "Active" else 0,
        int(waiver["options_remaining"] or 0),
        waiver["fa_class"] or "",
        waiver["fangraphs_id"] or "",
        waiver["mlbam_id"],
        int(waiver["player_id"]),
    ))


def sync_after_waiver_processing() -> None:
    try:
        from rulev_app import sync_rulev_from_roster_db
        sync_rulev_from_roster_db()
    except Exception:
        current_app.logger.exception("Rule V sync failed after waiver processing")
    try:
        from fa_app import sync_free_agents_from_roster_db
        sync_free_agents_from_roster_db()
    except Exception:
        current_app.logger.exception("FA sync failed after waiver processing")


def process_due_waivers() -> dict[str, int]:
    """Process every active waiver whose scheduled Sunday noon run has arrived.

    Within each weekly run, priority starts at the default order.  The highest
    priority team with any pending claim receives its highest-ranked remaining
    claim, then moves to the bottom of the priority list.  Repeat until no
    claimable pending claims remain, then mark the rest unclaimed.
    """
    now = utcnow()
    payments_to_post: list[dict[str, Any]] = []
    processed = 0
    claimed = 0
    unclaimed = 0

    conn = get_roster_conn()
    ensure_waiver_schema(conn)
    cur = conn.cursor()
    cur.execute("""
        SELECT DISTINCT run_at
        FROM waiver_entries
        WHERE status='active' AND datetime(run_at) <= datetime(?)
        ORDER BY datetime(run_at) ASC
    """, (iso(now),))
    due_runs = [r["run_at"] for r in cur.fetchall()]

    for run_at in due_runs:
        priority = list(DEFAULT_WAIVER_PRIORITY)

        while True:
            cur.execute("""
                SELECT
                    c.id AS claim_id,
                    c.waiver_id,
                    c.team_abbr,
                    c.claimed_at,
                    COALESCE(c.claim_priority, 1000000) AS claim_priority
                FROM waiver_claims c
                JOIN waiver_entries w ON w.id = c.waiver_id
                WHERE w.status='active'
                  AND w.run_at=?
                  AND c.status='pending'
                  AND c.team_abbr != w.waived_from_team
                ORDER BY c.team_abbr ASC,
                         COALESCE(c.claim_priority, 1000000) ASC,
                         datetime(c.claimed_at) ASC,
                         c.id ASC
            """, (run_at,))
            active_claims = cur.fetchall()
            if not active_claims:
                break

            teams_with_claims = sorted({c["team_abbr"] for c in active_claims})
            winner_team = min(teams_with_claims, key=lambda t: _priority_index(t, priority))
            team_claims = [c for c in active_claims if c["team_abbr"] == winner_team]
            winner_claim = min(
                team_claims,
                key=lambda c: (int(c["claim_priority"] or 1000000), c["claimed_at"] or "", int(c["claim_id"])),
            )
            waiver_id = int(winner_claim["waiver_id"])

            cur.execute("SELECT * FROM waiver_entries WHERE id=? AND status='active'", (waiver_id,))
            waiver = cur.fetchone()
            if not waiver:
                cur.execute("UPDATE waiver_claims SET status='denied' WHERE id=?", (int(winner_claim["claim_id"]),))
                continue

            _restore_claimed_player(cur, waiver, winner_team)
            cur.execute("""
                UPDATE waiver_entries
                SET status='claimed', claimed_by_team=?, processed_at=?
                WHERE id=?
            """, (winner_team, iso(now), waiver_id))
            cur.execute("UPDATE waiver_claims SET status='denied' WHERE waiver_id=?", (waiver_id,))
            cur.execute("UPDATE waiver_claims SET status='awarded' WHERE id=?", (int(winner_claim["claim_id"]),))
            payments_to_post.append({
                "source_id": waiver_id,
                "payer": winner_team,
                "receiver": waiver["waived_from_team"],
                "amount": WAIVER_CLAIM_FEE,
                "description": f"Waiver claim fee for {waiver['player_name']}",
            })
            if winner_team in priority:
                priority.remove(winner_team)
            priority.append(winner_team)
            processed += 1
            claimed += 1

        cur.execute("""
            SELECT id
            FROM waiver_entries
            WHERE status='active' AND run_at=?
            ORDER BY datetime(waived_at) ASC, id ASC
        """, (run_at,))
        remaining = [int(r["id"]) for r in cur.fetchall()]
        for waiver_id in remaining:
            cur.execute("""
                UPDATE waiver_entries
                SET status='unclaimed', processed_at=?
                WHERE id=? AND status='active'
            """, (iso(now), waiver_id))
            if cur.rowcount:
                cur.execute("UPDATE waiver_claims SET status='denied' WHERE waiver_id=?", (waiver_id,))
                processed += 1
                unclaimed += 1

    conn.commit()
    conn.close()

    for p in payments_to_post:
        try:
            from financials_app import record_finance_payment
            record_finance_payment(
                source_type="waiver_claim",
                source_id=p["source_id"],
                payer_team_abbr=p["payer"],
                receiver_team_abbr=p["receiver"],
                amount=p["amount"],
                description=p["description"],
                effective_date=now.date().isoformat(),
            )
        except Exception:
            current_app.logger.exception("Failed to post waiver-claim payment")

    if processed:
        sync_after_waiver_processing()

    return {"processed": processed, "claimed": claimed, "unclaimed": unclaimed}


def bootstrap_waivers() -> None:
    conn = get_roster_conn()
    ensure_waiver_schema(conn)
    conn.close()


def require_team() -> str:
    team = session.get("waivers_authed_team") or session.get("roster_authed_team")
    if not team:
        abort(401, "Not logged in")
    return team


def waiver_to_dict(row: sqlite3.Row, authed_team: str = "", claims_by_waiver: dict[int, list[str]] | None = None) -> dict[str, Any]:
    claims = claims_by_waiver.get(int(row["id"]), []) if claims_by_waiver else []
    return {
        "id": int(row["id"]),
        "player_id": int(row["player_id"]),
        "name": row["player_name"],
        "position": row["position"] or "",
        "team": row["waived_from_team"] or "",
        "salary": float(row["salary"] or 0.0),
        "salary_m": round(float(row["salary"] or 0.0) / 1_000_000.0, 3),
        "contract_type": row["contract_type"] or "",
        "service_time": round(float(row["service_time"] or 0.0), 2),
        "options_remaining": int(row["options_remaining"] or 0),
        "status": row["status"],
        "desired_status": row["desired_status"] or "",
        "claim_restore_status": row["pre_waiver_status"] or "",
        "reason": row["waiver_reason"] or "",
        "waived_at": row["waived_at"],
        "waived_at_et": format_et(row["waived_at"]),
        "run_at": row["run_at"],
        "run_at_et": format_et(row["run_at"]),
        "claimed_by_team": row["claimed_by_team"] or "",
        "processed_at_et": format_et(row["processed_at"]),
        "claims": claims,
        "claim_count": len(claims),
        "my_claim": bool(authed_team and authed_team in claims),
        "can_claim": bool(authed_team and row["status"] == "active" and authed_team != (row["waived_from_team"] or "")),
    }


@waivers_bp.get("/api/status")
def api_status():
    process_due_waivers()
    return jsonify({
        "authed_team": session.get("waivers_authed_team") or session.get("roster_authed_team", ""),
        "authed_email": session.get("waivers_authed_email") or session.get("roster_authed_email", ""),
        "teams": TEAM_ABBRS,
        "priority": DEFAULT_WAIVER_PRIORITY,
        "next_run_at_et": format_et(iso(next_waiver_run_at())),
        "claim_fee": WAIVER_CLAIM_FEE,
    })


@waivers_bp.post("/api/login_team")
def api_login_team():
    data = request.get_json(force=True, silent=True) or {}
    team = (data.get("team") or "").strip().upper()
    email = (data.get("email") or "").strip()
    expected = TEAM_EMAILS.get(team)
    if not expected:
        return ("Unknown team", 400)
    if emails_equal(email, expected):
        session["waivers_authed_team"] = team
        session["waivers_authed_email"] = email
        return jsonify({"ok": True})
    return ("Invalid email", 401)


@waivers_bp.get("/api/waivers")
def api_waivers():
    process_due_waivers()
    show_all = request.args.get("show_all") == "1"
    authed_team = session.get("waivers_authed_team") or session.get("roster_authed_team", "")
    conn = get_roster_conn()
    ensure_waiver_schema(conn)
    cur = conn.cursor()
    where = ""
    if not show_all:
        where = "WHERE status='active'"
    cur.execute(f"""
        SELECT *
        FROM waiver_entries
        {where}
        ORDER BY CASE status WHEN 'active' THEN 0 ELSE 1 END,
                 datetime(run_at) ASC,
                 datetime(waived_at) DESC,
                 id DESC
        LIMIT 500
    """)
    rows = cur.fetchall()

    ids = [int(r["id"]) for r in rows]
    claims_by_waiver: dict[int, list[str]] = {i: [] for i in ids}
    if ids:
        placeholders = ",".join("?" for _ in ids)
        cur.execute(f"""
            SELECT waiver_id, team_abbr
            FROM waiver_claims
            WHERE waiver_id IN ({placeholders})
              AND status IN ('pending', 'awarded')
            ORDER BY COALESCE(claim_priority, 1000000) ASC, datetime(claimed_at) ASC, id ASC
        """, ids)
        for c in cur.fetchall():
            claims_by_waiver.setdefault(int(c["waiver_id"]), []).append(c["team_abbr"])
    conn.close()
    return jsonify({"waivers": [waiver_to_dict(r, authed_team, claims_by_waiver) for r in rows]})


@waivers_bp.post("/api/claim")
def api_claim():
    team = require_team()
    process_due_waivers()
    data = request.get_json(force=True, silent=True) or {}
    waiver_id = int(data.get("waiver_id") or 0)
    if waiver_id <= 0:
        return ("Missing waiver id", 400)

    conn = get_roster_conn()
    ensure_waiver_schema(conn)
    cur = conn.cursor()
    cur.execute("SELECT * FROM waiver_entries WHERE id=?", (waiver_id,))
    waiver = cur.fetchone()
    if not waiver:
        conn.close()
        return ("Waiver not found", 404)
    if waiver["status"] != "active":
        conn.close()
        return ("This waiver has already been processed", 409)
    if team == (waiver["waived_from_team"] or ""):
        conn.close()
        return ("You cannot claim your own waived player", 409)
    if utcnow() >= parse_iso(waiver["run_at"]):
        conn.close()
        process_due_waivers()
        return ("This waiver run has already started", 409)

    cur.execute("""
        SELECT COALESCE(MAX(c.claim_priority), 0) + 1
        FROM waiver_claims c
        JOIN waiver_entries w ON w.id = c.waiver_id
        WHERE c.team_abbr=?
          AND c.status='pending'
          AND w.status='active'
    """, (team,))
    claim_priority = int(cur.fetchone()[0] or 1)

    cur.execute("""
        INSERT INTO waiver_claims(waiver_id, team_abbr, claimed_at, status, claim_priority)
        VALUES(?,?,?, 'pending', ?)
        ON CONFLICT(waiver_id, team_abbr) DO NOTHING
    """, (waiver_id, team, iso(utcnow()), claim_priority))
    compact_claim_priorities(cur, team)
    conn.commit()
    conn.close()
    return jsonify({"ok": True})


@waivers_bp.get("/api/my_claims")
def api_my_claims():
    team = require_team()
    process_due_waivers()
    conn = get_roster_conn()
    ensure_waiver_schema(conn)
    cur = conn.cursor()
    cur.execute("""
        SELECT
            c.id AS claim_id,
            c.claim_priority,
            c.claimed_at,
            w.*
        FROM waiver_claims c
        JOIN waiver_entries w ON w.id = c.waiver_id
        WHERE c.team_abbr=?
          AND c.status='pending'
          AND w.status='active'
        ORDER BY COALESCE(c.claim_priority, 1000000) ASC,
                 datetime(c.claimed_at) ASC,
                 c.id ASC
    """, (team,))
    rows = cur.fetchall()
    claims = []
    for idx, row in enumerate(rows, start=1):
        claims.append({
            "claim_id": int(row["claim_id"]),
            "rank": idx,
            "waiver_id": int(row["id"]),
            "player_id": int(row["player_id"]),
            "name": row["player_name"],
            "position": row["position"] or "",
            "waived_from_team": row["waived_from_team"] or "",
            "salary_m": round(float(row["salary"] or 0.0) / 1_000_000.0, 3),
            "restore_status": row["pre_waiver_status"] or "",
            "run_at_et": format_et(row["run_at"]),
        })
    conn.close()
    return jsonify({"claims": claims})


@waivers_bp.post("/api/reorder_claims")
def api_reorder_claims():
    team = require_team()
    data = request.get_json(force=True, silent=True) or {}
    ordered_claim_ids = [int(x) for x in (data.get("claim_ids") or []) if str(x).strip()]
    if not ordered_claim_ids:
        return ("Missing claim order", 400)

    conn = get_roster_conn()
    ensure_waiver_schema(conn)
    cur = conn.cursor()
    cur.execute("""
        SELECT c.id
        FROM waiver_claims c
        JOIN waiver_entries w ON w.id = c.waiver_id
        WHERE c.team_abbr=?
          AND c.status='pending'
          AND w.status='active'
    """, (team,))
    valid_ids = {int(r["id"]) for r in cur.fetchall()}
    if set(ordered_claim_ids) != valid_ids:
        conn.close()
        return ("Claim order does not match your active pending claims", 400)

    for rank, claim_id in enumerate(ordered_claim_ids, start=1):
        cur.execute("UPDATE waiver_claims SET claim_priority=? WHERE id=? AND team_abbr=?", (rank, claim_id, team))
    conn.commit()
    conn.close()
    return jsonify({"ok": True})


@waivers_bp.post("/api/withdraw_claim")
def api_withdraw_claim():
    team = require_team()
    data = request.get_json(force=True, silent=True) or {}
    claim_id = int(data.get("claim_id") or 0)
    if claim_id <= 0:
        return ("Missing claim id", 400)

    conn = get_roster_conn()
    ensure_waiver_schema(conn)
    cur = conn.cursor()
    cur.execute("""
        SELECT c.id
        FROM waiver_claims c
        JOIN waiver_entries w ON w.id = c.waiver_id
        WHERE c.id=?
          AND c.team_abbr=?
          AND c.status='pending'
          AND w.status='active'
    """, (claim_id, team))
    if not cur.fetchone():
        conn.close()
        return ("Claim not found", 404)
    cur.execute("UPDATE waiver_claims SET status='withdrawn' WHERE id=? AND team_abbr=?", (claim_id, team))
    compact_claim_priorities(cur, team)
    conn.commit()
    conn.close()
    return jsonify({"ok": True})


@waivers_bp.post("/api/run_due")
def api_run_due():
    result = process_due_waivers()
    return jsonify(result)


WAIVERS_HTML = """
<!doctype html>
<html>
<head>
  <base href="/waivers/">
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Waivers</title>
  __BNSL_GAME_CSS__
  <style>
    .wrap { max-width: 1450px; margin: 0 auto; padding: 18px; }
    .topbar { display:flex; flex-wrap:wrap; gap:10px; align-items:center; margin-bottom:12px; }
    .subtle { opacity:.75; }
    .priority { display:flex; gap:6px; flex-wrap:wrap; margin: 10px 0 16px; }
    .chip { padding: 5px 8px; border-radius:999px; border:1px solid rgba(140,170,255,.22); background:rgba(255,255,255,.06); font-size:12px; }
    .panel-lite { border:1px solid rgba(140,170,255,.18); background:rgba(255,255,255,.04); border-radius:16px; padding:12px; margin:12px 0 16px; }
    table { width:100%; border-collapse: collapse; }
    th, td { padding:8px 10px; border-bottom:1px solid rgba(255,255,255,.08); vertical-align:top; }
    th { text-align:left; }
    .num { text-align:right; white-space:nowrap; }
    .btn[disabled] { opacity:.5; cursor:not-allowed; }
    .claim-order-actions { display:flex; gap:6px; flex-wrap:wrap; }
  </style>
</head>
<body>
  <div class="wrap">
    <h1>Waivers</h1>
    <p class="subtle">Anyone can view the waiver wire. Log in to place, withdraw, or reorder claims. Claims process once per week on Sunday at 12:00 PM ET. Players waived inside 48 hours of that deadline roll to the following Sunday. Successful claims pay $50,000 to the waiving team.</p>
    <div class="topbar">
      <button id="login-btn">Login</button>
      <button id="run-btn">Run due waivers</button>
      <label><input type="checkbox" id="show-all"> Show processed waivers</label>
      <span id="login-status">Not logged in</span>
      <span id="next-run"></span>
    </div>
    <div class="priority" id="priority"></div>

    <div id="my-claims-panel" class="panel-lite" style="display:none;">
      <h2>My Claim Order</h2>
      <p class="subtle">Rank these in the order you want your team to receive players. If you win a claim, your team drops to the bottom of priority before your next claim is considered.</p>
      <table>
        <thead>
          <tr>
            <th>Rank</th>
            <th>Player</th>
            <th>Waived by</th>
            <th>Restores if claimed</th>
            <th>Run</th>
            <th>Reorder</th>
          </tr>
        </thead>
        <tbody id="my-claims-body"></tbody>
      </table>
    </div>

    <table>
      <thead>
        <tr>
          <th>Player</th>
          <th>Waived by</th>
          <th>Reason</th>
          <th>Current status</th>
          <th>Restores if claimed</th>
          <th class="num">Salary</th>
          <th>Run</th>
          <th>Claims</th>
          <th>Action</th>
        </tr>
      </thead>
      <tbody id="waivers-body"></tbody>
    </table>
  </div>

<script>
const body = document.getElementById("waivers-body");
const loginBtn = document.getElementById("login-btn");
const runBtn = document.getElementById("run-btn");
const loginStatus = document.getElementById("login-status");
const nextRun = document.getElementById("next-run");
const showAll = document.getElementById("show-all");
const priorityBox = document.getElementById("priority");
const myClaimsPanel = document.getElementById("my-claims-panel");
const myClaimsBody = document.getElementById("my-claims-body");

let state = { authedTeam: "", showAll: false, myClaims: [] };

function moneyM(x) { return `$${Number(x).toFixed(3)}M`; }
function esc(s) { return String(s ?? "").replace(/[&<>\"]/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;'}[c])); }

async function fetchStatus() {
  const res = await fetch("api/status");
  const data = await res.json();
  state.authedTeam = data.authed_team || "";
  loginStatus.textContent = state.authedTeam ? `Logged in for ${state.authedTeam}` : "Not logged in";
  nextRun.textContent = data.next_run_at_et ? `Next new waiver deadline: ${data.next_run_at_et}` : "";
  priorityBox.innerHTML = `<span class="subtle">Default priority:</span> ` +
    (data.priority || []).map((t, i) => `<span class="chip">${i+1}. ${t}</span>`).join("");
  myClaimsPanel.style.display = state.authedTeam ? "block" : "none";
}

async function fetchMyClaims() {
  if (!state.authedTeam) {
    state.myClaims = [];
    myClaimsBody.innerHTML = "";
    myClaimsPanel.style.display = "none";
    return;
  }
  const res = await fetch("api/my_claims");
  if (!res.ok) {
    myClaimsBody.innerHTML = `<tr><td colspan="6">Unable to load claims.</td></tr>`;
    return;
  }
  const data = await res.json();
  state.myClaims = data.claims || [];
  renderMyClaims();
}

function renderMyClaims() {
  myClaimsPanel.style.display = state.authedTeam ? "block" : "none";
  if (!state.myClaims.length) {
    myClaimsBody.innerHTML = `<tr><td colspan="6" class="subtle">No active claims yet.</td></tr>`;
    return;
  }
  myClaimsBody.innerHTML = "";
  state.myClaims.forEach((c, idx) => {
    const tr = document.createElement("tr");
    tr.innerHTML = `
      <td>${idx + 1}</td>
      <td><b>${esc(c.name)}</b><br><span class="subtle">${esc(c.position)} • ${moneyM(c.salary_m)}</span></td>
      <td>${esc(c.waived_from_team)}</td>
      <td>${esc(c.restore_status)}</td>
      <td>${esc(c.run_at_et)}</td>
      <td><div class="claim-order-actions">
        <button class="btn mini move-claim" data-dir="up" data-id="${c.claim_id}" ${idx === 0 ? "disabled" : ""}>↑</button>
        <button class="btn mini move-claim" data-dir="down" data-id="${c.claim_id}" ${idx === state.myClaims.length - 1 ? "disabled" : ""}>↓</button>
        <button class="btn mini danger withdraw-claim" data-id="${c.claim_id}">Withdraw</button>
      </div></td>
    `;
    myClaimsBody.appendChild(tr);
  });

  document.querySelectorAll(".move-claim").forEach(btn => {
    btn.onclick = async () => {
      const id = Number(btn.dataset.id);
      const idx = state.myClaims.findIndex(c => c.claim_id === id);
      if (idx < 0) return;
      const dir = btn.dataset.dir;
      const swapIdx = dir === "up" ? idx - 1 : idx + 1;
      if (swapIdx < 0 || swapIdx >= state.myClaims.length) return;
      [state.myClaims[idx], state.myClaims[swapIdx]] = [state.myClaims[swapIdx], state.myClaims[idx]];
      await saveClaimOrder();
    };
  });

  document.querySelectorAll(".withdraw-claim").forEach(btn => {
    btn.onclick = async () => {
      if (!confirm("Withdraw this waiver claim?")) return;
      const resp = await fetch("api/withdraw_claim", {
        method: "POST",
        headers: {"Content-Type":"application/json"},
        body: JSON.stringify({ claim_id: Number(btn.dataset.id) })
      });
      if (!resp.ok) { alert(await resp.text()); }
      await fetchWaivers();
      await fetchMyClaims();
    };
  });
}

async function saveClaimOrder() {
  const claimIds = state.myClaims.map(c => c.claim_id);
  const resp = await fetch("api/reorder_claims", {
    method: "POST",
    headers: {"Content-Type":"application/json"},
    body: JSON.stringify({ claim_ids: claimIds })
  });
  if (!resp.ok) { alert(await resp.text()); }
  await fetchMyClaims();
}

async function fetchWaivers() {
  const res = await fetch("api/waivers?show_all=" + (state.showAll ? "1" : "0"));
  const data = await res.json();
  body.innerHTML = "";
  for (const w of data.waivers || []) {
    const tr = document.createElement("tr");
    const claimText = w.claim_count ? `${w.claim_count} claim(s): ${w.claims.join(", ")}` : "None";
    const action = w.status === "active"
      ? `<button class="btn claim-btn" data-id="${w.id}" ${!w.can_claim || w.my_claim ? "disabled" : ""}>${w.my_claim ? "Claim entered" : "Claim"}</button>`
      : `<span>${w.status === "claimed" ? "Claimed by " + esc(w.claimed_by_team) : "Unclaimed"}</span>`;
    tr.innerHTML = `
      <td><b>${esc(w.name)}</b><br><span class="subtle">${esc(w.position)} • ${esc(w.contract_type)} • ${Number(w.service_time).toFixed(2)} svc • opt ${w.options_remaining}</span></td>
      <td>${esc(w.team)}</td>
      <td>${esc(w.reason)}</td>
      <td>${esc(w.desired_status)}</td>
      <td>${esc(w.claim_restore_status)}</td>
      <td class="num">${moneyM(w.salary_m)}</td>
      <td>${esc(w.run_at_et)}</td>
      <td>${esc(claimText)}</td>
      <td>${action}</td>
    `;
    body.appendChild(tr);
  }
  document.querySelectorAll(".claim-btn").forEach(btn => {
    btn.onclick = async () => {
      const resp = await fetch("api/claim", {
        method: "POST",
        headers: {"Content-Type":"application/json"},
        body: JSON.stringify({ waiver_id: Number(btn.dataset.id) })
      });
      if (!resp.ok) { alert(await resp.text()); }
      await fetchStatus();
      await fetchWaivers();
      await fetchMyClaims();
    };
  });
}

loginBtn.onclick = async () => {
  const team = prompt("Team abbreviation?");
  if (!team) return;
  const email = prompt("Manager email?");
  if (!email) return;
  const resp = await fetch("api/login_team", {
    method: "POST",
    headers: {"Content-Type":"application/json"},
    body: JSON.stringify({ team: team.trim().toUpperCase(), email: email.trim() })
  });
  if (!resp.ok) { alert(await resp.text()); return; }
  await fetchStatus();
  await fetchWaivers();
  await fetchMyClaims();
};

runBtn.onclick = async () => {
  const resp = await fetch("api/run_due", { method: "POST" });
  const data = await resp.json();
  alert(`Processed ${data.processed} waiver(s): ${data.claimed} claimed, ${data.unclaimed} unclaimed.`);
  await fetchStatus();
  await fetchWaivers();
  await fetchMyClaims();
};

showAll.onchange = () => { state.showAll = showAll.checked; fetchWaivers(); };

(async function boot() {
  await fetchStatus();
  await fetchWaivers();
  await fetchMyClaims();
})();
</script>
</body>
</html>
""".replace("__BNSL_GAME_CSS__", BNSL_GAME_CSS)


@waivers_bp.get("/")
def waivers_index():
    return render_template_string(WAIVERS_HTML)
