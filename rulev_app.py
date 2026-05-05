#!/usr/bin/env python3
from __future__ import annotations

import os
import logging
import re
import sqlite3
from pathlib import Path
from datetime import datetime
from typing import Any, Dict, List

from flask import Blueprint, current_app, request, jsonify, session, render_template_string, abort

# Reuse teams + emails from your main draft app
from team_config import MLB_TEAMS, TEAM_EMAILS, emails_equal
from ui_skin import BNSL_GAME_CSS

rulev_bp = Blueprint("rulev", __name__)

APP_DIR = Path(__file__).resolve().parent
DEFAULT_DB = APP_DIR / "rulev.db"


# 2026 Rule V draft order: reverse of supplied 2025 standings finish
# after applying tiebreaks.  This uses the full team names used by rulev_order
# and draft_app.TEAM_EMAILS.
RULEV_DRAFT_ORDER_2026 = [
    "St. Louis Cardinals",
    "Tampa Bay Rays",
    "Milwaukee Brewers",
    "Texas Rangers",
    "Seattle Mariners",
    "Pittsburgh Pirates",
    "Kansas City Royals",
    "Toronto Blue Jays",
    "Atlanta Braves",
    "Washington Nationals",
    "Baltimore Orioles",
    "Cincinnati Reds",
    "Los Angeles Angels",
    "Los Angeles Dodgers",
    "Boston Red Sox",
    "New York Yankees",
    "Houston Astros",
    "Cleveland Guardians",
    "Chicago White Sox",
    "Oakland Athletics",
    "Minnesota Twins",
    "San Francisco Giants",
    "San Diego Padres",
    "Arizona Diamondbacks",
    "New York Mets",
    "Colorado Rockies",
    "Chicago Cubs",
    "Philadelphia Phillies",
    "Detroit Tigers",
    "Miami Marlins",
]

RULEV_PICK_FEE = 250_000.0
RULEV_PROTECTED_DRAFT_YEAR = 2025

RULEV_TEAM_ABBR = {
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


def canonical_team_abbr(team: Any) -> str:
    text = str(team or "").strip()
    if not text:
        return ""
    if text in RULEV_TEAM_ABBR:
        return RULEV_TEAM_ABBR[text]
    code = text.upper()
    return "WAS" if code == "WSH" else code


def get_db_path() -> Path:
    cfg = current_app.config.get("RULEV_DB_PATH")
    return Path(cfg) if cfg else DEFAULT_DB


def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(str(get_db_path()))
    conn.row_factory = sqlite3.Row
    return conn


def ensure_column(conn: sqlite3.Connection, table: str, col: str, coldef: str) -> None:
    cur = conn.cursor()
    cur.execute(f"PRAGMA table_info({table})")
    cols = {r[1] for r in cur.fetchall()}
    if col not in cols:
        cur.execute(f"ALTER TABLE {table} ADD COLUMN {coldef}")
        conn.commit()


def init_db():
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
      CREATE TABLE IF NOT EXISTS rulev_players (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL,
        position TEXT,
        org TEXT,
        drafted_by TEXT,
        drafted_at TEXT
      )
    """)

    # Roster-sync columns.  rulev_players.id remains the Rule V draft's local
    # player id because rulev_order.player_id already points at it.  The roster
    # player id is stored separately and kept unique.
    ensure_column(conn, "rulev_players", "roster_player_id", "roster_player_id INTEGER")
    ensure_column(conn, "rulev_players", "dob", "dob TEXT")
    ensure_column(conn, "rulev_players", "contract_type", "contract_type TEXT")
    ensure_column(conn, "rulev_players", "roster_status", "roster_status TEXT")
    ensure_column(conn, "rulev_players", "roster_source", "roster_source TEXT")
    ensure_column(conn, "rulev_players", "rulev_eligible", "rulev_eligible INTEGER NOT NULL DEFAULT 1")
    ensure_column(conn, "rulev_players", "last_seen_roster_sync", "last_seen_roster_sync TEXT")
    ensure_column(conn, "rulev_players", "removed_from_roster_sync", "removed_from_roster_sync TEXT")

    cur.execute("""
      CREATE UNIQUE INDEX IF NOT EXISTS rulev_players_roster_player_id_uq
      ON rulev_players(roster_player_id)
      WHERE roster_player_id IS NOT NULL
    """)
    cur.execute("""
      CREATE INDEX IF NOT EXISTS rulev_players_eligible_idx
      ON rulev_players(rulev_eligible, drafted_by)
    """)

    cur.execute("""
      CREATE TABLE IF NOT EXISTS rulev_order (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        round INTEGER NOT NULL,
        pick INTEGER NOT NULL,
        team TEXT NOT NULL,
        player_id INTEGER,
        drafted_at TEXT,
        UNIQUE(round, pick) ON CONFLICT IGNORE
      )
    """)

    conn.commit()
    conn.close()

def get_roster_db_path() -> Path:
    cfg = current_app.config.get("ROSTER_DB_PATH")
    return Path(cfg) if cfg else (APP_DIR / "roster.db")


def _infer_two_digit_birth_year(yy: int) -> int:
    # Baseball DOBs with 00-26 are 2000-2026; everything else is 1900s.
    return 2000 + yy if yy <= 26 else 1900 + yy


def _parse_roster_dob_parts(dob: Any) -> tuple[int, int | None, int | None] | None:
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
            return _infer_two_digit_birth_year(int(parts[2])), int(parts[1]), int(parts[0])

    # Last fallback: any explicit 19xx/20xx year embedded in the string.
    m = re.search(r"(19\d{2}|20\d{2})", text)
    if m:
        return int(m.group(1)), None, None

    return None


def _safe_birth_year(dob: Any) -> int | None:
    parsed = _parse_roster_dob_parts(dob)
    return parsed[0] if parsed else None


def _normalize_roster_dob(dob: Any) -> str:
    parsed = _parse_roster_dob_parts(dob)
    if not parsed:
        return str(dob or "").strip()
    year, month, day = parsed
    if month is None or day is None:
        return f"{year:04d}"
    if not (1 <= month <= 12 and 1 <= day <= 31):
        return str(dob or "").strip()
    return f"{year:04d}-{month:02d}-{day:02d}"


def _is_rulev_eligible_roster_row(row: sqlite3.Row) -> bool:
    """
    Rule V eligibility source of truth:
      - rostered to an organization, represented by a non-empty franchise
      - not protected on the 40-man roster, which is roster_status='Reserve'
      - born in or before 2001
      - not drafted in the protected 2025 BNSL draft class

    Do not use roster_players.signed here.  In the roster import, signed can be
    blank/0 for players who are still rostered to an org, so franchise/status
    are the authoritative rostered-state fields for Rule V.
    """
    franchise = str(row["franchise"] or "").strip()
    roster_status = str(row["roster_status"] or "").strip()
    birth_year = _safe_birth_year(row["date_of_birth"] if "date_of_birth" in row.keys() else "")
    draft_year = 0
    if "draft_year" in row.keys():
        try:
            draft_year = int(float(row["draft_year"] or 0))
        except Exception:
            draft_year = 0

    return (
        franchise != ""
        and roster_status == "Reserve"
        and draft_year != RULEV_PROTECTED_DRAFT_YEAR
        and birth_year is not None
        and birth_year <= 2001
    )


def seed_default_order_if_empty() -> None:
    """
    Keep the existing demo/default Rule V order behavior, but do not seed demo
    players.  The player pool now comes from roster.db.
    """
    init_db()
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM rulev_order")
    n_order = int(cur.fetchone()[0] or 0)
    if n_order > 0:
        conn.close()
        return

    rows = []
    for r in range(1, 4):
        for p, team in enumerate(RULEV_DRAFT_ORDER_2026, start=1):
            rows.append((r, p, team))
    cur.executemany("INSERT OR IGNORE INTO rulev_order(round, pick, team) VALUES(?,?,?)", rows)
    conn.commit()
    conn.close()


def sync_rulev_from_roster_db() -> tuple[int, int, int]:
    """
    Rebuild the Rule V eligible pool from live roster.db.

    This is intended to run at startup and after roster transactions, not on
    every Rule V search.  Existing drafted players are preserved so completed
    pick history/order rows do not break.

    Returns: (eligible_roster_rows, upserted_rows, hidden_stale_rows)
    """
    init_db()
    roster_path = get_roster_db_path()
    if not roster_path.exists():
        logging.warning("Rule V roster sync skipped: roster.db does not exist: %s", roster_path)
        return (0, 0, 0)

    rconn = sqlite3.connect(str(roster_path))
    rconn.row_factory = sqlite3.Row
    rcur = rconn.cursor()
    rcur.execute("SELECT * FROM roster_players")
    eligible_rows = [r for r in rcur.fetchall() if _is_rulev_eligible_roster_row(r)]
    rconn.close()

    now = datetime.utcnow().isoformat(timespec="seconds")
    conn = get_conn()
    cur = conn.cursor()

    # Hide all currently undrafted rows until proven eligible.  This removes
    # old demo rows from the visible list without deleting data or breaking
    # already-made picks.
    cur.execute("""
        UPDATE rulev_players
        SET rulev_eligible=0,
            removed_from_roster_sync=?
        WHERE drafted_by IS NULL OR drafted_by=''
    """, (now,))
    hidden = cur.rowcount if cur.rowcount is not None and cur.rowcount >= 0 else 0

    upserted = 0
    for r in eligible_rows:
        roster_player_id = int(r["id"])
        name = str(r["name"] or "").strip()
        if not name:
            continue
        position = str(r["position"] or "").strip()
        org = str(r["franchise"] or "").strip()
        dob = _normalize_roster_dob(r["date_of_birth"] if "date_of_birth" in r.keys() else "")
        contract_type = str(r["contract_type"] or "").strip().upper()
        roster_status = str(r["roster_status"] or "").strip()

        cur.execute("SELECT id FROM rulev_players WHERE roster_player_id=?", (roster_player_id,))
        existing = cur.fetchone()
        if existing:
            cur.execute("""
                UPDATE rulev_players
                SET name=?,
                    position=?,
                    org=?,
                    dob=?,
                    contract_type=?,
                    roster_status=?,
                    roster_source='roster_db',
                    rulev_eligible=1,
                    last_seen_roster_sync=?,
                    removed_from_roster_sync=NULL
                WHERE id=?
            """, (
                name, position, org, dob, contract_type, roster_status,
                now, int(existing["id"]),
            ))
        else:
            cur.execute("""
                INSERT INTO rulev_players(
                    name, position, org,
                    drafted_by, drafted_at,
                    roster_player_id, dob, contract_type, roster_status,
                    roster_source, rulev_eligible,
                    last_seen_roster_sync, removed_from_roster_sync
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)
            """, (
                name, position, org,
                None, None,
                roster_player_id, dob, contract_type, roster_status,
                "roster_db", 1,
                now, None,
            ))
        upserted += 1

    conn.commit()
    conn.close()
    logging.info(
        "Rule V roster sync complete: %s eligible roster rows, %s upserted, %s hidden/stale rows marked",
        len(eligible_rows), upserted, hidden,
    )
    return (len(eligible_rows), upserted, hidden)


def bootstrap_rulev(sync_roster: bool = True) -> None:
    """
    Call from app.py after bootstrap_roster().  This initializes schema, keeps
    the order table available, and refreshes the eligible player pool once.
    """
    init_db()
    seed_default_order_if_empty()
    if sync_roster:
        sync_rulev_from_roster_db()

def current_pick() -> Dict[str, Any] | None:
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
      SELECT id, round, pick, team
      FROM rulev_order
      WHERE player_id IS NULL
      ORDER BY round ASC, pick ASC
      LIMIT 1
    """)
    row = cur.fetchone()
    if not row:
        conn.close()
        return None

    cur.execute("SELECT COUNT(*) FROM rulev_order")
    total = int(cur.fetchone()[0] or 0)
    cur.execute("SELECT COUNT(*) FROM rulev_order WHERE player_id IS NOT NULL")
    made = int(cur.fetchone()[0] or 0)
    conn.close()

    return {
        "id": int(row["id"]),
        "round": int(row["round"]),
        "pick": int(row["pick"]),
        "team": row["team"],
        "picks_made": made,
        "total_picks": total,
    }


def _require_authed_team() -> str:
    team = session.get("authed_team")
    if not team:
        abort(401, "Not logged in")
    return team


INDEX_HTML = r"""
<!doctype html>
<html>
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Rule V Draft</title>
__BNSL_GAME_CSS__
<style>
  /* Rule V only */
  .taken { opacity: 0.55; }
</style>
</head>
<body>
  <div class="page">
    <div class="brand">
      <div>
        <h1>RULE V DRAFT</h1>
      </div>
      <div class="right">
        <a class="btn primary" href="/rulev/order">Order & Times →</a>
      </div>
    </div>

    <div class="panel pad">
      <div class="topbar">
        <div class="pill" id="status">
          <span>Current Pick:</span>
          <span id="cur">Loading…</span>
          <span id="prog" class="badge"></span>
        </div>

        <div class="pill" style="margin-left:auto;">
          <span class="muted">Tip:</span>
          <span>Login to unlock picks when your team is on the clock. Each pick posts a $0.25M fee to the player's old org.</span>
        </div>
      </div>

      <hr class="sep"/>

      <div class="topbar">
        <label class="pill">Your Team:
          <select id="team" style="margin-left:8px;"></select>
        </label>
        <button class="btn primary" id="login">Login</button>

        <div class="pill" style="margin-left:auto;">
          <span>Search:</span>
          <input id="search" type="text" placeholder="Type a player name…" style="min-width:260px;" />
          <span class="muted">(substring)</span>
        </div>
      </div>

      <div class="pill" style="margin-top:10px;">
        <span id="login-status">🔒 Not logged in</span>
      </div>

      <hr class="sep"/>

      <div class="table-wrap">
        <table>
          <thead>
            <tr>
              <th style="width:26%;">Name</th>
              <th style="width:10%;">Pos</th>
              <th style="width:12%;">Org</th>
              <th style="width:26%;">Picked By</th>
              <th style="width:16%;">Picked At</th>
              <th style="width:10%;">Action</th>
            </tr>
          </thead>
          <tbody id="body"></tbody>
        </table>
      </div>

<script>
const teamSel = document.getElementById('team');
const loginBtn = document.getElementById('login');
const loginStatus = document.getElementById('login-status');
const curSpan = document.getElementById('cur');
const prog = document.getElementById('prog');
const tbody = document.getElementById('body');
const search = document.getElementById('search');

let state = { search:'', selectedTeam:'', authed:false, authedEmail:'', current:null, players:[] };

function fmtIso(s){ if(!s) return ''; try { return new Date(s).toLocaleString(); } catch { return s; } }

function setTeams(teams){
  teamSel.innerHTML = '<option value="">— Select Team —</option>' + teams.map(t => `<option value="${t}">${t}</option>`).join('');
}

async function fetchStatus(){
  const r = await fetch('/rulev/api/status');
  const d = await r.json();
  setTeams(d.teams || []);
  if (d.selected_team) teamSel.value = d.selected_team;

  state.selectedTeam = d.selected_team || '';
  state.authed = !!d.authed_for_selected;
  state.authedEmail = d.authed_email || '';
  state.current = d.current || null;

  loginBtn.disabled = !teamSel.value;

  if (state.authed && state.selectedTeam){
    loginStatus.textContent = `🔓 Logged in as ${state.authedEmail} for ${state.selectedTeam}`;
  } else {
    loginStatus.textContent = '🔒 Not logged in';
  }

  if (!state.current){
    curSpan.textContent = 'Draft complete';
    prog.textContent = `${d.picks_made}/${d.total_picks}`;
  } else {
    curSpan.textContent = `Round ${state.current.round}, Pick ${state.current.pick} — ${state.current.team}`;
    prog.textContent = `${d.picks_made}/${d.total_picks}`;
  }
}

async function fetchPlayers(){
  const params = new URLSearchParams({ search: state.search });
  const r = await fetch('/rulev/api/players?' + params.toString());
  const d = await r.json();
  state.players = d.players || [];
  render();
}

function render(){
  tbody.innerHTML = '';
  const canPickNow = state.current && state.selectedTeam && state.authed && (state.current.team === state.selectedTeam);

  for (const p of state.players){
    const tr = document.createElement('tr');
    tr.className = 'row-hover' + (p.drafted_by ? ' taken' : '');

    const action = document.createElement('td');
    if (canPickNow && !p.drafted_by){
      const btn = document.createElement('button');
      btn.className = 'btn good';
      btn.textContent = 'Pick';
      btn.onclick = async () => {
        const ok = confirm(`Pick ${p.name} for ${state.selectedTeam}?`);
        if (!ok) return;
        btn.disabled = true;
        const resp = await fetch('/rulev/api/pick', {
          method:'POST',
          headers:{'Content-Type':'application/json'},
          body: JSON.stringify({ player_id: p.id })
        });
        if (!resp.ok){
          alert('Pick failed: ' + await resp.text());
          btn.disabled = false;
          return;
        }
        await fetchStatus();
        await fetchPlayers();
      };
      action.appendChild(btn);
    } else {
      action.innerHTML = '<span class="muted">—</span>';
    }

    tr.innerHTML = `
      <td><b>${p.name}</b></td>
      <td>${p.position || '—'}</td>
      <td>${p.org || '—'}</td>
      <td>${p.drafted_by || ''}</td>
      <td>${fmtIso(p.drafted_at) || ''}</td>
    `;
    tr.appendChild(action);
    tbody.appendChild(tr);
  }
}

function debounce(fn, ms){ let t; return (...a)=>{ clearTimeout(t); t=setTimeout(()=>fn(...a), ms); } }

search.addEventListener('input', debounce(()=>{ state.search = search.value || ''; fetchPlayers(); }, 120));

teamSel.addEventListener('change', async ()=>{
  const t = teamSel.value || '';
  await fetch('/rulev/api/select_team', { method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify({ team: t })});
  await fetchStatus();
  await fetchPlayers();
});

loginBtn.addEventListener('click', async ()=>{
  const t = teamSel.value || '';
  if (!t){ alert('Select a team first.'); return; }
  const email = prompt(`Enter the manager email for ${t}:`);
  if (!email || !email.trim()) return;
  const resp = await fetch('/rulev/api/login_team', { method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify({ team: t, email: email.trim() })});
  if (!resp.ok){ alert('Login failed: ' + await resp.text()); }
  await fetchStatus();
  await fetchPlayers();
});

(async function boot(){
  await fetchStatus();
  await fetchPlayers();
})();
</script>

    </div> <!-- /panel -->
  </div>   <!-- /page -->
</body>
</html>
"""
INDEX_HTML = INDEX_HTML.replace("__BNSL_GAME_CSS__", BNSL_GAME_CSS)



_schema_bootstrapped = False

@rulev_bp.before_app_request
def _ensure_schema_once():
    # Safety net for direct imports/tests.  The expensive roster sync is done
    # only in bootstrap_rulev() or explicit roster transactions.
    global _schema_bootstrapped
    if not _schema_bootstrapped:
        init_db()
        seed_default_order_if_empty()
        _schema_bootstrapped = True


@rulev_bp.get("/")
def index():
    return render_template_string(INDEX_HTML)


@rulev_bp.get("/api/status")
def api_status():
    cur = current_pick()
    selected_team = session.get("selected_team", "") or ""
    authed_team = session.get("authed_team", "") or ""
    authed_email = session.get("authed_email", "") or ""

    # totals for UI (even if draft complete)
    conn = get_conn()
    c = conn.cursor()
    c.execute("SELECT COUNT(*) FROM rulev_order WHERE player_id IS NOT NULL")
    made = int(c.fetchone()[0] or 0)
    c.execute("SELECT COUNT(*) FROM rulev_order")
    total = int(c.fetchone()[0] or 0)
    conn.close()

    return jsonify({
        "teams": MLB_TEAMS,
        "selected_team": selected_team,
        "authed_team": authed_team,
        "authed_email": authed_email,
        "authed_for_selected": bool(selected_team) and (authed_team == selected_team),
        "current": cur,
        "picks_made": made,
        "total_picks": total,
    })


@rulev_bp.post("/api/select_team")
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


@rulev_bp.post("/api/login_team")
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


@rulev_bp.get("/api/players")
def api_players():
    q = (request.args.get("search") or "").strip().lower()

    conn = get_conn()
    cur = conn.cursor()

    clauses = ["(COALESCE(rulev_eligible, 1)=1 OR COALESCE(drafted_by, '') != '')"]
    params: List[Any] = []
    if q:
        clauses.append("LOWER(name) LIKE ?")
        params.append(f"%{q}%")

    sql = "SELECT * FROM rulev_players WHERE " + " AND ".join(clauses) + " ORDER BY name COLLATE NOCASE ASC"
    cur.execute(sql, params)
    rows = cur.fetchall()
    conn.close()

    players = []
    for r in rows:
        players.append({
            "id": int(r["id"]),
            "name": r["name"],
            "position": r["position"],
            "org": r["org"],
            "dob": r["dob"] if "dob" in r.keys() else "",
            "roster_player_id": int(r["roster_player_id"] or 0) if "roster_player_id" in r.keys() else None,
            "rulev_eligible": bool(int(r["rulev_eligible"] or 0)) if "rulev_eligible" in r.keys() else True,
            "drafted_by": r["drafted_by"],
            "drafted_at": r["drafted_at"],
        })
    return jsonify({"players": players})


@rulev_bp.post("/api/pick")
def api_pick():
    team = _require_authed_team()
    data = request.get_json(force=True, silent=True) or {}
    player_id = int(data.get("player_id") or 0)
    if player_id <= 0:
        return ("Missing player_id", 400)

    cur = current_pick()
    if not cur:
        return ("Draft is complete", 400)

    selected_team = session.get("selected_team") or ""
    if selected_team != cur["team"]:
        return ("Not your pick", 403)
    if team != selected_team:
        return ("Not logged in for this team", 401)

    conn = get_conn()
    c = conn.cursor()

    # validate player availability
    c.execute("SELECT id, name, org, drafted_by FROM rulev_players WHERE id=?", (player_id,))
    r = c.fetchone()
    if not r:
        conn.close()
        return ("Player not found", 404)
    if (r["drafted_by"] or "").strip():
        conn.close()
        return ("Player already picked", 409)

    now = datetime.utcnow().isoformat(timespec="seconds")
    losing_team = canonical_team_abbr(r["org"] or "")
    picking_team = canonical_team_abbr(team)
    player_name = str(r["name"] or "").strip()

    # assign player + mark order
    c.execute("UPDATE rulev_players SET drafted_by=?, drafted_at=? WHERE id=?", (team, now, player_id))
    c.execute("UPDATE rulev_order SET player_id=?, drafted_at=? WHERE id=?", (player_id, now, cur["id"]))
    conn.commit()
    conn.close()

    if losing_team and picking_team:
        try:
            from financials_app import record_finance_payment
            record_finance_payment(
                source_type="rulev_pick_fee",
                source_id=int(cur["id"]),
                payer_team_abbr=picking_team,
                receiver_team_abbr=losing_team,
                amount=RULEV_PICK_FEE,
                description=f"Rule V draft fee for {player_name}",
                effective_date=now[:10],
            )
        except Exception:
            current_app.logger.exception("Failed to post Rule V draft payment")

    return ("", 204)


if __name__ == "__main__":
    import argparse
    from flask import Flask

    parser = argparse.ArgumentParser(description="Rule V roster-sync utilities")
    parser.add_argument("--rulev-db", default=str(DEFAULT_DB), help="Path to rulev.db")
    parser.add_argument("--roster-db", default=str(APP_DIR / "roster.db"), help="Path to roster.db")
    parser.add_argument("--sync-roster", action="store_true", help="Refresh Rule V eligible pool from roster.db")
    args = parser.parse_args()

    app = Flask(__name__)
    app.config["RULEV_DB_PATH"] = args.rulev_db
    app.config["ROSTER_DB_PATH"] = args.roster_db
    with app.app_context():
        init_db()
        seed_default_order_if_empty()
        if args.sync_roster:
            print(sync_rulev_from_roster_db())
        else:
            print("Initialized rulev.db. Use --sync-roster to refresh eligibles from roster.db.")
