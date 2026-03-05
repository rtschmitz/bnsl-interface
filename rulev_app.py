#!/usr/bin/env python3
from __future__ import annotations

import os
import sqlite3
from pathlib import Path
from datetime import datetime
from typing import Any, Dict, List

from flask import Blueprint, current_app, request, jsonify, session, render_template_string, abort

# Reuse teams + emails from your main draft app
from draft_app import MLB_TEAMS, TEAM_EMAILS, emails_equal

rulev_bp = Blueprint("rulev", __name__)

APP_DIR = Path(__file__).resolve().parent
DEFAULT_DB = APP_DIR / "rulev.db"


def get_db_path() -> Path:
    cfg = current_app.config.get("RULEV_DB_PATH")
    return Path(cfg) if cfg else DEFAULT_DB


def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(str(get_db_path()))
    conn.row_factory = sqlite3.Row
    return conn


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


def db_empty() -> bool:
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM rulev_players")
    n = int(cur.fetchone()[0] or 0)
    conn.close()
    return n == 0


def seed_demo_data():
    """
    3 rounds, arbitrary order and arbitrary player pool
    """
    init_db()
    if not db_empty():
        return

    # Arbitrary 10-team order (use any subset you like)
    teams = [
        "Oakland Athletics", "Kansas City Royals", "Colorado Rockies", "Washington Nationals", "Miami Marlins",
        "Pittsburgh Pirates", "Detroit Tigers", "Chicago White Sox", "Arizona Diamondbacks", "Los Angeles Angels",
    ]

    # 3 rounds, straight order, no comp picks
    conn = get_conn()
    cur = conn.cursor()

    # seed players
    demo_players = [
        ("Luis Araujo", "SS", "NYY"),
        ("Marco Diaz", "OF", "LAD"),
        ("Ethan Park", "SP", "BOS"),
        ("Javier Santos", "C", "TBR"),
        ("Noah Kim", "2B", "SEA"),
        ("Diego Alvarez", "3B", "ATL"),
        ("Ryan Chen", "RP", "HOU"),
        ("Mateo Rivera", "OF", "MIL"),
        ("Caleb Johnson", "SP", "CLE"),
        ("Isaac Patel", "SS", "TOR"),
        ("Ben Thompson", "OF", "STL"),
        ("Adrian Flores", "SP", "CHC"),
        ("Tyler Nguyen", "C", "SDP"),
        ("Samir Hassan", "2B", "SFG"),
        ("Owen Brooks", "RP", "MIN"),
        ("Jordan Price", "3B", "NYM"),
        ("Leo Simmons", "OF", "PHI"),
        ("Carter Bell", "SP", "TEX"),
        ("Julian Ortiz", "SS", "BAL"),
        ("Elias Gomez", "RP", "LAA"),
        ("Grayson Lee", "SP", "DET"),
        ("Hudson Clark", "SP", "COL"),
        ("Dominic Ward", "OF", "PIT"),
        ("Anthony Ross", "OF", "WSN"),
        ("Nolan Baker", "C", "MIA"),
        ("Asher Perry", "RP", "OAK"),
        ("Ezra Watson", "2B", "KCR"),
        ("Aaron Foster", "3B", "CHW"),
        ("Lucas Patel", "SP", "ARI"),
        ("Michael Hughes", "OF", "SEA"),
    ]
    cur.executemany("INSERT INTO rulev_players(name, position, org) VALUES(?,?,?)", demo_players)

    # seed order
    rows = []
    for r in range(1, 4):  # 3 rounds
        for p, team in enumerate(teams, start=1):
            rows.append((r, p, team))
    cur.executemany("INSERT OR IGNORE INTO rulev_order(round, pick, team) VALUES(?,?,?)", rows)

    conn.commit()
    conn.close()


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
  <style>
    body { font-family: system-ui; margin: 24px; }
    .topbar { display:flex; flex-wrap:wrap; gap:12px; align-items:center; margin-bottom: 12px; }
    .pill { padding:6px 10px; border-radius:999px; background:#f2f2f2; display:inline-flex; gap:8px; align-items:center; }
    .btn { padding:6px 10px; border:1px solid #333; background:#fff; border-radius:6px; cursor:pointer; }
    .btn[disabled]{opacity:.5; cursor:not-allowed;}
    .muted { color:#666; }
    .badge { font-size:12px; background:#eef7ff; color:#184a7d; border:1px solid #cfe5ff; padding:2px 6px; border-radius:4px; }
    table { border-collapse: collapse; width: 100%; }
    th, td { border-bottom:1px solid #e5e5e5; padding:8px 10px; text-align:left; }
    th { background:#fafafa; position:sticky; top:0; z-index:1; }
    tr.row-hover:hover { background:#fcfcfc; }
    .taken { opacity: 0.5; }
  </style>
</head>
<div class="pill" style="display:inline-block; margin-bottom:12px;">
  <a href="/rulev/order" style="text-decoration:none; color:#184a7d;">View Draft Order & Times →</a>
</div>
<body>
  <h1>Rule V Draft</h1>

  <div class="pill" id="status">
    <span>Current Pick:</span>
    <span id="cur">Loading…</span>
    <span id="prog" class="badge"></span>
  </div>

  <div class="topbar">
    <label class="pill">Your Team:
      <select id="team"></select>
    </label>
    <button class="btn" id="login">Login</button>
    <div class="pill" style="margin-left:auto;">
      <span>Search:</span>
      <input id="search" type="text" placeholder="Type a player name…" style="border:1px solid #ddd; padding:6px 8px; border-radius:6px; min-width:260px;" />
      <span class="muted">(substring match)</span>
    </div>
  </div>

  <div class="pill"><span id="login-status">🔒 Not logged in</span></div>

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
      btn.className = 'btn';
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
</body>
</html>
"""


@rulev_bp.before_app_request
def _bootstrap_once():
    # cheap, safe: ensure db exists/seeded before first request
    # (idempotent; only seeds if empty)
    seed_demo_data()


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
    if q:
        cur.execute("""
          SELECT * FROM rulev_players
          WHERE LOWER(name) LIKE ?
          ORDER BY name COLLATE NOCASE ASC
        """, (f"%{q}%",))
    else:
        cur.execute("SELECT * FROM rulev_players ORDER BY name COLLATE NOCASE ASC")
    rows = cur.fetchall()
    conn.close()

    players = []
    for r in rows:
        players.append({
            "id": int(r["id"]),
            "name": r["name"],
            "position": r["position"],
            "org": r["org"],
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
    c.execute("SELECT drafted_by FROM rulev_players WHERE id=?", (player_id,))
    r = c.fetchone()
    if not r:
        conn.close()
        return ("Player not found", 404)
    if (r["drafted_by"] or "").strip():
        conn.close()
        return ("Player already picked", 409)

    now = datetime.utcnow().isoformat(timespec="seconds")

    # assign player + mark order
    c.execute("UPDATE rulev_players SET drafted_by=?, drafted_at=? WHERE id=?", (team, now, player_id))
    c.execute("UPDATE rulev_order SET player_id=?, drafted_at=? WHERE id=?", (player_id, now, cur["id"]))
    conn.commit()
    conn.close()

    return ("", 204)
