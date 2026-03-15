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
    "TBR": "smith.mark.louis@gmail.com",
    "BAL": "bsweis@ptd.net",
    "DET": "manconley@gmail.com",
    "KCR": "jim@timhafer.com",
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
    "SFG": "jasonmallet@gmail.com",
    "SDP": "mattaca77@gmail.com",
}

TEAM_ABBRS = sorted(TEAM_EMAILS.keys())

POSITIONS = ["P","C","1B","2B","3B","SS","LF","CF","RF","DH","IF","OF"]
CONTRACT_TYPES = ["R","A","X","FA"]
STATUS_TYPES = ["40-man","Reserve"]
FA_CLASSES = ["2026","2027","2028","2029","2030"]


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


def roster_status_from_csv(status: str) -> str:
    s = (status or "").strip().lower()
    if s in ("active", "expanded"):
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
    contract_type = (row["contract_type"] or "").strip().upper()
    service_2025 = float(row["service_time_2025"] or 0.0)

    if contract_type in ("R", "A", ""):
        for year in range(2026, 2036):
            projected = service_2025 + (year - 2025)
            if projected > 6.0:
                return str(year)
        return ""

    if contract_type in ("X", "FA"):
        expires = row["contract_expires"]
        if expires:
            try:
                exp_year = int(str(expires)[:4])
                return str(exp_year + 1)
            except Exception:
                pass

        initial = row["contract_initial_season"]
        length = row["contract_length"]
        if initial and length:
            try:
                return str(int(initial) + int(length))
            except Exception:
                pass

    return ""


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
            options_remaining INTEGER,
            fangraphs_id TEXT,
            mlbam_id INTEGER
        )
    """)

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

                    def as_int(val, default=None):
                        s = (val or "").strip()
                        if s == "":
                            return default
                        try:
                            return int(float(s))
                        except Exception:
                            return default

                    def as_float(val, default=0.0):
                        s = (val or "").strip()
                        if s == "":
                            return default
                        try:
                            return float(s)
                        except Exception:
                            return default

                    def as_bool(val):
                        return str(val).strip().lower() == "true"

                    cur.execute("""
                        INSERT INTO roster_players (
                            id, name, last_name, first_name, suffix, nickname,
                            position, date_of_birth, bats, throws, signed,
                            contract_type, salary, contract_initial_season,
                            contract_length, contract_option, contract_expires,
                            service_time, previous_service_time, service_time_2025,
                            franchise, affiliate_team, roster_status,
                            options_remaining, fangraphs_id, mlbam_id
                        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
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
                        (r.get("contract_type") or "").strip(),
                        as_float(r.get("salary"), 0.0),
                        as_int(r.get("contract_initial_season")),
                        as_int(r.get("contract_length")),
                        1 if as_bool(r.get("contract_option")) else 0,
                        (r.get("contract_expires") or "").strip(),
                        as_float(r.get("service_time"), 0.0),
                        as_float(r.get("previous_service_time"), 0.0),
                        as_float(r.get("service_time_2025"), 0.0),
                        (r.get("franchise") or "").strip(),
                        (r.get("team") or "").strip(),
                        roster_status_from_csv(r.get("status")),
                        as_int(r.get("options_remaining"), 0),
                        (r.get("fangraphs_id") or "").strip(),
                        as_int(r.get("mlbam_id")),
                    ))

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
    conn.close()

    out = []
    for r in rows:
        row_fa_class = compute_fa_class(r)
        row_rulev = rulev_eligible(r["roster_status"], r["date_of_birth"])

        if rulev_only and not row_rulev:
            continue
        if fa_class_filter and row_fa_class != fa_class_filter:
            continue

        out.append({
            "id": r["id"],
            "name": r["name"],
            "position": r["position"],
            "team": r["franchise"] or "",
            "dob": r["date_of_birth"],
            "bt": bt_text(r["bats"], r["throws"]),
            "roster_status": r["roster_status"],
            "contract_type": r["contract_type"] or "",
            "service_time": round(float(r["service_time"] or 0), 2),
            "salary_m": round(float(r["salary"] or 0) / 1_000_000.0, 2),
            "team_opt": bool(int(r["contract_option"] or 0)),
            "fa_class": row_fa_class,
            "options_remaining": int(r["options_remaining"] or 0),
            "rulev_eligible": row_rulev,
            "can_edit_status": (
              session.get("roster_authed_team", "") != "" and
              (r["franchise"] or "") == session.get("roster_authed_team", "")),
        })
        forty_man_count = None
        if team:
            conn2 = get_conn()
            cur2 = conn2.cursor()
            cur2.execute("""
                SELECT COUNT(*)
                FROM roster_players
                WHERE franchise = ? AND roster_status = '40-man'
            """, (team,))
            forty_man_count = int(cur2.fetchone()[0] or 0)
            conn2.close()

    return jsonify({
        "players": out,
        "teams": TEAM_ABBRS,
        "contract_types": CONTRACT_TYPES,
        "positions": POSITIONS,
        "status_types": STATUS_TYPES,
        "fa_classes": FA_CLASSES,
        "forty_man_count": forty_man_count,

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

    if new_status not in ("40-man", "Reserve"):
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

    # only block promotions to 40-man
    if old_status != "40-man" and new_status == "40-man":
        cur.execute("""
            SELECT COUNT(*)
            FROM roster_players
            WHERE franchise = ? AND roster_status = '40-man'
        """, (team,))
        forty_count = int(cur.fetchone()[0] or 0)

        if forty_count >= 40:
            conn.close()
            return ("Cannot add player to 40-man: team already has 40 players on the 40-man roster", 409)

    cur.execute("""
        UPDATE roster_players
        SET roster_status=?
        WHERE id=?
    """, (new_status, player_id))

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
  fortyManCount.textContent = `${{data.forty_man_count}}/40 on 40-man`;
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
      const sel40 = p.roster_status === "40-man" ? "selected" : "";
      const selRes = p.roster_status === "Reserve" ? "selected" : "";
      statusHtml =
        '<select class="status-edit" data-player-id="' + p.id + '">' +
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
      <td>${{moneyM(p.salary_m)}}</td>
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
