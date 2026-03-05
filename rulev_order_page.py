# rulev_order_page.py
from __future__ import annotations
import math
import sqlite3
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
from ui_skin import BNSL_GAME_CSS
from flask import Blueprint, current_app, request, jsonify, render_template_string
from zoneinfo import ZoneInfo

EASTERN = ZoneInfo("America/New_York")

# Rule V: keep it simple – fixed start time + hourly slots (no comp picks, 3 rounds)
RULEV_START = datetime(2026, 3, 5, 9, 0, 0, tzinfo=EASTERN)
SLOT_MINUTES = 60

rulev_order_bp = Blueprint("rulev_order_bp", __name__)

ORDER_HTML = r"""
<!doctype html>
<html>
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Rule V Draft Order</title>
__BNSL_GAME_CSS__
<style>
  /* Order-page-only tweaks */
  .controls { display:flex; gap:12px; align-items:center; flex-wrap:wrap; margin: 12px 0; }
  .pagination { margin-top: 14px; display:flex; gap: 10px; align-items:center; flex-wrap:wrap; }
  .navrow { display:flex; gap:12px; align-items:center; flex-wrap:wrap; }
</style>
</head>
<body>
  <div class="page">
    <div class="brand">
      <div>
        <h1>RULE V ORDER</h1>
        <div class="sub">Times shown in ET • Missed picks roll to the end of the day (7:00 PM). If that is missed, they roll to the end of the next day, and so on.</div>
      </div>
      <div class="right">
        <a class="btn" href="/rulev/">← Back</a>
        <span class="badge">SCHEDULE</span>
      </div>
    </div>

    <div class="panel pad">

      <form class="controls" method="get" action="/rulev/order">
        <label class="pill" style="background: rgba(0,0,0,.16);">
          <span style="margin-right:8px;">Filter by Team:</span>
          <select name="team" onchange="this.form.submit()">
            <option value="">All Teams</option>
            {% for t in teams %}
              <option value="{{ t }}" {% if t == team %}selected{% endif %}>{{ t }}</option>
            {% endfor %}
          </select>
        </label>
        <input type="hidden" name="per" value="{{ per }}">
        <input type="hidden" name="page" value="1">
      </form>

      <hr class="sep"/>

      <div class="table-wrap">
        <table>
          <thead>
            <tr>
              <th style="width:14%;">Pick</th>
              <th style="width:26%;">Team</th>
              <th style="width:34%;">Time / Player</th>
              <th style="width:26%;">Status</th>
            </tr>
          </thead>
          <tbody>
            {% for row in rows %}
            <tr class="row-hover">
              <td><b>{{ row.pick_label }}</b></td>
              <td>{{ row.team }}</td>
              <td>
                {% if row.player %}
                  {{ row.player }}
                {% else %}
                  {{ row.time_display }}
                {% endif %}
              </td>
              <td class="muted">{{ row.status }}</td>
            </tr>
            {% endfor %}
          </tbody>
        </table>
      </div>

      <div class="pagination">
        <form method="get" class="navrow">
          <input type="hidden" name="per" value="{{ per }}">
          <input type="hidden" name="team" value="{{ team }}">
          <button class="btn" name="page" value="{{ prev_page }}" {% if prev_page < 1 %}disabled{% endif %}>Prev</button>
          <span class="pill">Page <b>{{ page }}</b> / <b>{{ pages }}</b></span>
          <button class="btn" name="page" value="{{ next_page }}" {% if next_page > pages %}disabled{% endif %}>Next</button>
        </form>
      </div>

    </div> <!-- /panel -->
  </div>   <!-- /page -->
</body>
</html>
"""
ORDER_HTML = ORDER_HTML.replace("__BNSL_GAME_CSS__", BNSL_GAME_CSS)



def get_conn() -> sqlite3.Connection:
    # Rule V DB key
    conn = sqlite3.connect(current_app.config["RULEV_DB_PATH"])
    conn.row_factory = sqlite3.Row
    return conn


def get_all_teams() -> list[str]:
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT DISTINCT team FROM rulev_order ORDER BY team COLLATE NOCASE ASC")
    teams = [r[0] for r in cur.fetchall() if r[0]]
    conn.close()
    return teams


def fmt_est(dt: datetime) -> str:
    # Portable formatting: avoid %-d / %-I on platforms that might not support it
    s = dt.astimezone(EASTERN).strftime("%a %b %d, %Y • %I:%M %p ET")
    return s.replace(" 0", " ")  # cosmetic: strip leading zeros


def compute_rows(team_filter: Optional[str] = None) -> List[Dict[str, Any]]:
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
      SELECT id, round, pick, team, player_id, drafted_at
      FROM rulev_order
      ORDER BY round ASC, pick ASC
    """)
    picks = cur.fetchall()

    # Map player_id -> name
    cur.execute("SELECT id, name FROM rulev_players")
    player_name_by_id = {int(r["id"]): r["name"] for r in cur.fetchall()}
    conn.close()

    rows: List[Dict[str, Any]] = []
    for idx, rec in enumerate(picks):
        # pick label like "1.01" "2.03" etc
        pick_label = f"{int(rec['round'])}.{int(rec['pick']):02d}"
        scheduled = RULEV_START + timedelta(minutes=SLOT_MINUTES * idx)

        if rec["player_id"]:
            pname = player_name_by_id.get(int(rec["player_id"]), f"Player #{rec['player_id']}")
            rows.append({
                "pick_label": pick_label,
                "team": rec["team"],
                "player": pname,
                "time_display": "",
                "status": f"Selected at {rec['drafted_at'] or '—'}",
            })
        else:
            rows.append({
                "pick_label": pick_label,
                "team": rec["team"],
                "player": None,
                "time_display": fmt_est(scheduled),
                "status": "Scheduled",
            })

    if team_filter:
        rows = [r for r in rows if r["team"] == team_filter]
    return rows


@rulev_order_bp.route("/order")
def order_page():
    # pagination
    try:
        page = max(1, int(request.args.get("page", "1")))
    except ValueError:
        page = 1
    try:
        per = max(5, min(50, int(request.args.get("per", "25"))))
    except ValueError:
        per = 25

    team = (request.args.get("team") or "").strip()

    rows = compute_rows(team_filter=team or None)
    total = len(rows)
    pages = max(1, math.ceil(total / per))
    page = min(page, pages)

    start = (page - 1) * per
    end = start + per
    page_rows = rows[start:end]

    return render_template_string(
        ORDER_HTML,
        rows=page_rows,
        page=page, per=per, pages=pages,
        prev_page=page - 1, next_page=page + 1,
        teams=get_all_teams(),
        team=team,
        start_display=fmt_est(RULEV_START),
    )


@rulev_order_bp.get("/api/order")
def api_order():
    try:
        page = max(1, int(request.args.get("page", "1")))
    except ValueError:
        page = 1
    try:
        per = max(5, min(100, int(request.args.get("per", "50"))))
    except ValueError:
        per = 50

    team = (request.args.get("team") or "").strip()

    rows = compute_rows(team_filter=team or None)
    total = len(rows)
    pages = max(1, math.ceil(total / per))
    page = min(page, pages)

    start = (page - 1) * per
    end = start + per

    return jsonify({
        "page": page,
        "per": per,
        "pages": pages,
        "total": total,
        "team": team,
        "rows": rows[start:end],
        "teams": get_all_teams(),
    })
