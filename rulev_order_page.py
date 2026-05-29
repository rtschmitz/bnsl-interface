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

# Rule V: fixed start time + normal hourly draft slots.
# Pick times are 9 AM through 6 PM ET, Sundays are skipped.
RULEV_START = datetime(2026, 3, 5, 9, 0, 0, tzinfo=EASTERN)
DAY_FIRST_HOUR = 9
DAY_LAST_HOUR = 18
SLOT_MINUTES = 60
PICKS_PER_DAY = DAY_LAST_HOUR - DAY_FIRST_HOUR + 1

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
        <div class="sub">Times shown in ET • Rule V missed picks are skipped; the draft simply moves on to the next scheduled pick.</div>
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


def next_non_sunday_date(d):
    while d.weekday() == 6:
        d = d + timedelta(days=1)
    return d


def validate_regular_pick_time(dt: datetime) -> datetime:
    """Return an Eastern, minute-clean time inside the normal 9 AM-6 PM Mon-Sat draft window."""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=EASTERN)
    else:
        dt = dt.astimezone(EASTERN)
    dt = dt.replace(second=0, microsecond=0)
    if dt.weekday() == 6:
        raise ValueError("Rule V pick times cannot be scheduled on Sunday")
    if dt.minute != 0:
        raise ValueError("Rule V pick times must be on the hour")
    if dt.hour < DAY_FIRST_HOUR or dt.hour > DAY_LAST_HOUR:
        raise ValueError("Rule V pick times must be between 9:00 AM and 6:00 PM ET")
    return dt


def next_regular_pick_slot(dt: datetime) -> datetime:
    """One hour later, rolling from after 6 PM to the next non-Sunday day at 9 AM ET."""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=EASTERN)
    else:
        dt = dt.astimezone(EASTERN)
    candidate = dt + timedelta(minutes=SLOT_MINUTES)
    if candidate.weekday() == 6 or candidate.hour > DAY_LAST_HOUR:
        nd = next_non_sunday_date(dt.date() + timedelta(days=1))
        return datetime(nd.year, nd.month, nd.day, DAY_FIRST_HOUR, 0, 0, tzinfo=EASTERN)
    return candidate.replace(second=0, microsecond=0)


def regular_pick_slots_from(start_dt: datetime, count: int) -> list[datetime]:
    slots: list[datetime] = []
    cur = validate_regular_pick_time(start_dt)
    for _ in range(max(0, int(count))):
        slots.append(cur)
        cur = next_regular_pick_slot(cur)
    return slots


def base_slot_for_index(idx: int) -> datetime:
    day = next_non_sunday_date(RULEV_START.date())
    cur = datetime(day.year, day.month, day.day, RULEV_START.hour, 0, 0, tzinfo=EASTERN)
    for _ in range(max(0, int(idx))):
        cur = next_regular_pick_slot(cur)
    return cur


def _ensure_pick_overrides_table(conn: sqlite3.Connection) -> None:
    cur = conn.cursor()
    cur.execute("""
      CREATE TABLE IF NOT EXISTS rulev_pick_overrides (
        rulev_order_id INTEGER PRIMARY KEY,
        scheduled_time TEXT NOT NULL
      )
    """)
    conn.commit()


def _load_picks_overrides_and_designated():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
      SELECT id, round, pick, team, player_id, drafted_at
      FROM rulev_order
      ORDER BY round ASC, pick ASC
    """)
    picks = cur.fetchall()
    _ensure_pick_overrides_table(conn)
    cur.execute("SELECT rulev_order_id, scheduled_time FROM rulev_pick_overrides")
    overrides_raw = cur.fetchall()
    conn.close()

    overrides: Dict[int, datetime] = {}
    for r in overrides_raw:
        try:
            dt = datetime.fromisoformat(r["scheduled_time"])
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=EASTERN)
            else:
                dt = dt.astimezone(EASTERN)
            if dt.weekday() != 6:
                overrides[int(r["rulev_order_id"])] = dt.replace(second=0, microsecond=0)
        except Exception:
            pass

    designated: List[datetime] = []
    for idx, rec in enumerate(picks):
        designated.append(overrides.get(int(rec["id"]), base_slot_for_index(idx)))
    return picks, designated


def _deadline_for_index(designated: list[datetime], idx: int) -> datetime:
    if idx + 1 < len(designated):
        return designated[idx + 1]
    return designated[idx] + timedelta(minutes=SLOT_MINUTES)


def get_current_on_clock_pick(now: Optional[datetime] = None) -> Optional[Dict[str, Any]]:
    """Return the first undrafted Rule V pick whose window has not expired; missed picks are skipped."""
    if now is None:
        now = datetime.now(tz=EASTERN)
    elif now.tzinfo is None:
        now = now.replace(tzinfo=EASTERN)
    else:
        now = now.astimezone(EASTERN)

    picks, designated = _load_picks_overrides_and_designated()
    for idx, rec in enumerate(picks):
        if rec["player_id"]:
            continue
        deadline = _deadline_for_index(designated, idx)
        if now < deadline:
            scheduled = designated[idx].astimezone(EASTERN)
            return {
                "id": int(rec["id"]),
                "round": int(rec["round"]),
                "pick": int(rec["pick"]),
                "team": rec["team"],
                "scheduled_time_iso": scheduled.isoformat(timespec="minutes"),
                "deadline_time_iso": deadline.astimezone(EASTERN).isoformat(timespec="minutes"),
            }
    return None


def set_pick_and_following_times(round_num: int, pick_num: int, start_dt: datetime) -> Dict[str, Any]:
    """Admin helper: set one Rule V pick's time and regenerate every later Rule V pick slot."""
    start_dt = validate_regular_pick_time(start_dt)
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
      SELECT id, round, pick, team, player_id, drafted_at
      FROM rulev_order
      ORDER BY round ASC, pick ASC
    """)
    picks = cur.fetchall()
    target_idx = None
    for idx, rec in enumerate(picks):
        if int(rec["round"]) == int(round_num) and int(rec["pick"]) == int(pick_num):
            target_idx = idx
            break
    if target_idx is None:
        conn.close()
        raise ValueError(f"No Rule V pick found for round {round_num}, pick {pick_num}")

    _ensure_pick_overrides_table(conn)
    following = picks[target_idx:]
    slots = regular_pick_slots_from(start_dt, len(following))
    cur.executemany(
        """
        INSERT INTO rulev_pick_overrides(rulev_order_id, scheduled_time)
        VALUES (?, ?)
        ON CONFLICT(rulev_order_id) DO UPDATE SET
            scheduled_time=excluded.scheduled_time
        """,
        [(int(rec["id"]), slot.isoformat(timespec="minutes")) for rec, slot in zip(following, slots)],
    )
    conn.commit()
    target = picks[target_idx]
    conn.close()
    return {
        "draft_kind": "rulev",
        "draft_name": "Rule V Draft",
        "round": int(round_num),
        "pick": int(pick_num),
        "pick_label": f"{int(round_num)}.{int(pick_num):02d}",
        "team": target["team"],
        "start_time": start_dt.isoformat(timespec="minutes"),
        "updated_count": len(following),
    }


def compute_rows(team_filter: Optional[str] = None) -> List[Dict[str, Any]]:
    now = datetime.now(tz=EASTERN)
    picks, designated = _load_picks_overrides_and_designated()

    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT id, name FROM rulev_players")
    player_name_by_id = {int(r["id"]): r["name"] for r in cur.fetchall()}
    conn.close()

    rows: List[Dict[str, Any]] = []
    for idx, rec in enumerate(picks):
        pick_label = f"{int(rec['round'])}.{int(rec['pick']):02d}"
        scheduled = designated[idx].astimezone(EASTERN)
        deadline = _deadline_for_index(designated, idx).astimezone(EASTERN)

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
            if now >= deadline:
                status = "Missed"
            elif now >= scheduled:
                status = "On clock"
            else:
                status = "Scheduled"
            rows.append({
                "pick_label": pick_label,
                "team": rec["team"],
                "player": None,
                "time_display": fmt_est(scheduled),
                "status": status,
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
