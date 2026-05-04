# draft_order_page.py
from __future__ import annotations
import math
import sqlite3
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Any, Optional
from ui_skin import BNSL_GAME_CSS
from flask import Blueprint, current_app, request, jsonify, render_template_string

from zoneinfo import ZoneInfo  # Python 3.9+
EASTERN = ZoneInfo("America/New_York")


# ===== Time rules =====
DRAFT_START = datetime(2025, 10, 20, 9, 0, 0, tzinfo=EASTERN)

# Draft window: 9am..6pm inclusive (10 normal picks/day), then the "end-of-day miss slot" at 7pm
DAY_FIRST_HOUR = 9
DAY_LAST_HOUR = 18  # 6pm
END_OF_DAY_MISS_HOUR = 19  # 7pm
PICKS_PER_DAY = DAY_LAST_HOUR - DAY_FIRST_HOUR + 1  # 10/hourly slots per day

order_bp = Blueprint("order_bp", __name__)

# Small HTML (kept here to avoid new templates folder)
ORDER_HTML = r"""
<!doctype html>
<html>
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Draft Order & Times</title>
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
        <h1>DRAFT ORDER</h1>
        <div class="sub">Times shown in ET • Missed picks roll to the end of the day (7:00 PM). If that is missed, they roll to the end of the next day, and so on.</div>
      </div>
      <div class="right">
        <a class="btn" href="/draft/">← Back</a>
        <a class="btn" href="/draft/pick-stock">Future Picks</a>
        <span class="badge">SCHEDULE</span>
      </div>
    </div>

    <div class="panel pad">

      <form class="controls" method="get" action="/draft/order">
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
              <th style="width:12%;">Pick</th>
              <th style="width:28%;">Team</th>
              <th style="width:30%;">Time / Player</th>
              <th style="width:30%;">Status</th>
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

def get_all_teams() -> list[str]:
    """Distinct teams present in draft_order, ordered A→Z."""
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT DISTINCT team FROM draft_order ORDER BY team COLLATE NOCASE ASC")
    teams = [r[0] for r in cur.fetchall() if r[0]]
    conn.close()
    return teams


def get_conn() -> sqlite3.Connection:
    # reuse the app's DB path via current_app.config
    conn = sqlite3.connect(current_app.config["DRAFT_DB_PATH"])
    conn.row_factory = sqlite3.Row
    return conn

# --- Sunday helpers ---
def is_sunday(d: datetime | date) -> bool:
    return (d.weekday() == 6)  # Monday=0 … Sunday=6

def next_non_sunday_date(d: date) -> date:
    while d.weekday() == 6:
        d = d + timedelta(days=1)
    return d

def bump_if_sunday(dt: datetime) -> datetime:
    # If a scheduled time lands on Sunday, bump to Monday at the same clock time.
    if dt.weekday() != 6:
        return dt
    nd = dt + timedelta(days=1)
    while nd.weekday() == 6:
        nd = nd + timedelta(days=1)
    return nd

def base_slot_for_index(i: int) -> datetime:
    """
    Initial designated time for pick index i (0-based), skipping Sundays entirely.
    9am..6pm hourly; after 6pm, go to the next non-Sunday day at 9am.
    """
    # How many full (non-Sunday) days ahead?
    full_days, offset_in_day = divmod(i, PICKS_PER_DAY)

    # Walk forward `full_days` non-Sunday days from DRAFT_START.date()
    day = DRAFT_START.date()
    # Ensure the start itself isn’t Sunday
    day = next_non_sunday_date(day)
    advanced = 0
    while advanced < full_days:
        day = next_non_sunday_date(day + timedelta(days=1))
        if day.weekday() != 6:  # skip Sundays
            advanced += 1

    # Ensure target day itself isn’t Sunday (paranoia)
    day = next_non_sunday_date(day)

    slot_hour = DAY_FIRST_HOUR + offset_in_day
    return datetime(day.year, day.month, day.day, slot_hour, 0, 0, tzinfo=EASTERN)


def init_db():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS players (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            dob TEXT,
            position TEXT,
            franchise TEXT,
            eligible INTEGER NOT NULL DEFAULT 1
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS draft_order (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            round INTEGER NOT NULL,
            pick INTEGER NOT NULL,
            team TEXT NOT NULL,
            player_id INTEGER,
            drafted_at TEXT,
            label TEXT,                         -- NEW: human display (e.g., 'C2.01')
            UNIQUE(round, pick) ON CONFLICT IGNORE
        )
        """
    )
    # Ensure 'label' exists if table pre-existed
    cur.execute("PRAGMA table_info(draft_order)")
    cols = {row[1] for row in cur.fetchall()}
    if "label" not in cols:
        cur.execute("ALTER TABLE draft_order ADD COLUMN label TEXT")
    conn.commit()
    conn.close()


def end_of_day(dt: datetime) -> datetime:
    """Return the end-of-day miss slot at 7pm for the date of dt."""
    d = dt.date()
    return datetime(d.year, d.month, d.day, END_OF_DAY_MISS_HOUR, 0, 0, tzinfo=EASTERN)

def end_of_next_day(dt: datetime) -> datetime:
    """7pm on the next calendar day."""
    return end_of_day(dt + timedelta(days=1))

def fmt_est(dt: datetime) -> str:
    return dt.strftime("%a %b %-d, %Y • %-I:%M %p ET")

def compute_rows(now: Optional[datetime] = None, team_filter: Optional[str] = None) -> List[Dict[str, Any]]:
    """
    Show rows using the SAME scheduler used by enforcement:
      • Time displayed = _compute_scheduled_times(now)
      • 'Missed → end of day' label still based on DESIGNATED deadlines
    """
    if now is None:
        now = datetime.now(tz=EASTERN)

    # Load picks & designated (override-or-base)
    picks, designated = _load_picks_overrides_and_designated()

    # Name lookup for drafted rows
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT id, name FROM players")
    player_name_by_id = {r["id"]: r["name"] for r in cur.fetchall()}
    conn.close()

    # Use the SAME scheduler the app uses elsewhere
    scheduled = _compute_scheduled_times(now)

    # For the “missed” label, use the order-page rule (deadline = next DESIGNATED time)
    next_deadlines = _next_deadlines_from_designated(designated)

    rows: List[Dict[str, Any]] = []
    for i, rec in enumerate(picks):
        pick_label = rec["label"] or f"{rec['round']}.{rec['pick']}"
        if rec["player_id"]:
            rows.append({
                "pick_label": pick_label,
                "team": rec["team"],
                "player": player_name_by_id.get(rec["player_id"], f"Player #{rec['player_id']}"),
                "time_display": "",
                "status": f"Selected at {rec['drafted_at'] or '—'}",
            })
            continue

        # Always display the unified scheduled time (this rolls evening re-misses to tomorrow)
        t = scheduled.get(i, designated[i])
        # Status string for context (unchanged logic)
        status_txt = "Missed → end of day" if (now >= next_deadlines[i]) else "Scheduled"

        rows.append({
            "pick_label": pick_label,
            "team": rec["team"],
            "player": None,
            "time_display": fmt_est(t),
            "status": status_txt,
        })

    if team_filter:
        rows = [r for r in rows if r["team"] == team_filter]

    return rows



def get_current_on_clock_pick(now: Optional[datetime] = None) -> Optional[Dict[str, Any]]:
    """
    Current pick = the undrafted pick with the *earliest scheduled time*
    (considering overrides, misses -> evening queue, and re-miss carryover).
    Managers can pick early, so we return it even if its scheduled time is in the future.
    """
    if now is None:
        now = datetime.now(tz=EASTERN)

    picks, designated = _load_picks_overrides_and_designated()
    scheduled_time = _compute_scheduled_times(now)

    # Among undrafted picks, select the one with the minimum scheduled_time; tie-breaker: draft order
    best_idx = None
    best_key = None
    for i, rec in enumerate(picks):
        if rec["player_id"]:
            continue
        t = scheduled_time.get(i, designated[i])
        key = (t, i)
        if best_key is None or key < best_key:
            best_key = key
            best_idx = i

    if best_idx is None:
        return None

    rec = picks[best_idx]
    return {"id": rec["id"], "round": rec["round"], "pick": rec["pick"], "team": rec["team"]}


def get_current_pick_info(now: Optional[datetime] = None) -> Optional[Dict[str, Any]]:
    """
    Returns info about the *current* pick by earliest scheduled time, plus its deadline:
      {
        "id", "round", "pick", "team",
        "pick_label",           # e.g., "3.3"
        "scheduled_time_iso",   # ISO 8601, Eastern local
        "deadline_time_iso"     # ISO 8601, Eastern local (designated time of next pick in ORDER)
      }
    Managers can pick early; we still return the earliest-by-time pick even if its scheduled time is future.
    """
    if now is None:
        now = datetime.now(tz=EASTERN)

    picks, designated = _load_picks_overrides_and_designated()
    if not picks:
        return None

    scheduled_time = _compute_scheduled_times(now)

    best_idx = None
    best_key = None
    for i, rec in enumerate(picks):
        if rec["player_id"]:
            continue
        t = scheduled_time.get(i, designated[i])
        key = (t, i)
        if best_key is None or key < best_key:
            best_key = key
            best_idx = i

    if best_idx is None:
        return None

    rec = picks[best_idx]
    lbl = f"{rec['round']}.{rec['pick']}"
    sched = (scheduled_time.get(best_idx, designated[best_idx])).astimezone(EASTERN)

    if best_idx + 1 < len(picks):
        deadline = designated[best_idx + 1].astimezone(EASTERN)
        deadline_iso = deadline.isoformat(timespec="minutes")
    else:
        deadline_iso = None

    return {
        "id": rec["id"],
        "round": rec["round"],
        "pick": rec["pick"],
        "team": rec["team"],
        "pick_label": lbl,
        "scheduled_time_iso": sched.isoformat(timespec="minutes"),
        "deadline_time_iso": deadline_iso,
    }


# --- Shared scheduling helpers for main draft page ---

def _load_picks_overrides_and_designated():
    """Return (picks_rows, designated_times:list[datetime]) where designated=override or base slot."""
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
      SELECT id, round, pick, team, player_id, drafted_at, label
      FROM draft_order
      ORDER BY round ASC, pick ASC
    """)    
    picks = cur.fetchall()

    # load overrides
    cur.execute("""
        CREATE TABLE IF NOT EXISTS pick_overrides (
            draft_order_id INTEGER PRIMARY KEY,
            scheduled_time TEXT NOT NULL
        )
    """)
    conn.commit()
    cur.execute("SELECT draft_order_id, scheduled_time FROM pick_overrides")
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
            dt = bump_if_sunday(dt)

            overrides[r["draft_order_id"]] = dt
        except Exception:
            pass

    designated: List[datetime] = []
    for idx, rec in enumerate(picks):
        bt = base_slot_for_index(idx)
        designated.append(overrides.get(rec["id"], bt))
    return picks, designated

def _next_deadlines_from_designated(designated: list[datetime]) -> list[datetime]:
    nd = []
    for i, t in enumerate(designated):
        if i + 1 < len(designated):
            nd.append(designated[i + 1])
        else:
            nd.append(t + timedelta(days=36500))
    return nd

def _compute_scheduled_times(now: datetime) -> Dict[int, datetime]:
    """
    For each undrafted pick (by index), compute the *current* scheduled time:
      - Start at designated times (override or base).
      - If 'missed' (now >= next pick's designated time), move to evening queue:
          same day 7pm, 8pm, ... in miss order.
      - If an evening slot is also missed, cascade to the *next day's* evening tail.
      - Always return a time that is >= now or the next valid slot in the future.

    NOTE: If an evening slot was yesterday (or earlier), and 'now' is already a later date,
          we treat it as re-missed immediately and roll it to today's evening tail (no need to
          wait until 9:00 AM). This fixes the "DET sticks at yesterday 7pm after NYY drafted" case.
    """
    picks, designated = _load_picks_overrides_and_designated()
    next_deadlines = _next_deadlines_from_designated(designated)

    undrafted_idxs = [i for i, r in enumerate(picks) if not r["player_id"]]

    # Initial classification: which picks are already 'missed' by their next designated deadline
    missed_by_day: Dict[tuple[int, int, int], List[int]] = {}
    scheduled_time: Dict[int, datetime] = {}

    for i in undrafted_idxs:
        if now >= next_deadlines[i]:
            d = designated[i].astimezone(EASTERN).date()
            missed_by_day.setdefault((d.year, d.month, d.day), []).append(i)
        else:
            scheduled_time[i] = designated[i]

    # Choose the first day to process: earliest day with misses, else earliest designated day, else start date
    if missed_by_day:
        y, m, d = sorted(missed_by_day.keys())[0]
        earliest_day = next_non_sunday_date(datetime(y, m, d, tzinfo=EASTERN).date())
    elif designated:
        earliest_day = next_non_sunday_date(min(designated).astimezone(EASTERN).date())
    else:
        earliest_day = next_non_sunday_date(DRAFT_START.date())

    day = earliest_day
    carryover_eod: List[int] = []

    # Local "today" used for previous-day re-miss detection
    now_local_date = now.astimezone(EASTERN).date()

    safety_days = 0
    total_needed = len(undrafted_idxs)
    while len(scheduled_time) < total_needed and safety_days < 3650:
        todays_misses = missed_by_day.get((day.year, day.month, day.day), [])
        todays_misses.sort(key=lambda idx: designated[idx])  # stable within-day by original designated time

        # Keep previously scheduled carryovers first (keep 7pm/8pm stable), append same-day misses after them.
        evening_queue = carryover_eod + todays_misses
        new_carryover: List[int] = []

        for j, idx in enumerate(evening_queue):
            # Proposed evening slot for this item on 'day'
            slot_dt = bump_if_sunday(
                datetime(day.year, day.month, day.day, END_OF_DAY_MISS_HOUR + j, 0, 0, tzinfo=EASTERN)
            )


            # Evening picks are one-hour windows: deadline is always start + 1 hour
            next_deadline = slot_dt + timedelta(hours=1)

            # Re-miss rule:
            #  - If the slot was on a previous calendar day relative to 'now', it is already re-missed.
            #  - Else (same day), compare 'now' to the computed next_deadline as usual.
            slot_date = slot_dt.astimezone(EASTERN).date()
            re_missed = (slot_date < now_local_date) or (now >= next_deadline)

            if re_missed:
                new_carryover.append(idx)  # will be placed at the next day's evening tail
            else:
                scheduled_time[idx] = slot_dt

        carryover_eod = new_carryover
        day = next_non_sunday_date(day + timedelta(days=1))
        safety_days += 1

    # Any leftovers → next non-Sunday day's evening tail
    if carryover_eod:
        nd = next_non_sunday_date(day)
        for j, idx in enumerate(carryover_eod):
            scheduled_time[idx] = bump_if_sunday(
                datetime(nd.year, nd.month, nd.day, END_OF_DAY_MISS_HOUR + j, 0, 0, tzinfo=EASTERN)
            )

    return scheduled_time


# --------- Routes ----------

@order_bp.route("/order")
def order_page():
    # opportunistic enforcement so opening this page processes outstanding picks
    try:
        from draft_app import enforce_queue_actions  # local import avoids circular at module import time
        enforce_queue_actions()
    except Exception as _e:
        # log to Flask logger
        try:
            current_app.logger.exception("[order] enforce_queue_actions failed: %s", _e)
        except Exception:
            pass
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

    # compute full rows then filter by team (inside compute_rows)
    rows = compute_rows(team_filter=team or None)
    total = len(rows)
    pages = max(1, math.ceil(total / per))
    page = min(page, pages)

    start = (page - 1) * per
    end = start + per
    page_rows = rows[start:end]

    teams = get_all_teams()

    return render_template_string(
        ORDER_HTML,
        rows=page_rows,
        page=page, per=per, pages=pages,
        prev_page=page - 1, next_page=page + 1,
        teams=teams, team=team
    )


@order_bp.get("/api/order")
def api_order():
# opportunistic enforcement so opening this page processes outstanding picks
    try:
        from draft_app import enforce_queue_actions  # local import avoids circular at module import time
        enforce_queue_actions()
    except Exception as _e:
        # log to Flask logger
        try:
            current_app.logger.exception("[order] enforce_queue_actions failed: %s", _e)
        except Exception:
            pass
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



# --------- Future draft-pick stock routes ----------
# This is intentionally separate from the live 2025 draft_order table.  The
# draft_pick_stock table is rebuilt from trades.txt by trades_app and only tracks
# future picks (2026+) so completed drafts are not mutated.

PICK_STOCK_HTML = r"""
<!doctype html>
<html>
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Future Draft Pick Stock</title>
  __BNSL_GAME_CSS__
  <style>
    .controls { display:flex; gap:12px; align-items:center; flex-wrap:wrap; margin: 12px 0; }
    .pill.small { font-size:12px; padding:7px 10px; }
  </style>
</head>
<body>
  <div class="page">
    <div class="brand">
      <div>
        <h1>FUTURE DRAFT PICKS</h1>
        <div class="sub">Current 2026+ 12-round draft-pick stock reconstructed from the trade log.</div>
      </div>
      <div class="right">
        <a class="btn" href="/draft/order">← Back to 2025 Order</a>
        <a class="btn" href="/trades/">Trades</a>
        <span class="badge">PICK STOCK</span>
      </div>
    </div>

    <div class="panel pad">
      <form class="controls" method="get" action="/draft/pick-stock">
        <label class="pill" style="background: rgba(0,0,0,.16);">
          <span style="margin-right:8px;">Year:</span>
          <select name="year" onchange="this.form.submit()">
            <option value="">All years</option>
            {% for y in years %}
              <option value="{{ y }}" {% if selected_year == y|string %}selected{% endif %}>{{ y }}</option>
            {% endfor %}
          </select>
        </label>

        <label class="pill" style="background: rgba(0,0,0,.16);">
          <span style="margin-right:8px;">Current Owner:</span>
          <select name="owner" onchange="this.form.submit()">
            <option value="">All Teams</option>
            {% for t in teams %}
              <option value="{{ t }}" {% if selected_owner == t %}selected{% endif %}>{{ team_label(t) }}</option>
            {% endfor %}
          </select>
        </label>
      </form>

      <hr class="sep"/>

      <div class="table-wrap">
        <table>
          <thead>
            <tr>
              <th style="width:10%;">Year</th>
              <th style="width:10%;">Round</th>
              <th style="width:24%;">Original Pick</th>
              <th style="width:24%;">Current Owner</th>
              <th>Last Movement</th>
            </tr>
          </thead>
          <tbody>
            {% for row in rows %}
            <tr class="row-hover">
              <td><b>{{ row.pick_year }}</b></td>
              <td>{{ row.pick_round }}</td>
              <td><b>{{ row.original_team_abbr }}</b> <span class="muted">{{ display_team(row.original_team_abbr) }}</span></td>
              <td><b>{{ row.current_owner_abbr }}</b> <span class="muted">{{ display_team(row.current_owner_abbr) }}</span></td>
              <td class="muted">
                {% if row.last_trade_date %}
                  {{ row.last_trade_date }} — {{ row.last_trade_title }}
                {% else %}
                  Original owner
                {% endif %}
              </td>
            </tr>
            {% endfor %}
            {% if not rows %}
              <tr><td colspan="5" class="muted">No future draft-stock rows are available yet. Open the Trades tab to rebuild from trades.txt, or check TRADES_LOG_PATH / DRAFT_STOCK_DB_PATH.</td></tr>
            {% endif %}
          </tbody>
        </table>
      </div>
    </div>
  </div>
</body>
</html>
""".replace("__BNSL_GAME_CSS__", BNSL_GAME_CSS)


def _pick_stock_db_path():
    from pathlib import Path
    configured = current_app.config.get("DRAFT_STOCK_DB_PATH")
    if configured:
        return Path(configured)
    return Path(current_app.config["DRAFT_DB_PATH"]).with_name("draft_stock.db")


def _pick_stock_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(_pick_stock_db_path())
    conn.row_factory = sqlite3.Row
    return conn


def _query_pick_stock(year: str = "", owner: str = "", limit: int = 5000):
    db_path = _pick_stock_db_path()
    if not db_path.exists():
        return []
    conn = _pick_stock_conn()
    params = []
    clauses = []
    if year:
        clauses.append("pick_year=?")
        params.append(int(year))
    if owner:
        clauses.append("current_owner_abbr=?")
        params.append(owner)
    where = "WHERE " + " AND ".join(clauses) if clauses else ""
    cur = conn.cursor()
    cur.execute(
        f"""
        SELECT *
        FROM draft_pick_stock
        {where}
        ORDER BY pick_year ASC, pick_round ASC, current_owner_abbr ASC, original_team_abbr ASC
        LIMIT ?
        """,
        (*params, limit),
    )
    rows = cur.fetchall()
    conn.close()
    return rows


def _pick_stock_years():
    db_path = _pick_stock_db_path()
    if not db_path.exists():
        return []
    conn = _pick_stock_conn()
    cur = conn.cursor()
    cur.execute("SELECT DISTINCT pick_year FROM draft_pick_stock ORDER BY pick_year ASC")
    years = [int(r[0]) for r in cur.fetchall()]
    conn.close()
    return years


@order_bp.get("/pick-stock")
def future_pick_stock_page():
    try:
        from trades_app import ABBR_TO_FULL, TEAM_ORDER, display_team, team_label, refresh_from_log
        refresh_from_log()
    except Exception as _e:
        try:
            current_app.logger.exception("[pick-stock] failed to refresh from trades log: %s", _e)
        except Exception:
            pass
        ABBR_TO_FULL = {}
        TEAM_ORDER = []
        display_team = lambda x: x or ""
        team_label = lambda x: x or ""

    selected_year = (request.args.get("year") or "").strip()
    selected_owner = (request.args.get("owner") or "").strip().upper()
    rows = _query_pick_stock(year=selected_year, owner=selected_owner)
    years = _pick_stock_years()

    return render_template_string(
        PICK_STOCK_HTML,
        rows=rows,
        years=years,
        teams=TEAM_ORDER,
        selected_year=selected_year,
        selected_owner=selected_owner,
        display_team=display_team,
        team_label=team_label,
    )


@order_bp.get("/api/pick-stock")
def api_future_pick_stock():
    try:
        from trades_app import refresh_from_log
        refresh_from_log()
    except Exception as _e:
        try:
            current_app.logger.exception("[api/pick-stock] failed to refresh from trades log: %s", _e)
        except Exception:
            pass
    selected_year = (request.args.get("year") or "").strip()
    selected_owner = (request.args.get("owner") or "").strip().upper()
    rows = [dict(r) for r in _query_pick_stock(year=selected_year, owner=selected_owner)]
    return jsonify({"rows": rows, "years": _pick_stock_years()})
