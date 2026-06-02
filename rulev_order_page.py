# rulev_order_page.py
from __future__ import annotations
import math
import sqlite3
from datetime import date, datetime, timedelta
from typing import List, Dict, Any, Optional
from ui_skin import BNSL_GAME_CSS
from flask import Blueprint, current_app, request, jsonify, render_template_string
from zoneinfo import ZoneInfo

EASTERN = ZoneInfo("America/New_York")

# Rule V: fixed start time + normal hourly draft slots.
# Pick times are 9 AM through 6 PM ET, Sundays are skipped.
# Missed normal-window picks get one end-of-day chance; if that is missed too,
# the pick is skipped instead of rolling to another day.
RULEV_START = datetime(2026, 3, 5, 9, 0, 0, tzinfo=EASTERN)
DAY_FIRST_HOUR = 9
DAY_LAST_HOUR = 18
END_OF_DAY_MISS_HOUR = 18
END_OF_DAY_MISS_MINUTE = 30
END_OF_DAY_MISS_INTERVAL_MINUTES = 30
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
        <div class="sub">Times shown in ET • Missed picks roll once to the end-of-day queue, starting at 6:30 PM ET in 30-minute slots. If that rescheduled pick is missed, it is skipped.</div>
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


def _coerce_eastern(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=EASTERN).replace(second=0, microsecond=0)
    return dt.astimezone(EASTERN).replace(second=0, microsecond=0)


def _parse_eastern(value: Any) -> Optional[datetime]:
    if value is None:
        return None
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return _coerce_eastern(datetime.fromisoformat(text))
    except Exception:
        return None


def _iso(dt: datetime) -> str:
    return _coerce_eastern(dt).isoformat(timespec="minutes")


def next_non_sunday_date(d: date) -> date:
    while d.weekday() == 6:
        d = d + timedelta(days=1)
    return d


def validate_regular_pick_time(dt: datetime) -> datetime:
    """Return an Eastern, minute-clean time inside the normal 9 AM-6 PM Mon-Sat draft window."""
    dt = _coerce_eastern(dt)
    if dt.weekday() == 6:
        raise ValueError("Rule V pick times cannot be scheduled on Sunday")
    if dt.minute != 0:
        raise ValueError("Rule V pick times must be on the hour")
    if dt.hour < DAY_FIRST_HOUR or dt.hour > DAY_LAST_HOUR:
        raise ValueError("Rule V pick times must be between 9:00 AM and 6:00 PM ET")
    return dt


def next_regular_pick_slot(dt: datetime) -> datetime:
    """One hour later, rolling from after 6 PM to the next non-Sunday day at 9 AM ET."""
    dt = _coerce_eastern(dt)
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


def evening_miss_slot(start_day: date, offset: int) -> datetime:
    """Return the offset-th end-of-day miss slot.

    Rule V end-of-day reschedules start at 6:30 PM ET and advance in
    30-minute increments: 6:30, 7:00, 7:30, 8:00, and so on.  If more
    picks are rescheduled than fit before midnight, overflow is placed at
    6:30 PM+ on the next non-Sunday evening.  That overflow is scheduling
    capacity, not a second-chance rollover after a missed evening slot.
    """
    start_minutes = END_OF_DAY_MISS_HOUR * 60 + END_OF_DAY_MISS_MINUTE
    interval = END_OF_DAY_MISS_INTERVAL_MINUTES
    slots_per_evening = max(1, ((24 * 60) - start_minutes + interval - 1) // interval)
    extra_days, slot_in_day = divmod(max(0, int(offset)), slots_per_evening)
    day = next_non_sunday_date(start_day)
    advanced = 0
    while advanced < extra_days:
        day = next_non_sunday_date(day + timedelta(days=1))
        advanced += 1
    total_minutes = start_minutes + slot_in_day * interval
    return datetime(day.year, day.month, day.day, total_minutes // 60, total_minutes % 60, 0, tzinfo=EASTERN)


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


def _ensure_pick_miss_state_table(conn: sqlite3.Connection) -> None:
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


def _load_pick_miss_state() -> Dict[int, Dict[str, Optional[datetime]]]:
    conn = get_conn()
    cur = conn.cursor()
    _ensure_pick_miss_state_table(conn)
    cur.execute("SELECT rulev_order_id, first_missed_at, rescheduled_time, skipped_at FROM rulev_pick_miss_state")
    rows = cur.fetchall()
    conn.close()
    return {
        int(r["rulev_order_id"]): {
            "first_missed_at": _parse_eastern(r["first_missed_at"]),
            "rescheduled_time": _parse_eastern(r["rescheduled_time"]),
            "skipped_at": _parse_eastern(r["skipped_at"]),
        }
        for r in rows
    }


def mark_rulev_pick_first_missed(rulev_order_id: int, rescheduled_time: datetime, when: Optional[datetime] = None) -> None:
    """Persist that a Rule V pick missed its normal window and moved to the evening tail."""
    when = _coerce_eastern(when or datetime.now(tz=EASTERN))
    rescheduled_time = _coerce_eastern(rescheduled_time)
    conn = get_conn()
    cur = conn.cursor()
    _ensure_pick_miss_state_table(conn)
    cur.execute("""
      INSERT INTO rulev_pick_miss_state(rulev_order_id, first_missed_at, rescheduled_time, skipped_at)
      VALUES (?, ?, ?, NULL)
      ON CONFLICT(rulev_order_id) DO UPDATE SET
        first_missed_at=COALESCE(rulev_pick_miss_state.first_missed_at, excluded.first_missed_at),
        rescheduled_time=COALESCE(rulev_pick_miss_state.rescheduled_time, excluded.rescheduled_time)
    """, (int(rulev_order_id), _iso(when), _iso(rescheduled_time)))
    conn.commit()
    conn.close()


def mark_rulev_pick_skipped(rulev_order_id: int, when: Optional[datetime] = None) -> None:
    """Persist that a Rule V pick missed its evening slot and is now skipped."""
    when = _coerce_eastern(when or datetime.now(tz=EASTERN))
    conn = get_conn()
    cur = conn.cursor()
    _ensure_pick_miss_state_table(conn)
    cur.execute("""
      INSERT INTO rulev_pick_miss_state(rulev_order_id, first_missed_at, rescheduled_time, skipped_at)
      VALUES (?, NULL, NULL, ?)
      ON CONFLICT(rulev_order_id) DO UPDATE SET skipped_at=excluded.skipped_at
    """, (int(rulev_order_id), _iso(when)))
    conn.commit()
    conn.close()


def clear_rulev_pick_miss_state(rulev_order_id: int) -> None:
    """Clear missed/skipped state, useful when an admin resets times or the pick is filled."""
    conn = get_conn()
    cur = conn.cursor()
    _ensure_pick_miss_state_table(conn)
    cur.execute("DELETE FROM rulev_pick_miss_state WHERE rulev_order_id=?", (int(rulev_order_id),))
    conn.commit()
    conn.close()


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
    _ensure_pick_miss_state_table(conn)
    cur.execute("SELECT rulev_order_id, scheduled_time FROM rulev_pick_overrides")
    overrides_raw = cur.fetchall()
    conn.close()

    overrides: Dict[int, datetime] = {}
    for r in overrides_raw:
        try:
            dt = _coerce_eastern(datetime.fromisoformat(r["scheduled_time"]))
            if dt.weekday() != 6:
                overrides[int(r["rulev_order_id"])] = dt
        except Exception:
            pass

    designated: List[datetime] = []
    for idx, rec in enumerate(picks):
        designated.append(overrides.get(int(rec["id"]), base_slot_for_index(idx)))
    return picks, designated


def _deadline_for_index(designated: list[datetime], idx: int) -> datetime:
    """Return the normal-window deadline for pick idx.

    Most picks expire when the next designated pick starts. For the last pick of
    a day, this still enforces the normal one-hour window rather than letting
    the pick stay live overnight; end-of-day reschedules use their own
    30-minute evening slots.
    """
    one_hour = designated[idx] + timedelta(minutes=SLOT_MINUTES)
    if idx + 1 < len(designated):
        return min(designated[idx + 1], one_hour)
    return one_hour


def _next_deadlines_from_designated(designated: list[datetime]) -> list[datetime]:
    return [_deadline_for_index(designated, i) for i in range(len(designated))]


def _is_evening_reschedule(idx: int, scheduled_time: datetime, designated: list[datetime]) -> bool:
    return _coerce_eastern(scheduled_time) != _coerce_eastern(designated[idx])


def _deadline_for_scheduled_index(idx: int, scheduled_time: datetime, designated: list[datetime]) -> datetime:
    scheduled_time = _coerce_eastern(scheduled_time)
    if _is_evening_reschedule(idx, scheduled_time, designated):
        return scheduled_time + timedelta(minutes=END_OF_DAY_MISS_INTERVAL_MINUTES)
    return _deadline_for_index(designated, idx)


def _evening_slot_day_for_designated_time(dt: datetime) -> date:
    """Return the evening-queue day for a missed normal-window pick."""
    return next_non_sunday_date(_coerce_eastern(dt).date())


def _evening_slot_offset_for_time(dt: datetime) -> int | None:
    """Return 0 for 6:30 PM, 1 for 7 PM, etc.; None if not an evening slot."""
    dt = _coerce_eastern(dt)
    start_minutes = END_OF_DAY_MISS_HOUR * 60 + END_OF_DAY_MISS_MINUTE
    dt_minutes = dt.hour * 60 + dt.minute
    delta = dt_minutes - start_minutes
    if delta < 0 or delta % END_OF_DAY_MISS_INTERVAL_MINUTES != 0:
        return None
    return delta // END_OF_DAY_MISS_INTERVAL_MINUTES


def _evening_slots_for_unpersisted_misses(
    now: datetime,
    picks: list[Any],
    designated: list[datetime],
    miss_state: Dict[int, Dict[str, Optional[datetime]]],
) -> Dict[int, datetime]:
    """Compute evening slots for normal-window misses that have not yet been persisted.

    Persisted first misses already occupy their evening slots.  This matters
    because enforcement usually persists one missed pick per pass; without
    accounting for those occupied slots, the next overdue pick would also be
    assigned the same 6:30 PM slot.  Instead, same-day misses are assigned 6:30 PM, 7:00 PM, 7:30 PM,
    ... in miss order.
    """
    now = _coerce_eastern(now)
    first_misses_by_day: Dict[tuple[int, int, int], List[int]] = {}
    occupied_offsets_by_day: Dict[tuple[int, int, int], set[int]] = {}
    next_deadlines = _next_deadlines_from_designated(designated)

    # Existing first-miss rows reserve their already-assigned evening slots,
    # even if the pick later became skipped.  Preserve the offset relative to
    # the pick's original regular draft day, so overflow slots after 11 PM are
    # also handled consistently.
    idx_by_order_id = {int(rec["id"]): idx for idx, rec in enumerate(picks)}
    for order_id, state in miss_state.items():
        rescheduled = state.get("rescheduled_time")
        idx = idx_by_order_id.get(int(order_id))
        if not rescheduled or idx is None:
            continue
        rescheduled = _coerce_eastern(rescheduled)
        original_day = _evening_slot_day_for_designated_time(designated[idx])
        occupied_key = (original_day.year, original_day.month, original_day.day)
        # Usually this is 0..10 (6:30 PM..11:30 PM).  If an evening has more misses
        # than fit before midnight, evening_miss_slot spills later offsets to
        # the next non-Sunday evening, so identify the exact relative offset.
        for offset in range(0, max(30, len(picks) + 5)):
            if evening_miss_slot(original_day, offset) == rescheduled:
                occupied_offsets_by_day.setdefault(occupied_key, set()).add(offset)
                break

    for idx, rec in enumerate(picks):
        if rec["player_id"]:
            continue
        order_id = int(rec["id"])
        state = miss_state.get(order_id, {})
        if state.get("skipped_at") or state.get("rescheduled_time"):
            continue
        if now >= next_deadlines[idx]:
            d = _evening_slot_day_for_designated_time(designated[idx])
            first_misses_by_day.setdefault((d.year, d.month, d.day), []).append(idx)

    slots: Dict[int, datetime] = {}
    for ymd in sorted(first_misses_by_day):
        y, m, d = ymd
        day = next_non_sunday_date(date(y, m, d))
        occupied = set(occupied_offsets_by_day.get((day.year, day.month, day.day), set()))
        indices = sorted(first_misses_by_day[ymd], key=lambda i: (designated[i], i))
        next_offset = 0
        for idx in indices:
            while next_offset in occupied:
                next_offset += 1
            slots[idx] = evening_miss_slot(day, next_offset)
            occupied.add(next_offset)
            next_offset += 1
    return slots


def _compute_scheduled_times(now: datetime, include_expired_evening: bool = False) -> Dict[int, datetime]:
    """Compute the live scheduled time for each still-actionable undrafted Rule V pick.

    Rules:
      - Normal picks keep their designated/admin-overridden time until their normal deadline.
      - A first miss moves once to the end-of-day queue, starting at 6:30 PM ET in 30-minute slots.
      - If the evening slot's 30-minute window is missed, the pick is omitted from
        the returned map and is treated as skipped.

    `include_expired_evening=True` is used by queue enforcement so it can detect
    evening slots whose deadline has just passed and either draft from queue or
    persist the skip.
    """
    now = _coerce_eastern(now)
    picks, designated = _load_picks_overrides_and_designated()
    miss_state = _load_pick_miss_state()
    next_deadlines = _next_deadlines_from_designated(designated)
    unpersisted_evening_slots = _evening_slots_for_unpersisted_misses(now, picks, designated, miss_state)

    scheduled: Dict[int, datetime] = {}
    for idx, rec in enumerate(picks):
        if rec["player_id"]:
            continue
        order_id = int(rec["id"])
        state = miss_state.get(order_id, {})
        if state.get("skipped_at"):
            continue

        persisted_evening = state.get("rescheduled_time")
        if persisted_evening:
            evening_deadline = persisted_evening + timedelta(minutes=END_OF_DAY_MISS_INTERVAL_MINUTES)
            if include_expired_evening or now < evening_deadline:
                scheduled[idx] = persisted_evening
            continue

        dynamic_evening = unpersisted_evening_slots.get(idx)
        if dynamic_evening:
            evening_deadline = dynamic_evening + timedelta(minutes=END_OF_DAY_MISS_INTERVAL_MINUTES)
            if include_expired_evening or now < evening_deadline:
                scheduled[idx] = dynamic_evening
            continue

        if now < next_deadlines[idx]:
            scheduled[idx] = designated[idx]

    return scheduled


def get_current_on_clock_pick(now: Optional[datetime] = None) -> Optional[Dict[str, Any]]:
    """Return the actionable Rule V pick with the earliest current scheduled time."""
    now = _coerce_eastern(now or datetime.now(tz=EASTERN))
    picks, designated = _load_picks_overrides_and_designated()
    scheduled_time = _compute_scheduled_times(now)

    best_idx = None
    best_key = None
    for idx, rec in enumerate(picks):
        if rec["player_id"] or idx not in scheduled_time:
            continue
        t = scheduled_time[idx]
        key = (t, idx)
        if best_key is None or key < best_key:
            best_key = key
            best_idx = idx

    if best_idx is None:
        return None

    rec = picks[best_idx]
    scheduled = scheduled_time[best_idx].astimezone(EASTERN)
    deadline = _deadline_for_scheduled_index(best_idx, scheduled, designated).astimezone(EASTERN)
    return {
        "id": int(rec["id"]),
        "round": int(rec["round"]),
        "pick": int(rec["pick"]),
        "team": rec["team"],
        "scheduled_time_iso": scheduled.isoformat(timespec="minutes"),
        "deadline_time_iso": deadline.isoformat(timespec="minutes"),
    }


def get_current_pick_info(now: Optional[datetime] = None) -> Optional[Dict[str, Any]]:
    """Compatibility helper mirroring the amateur draft order page API."""
    info = get_current_on_clock_pick(now)
    if not info:
        return None
    info = dict(info)
    info["pick_label"] = f"{int(info['round'])}.{int(info['pick']):02d}"
    return info


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
    _ensure_pick_miss_state_table(conn)
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
    # Admin-rescheduling makes later picks actionable again, so clear old missed/skipped state.
    cur.executemany(
        "DELETE FROM rulev_pick_miss_state WHERE rulev_order_id=?",
        [(int(rec["id"]),) for rec in following],
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
    scheduled_map = _compute_scheduled_times(now)
    miss_state = _load_pick_miss_state()
    normal_deadlines = _next_deadlines_from_designated(designated)

    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT id, name FROM rulev_players")
    player_name_by_id = {int(r["id"]): r["name"] for r in cur.fetchall()}
    conn.close()

    rows: List[Dict[str, Any]] = []
    for idx, rec in enumerate(picks):
        order_id = int(rec["id"])
        pick_label = f"{int(rec['round'])}.{int(rec['pick']):02d}"

        if rec["player_id"]:
            pname = player_name_by_id.get(int(rec["player_id"]), f"Player #{rec['player_id']}")
            rows.append({
                "pick_label": pick_label,
                "team": rec["team"],
                "player": pname,
                "time_display": "",
                "status": f"Selected at {rec['drafted_at'] or '—'}",
            })
            continue

        scheduled = scheduled_map.get(idx)
        state = miss_state.get(order_id, {})
        skipped = bool(state.get("skipped_at")) or (scheduled is None and now >= normal_deadlines[idx])

        if skipped:
            time_display = "—"
            status = "Skipped"
        else:
            scheduled = (scheduled or designated[idx]).astimezone(EASTERN)
            time_display = fmt_est(scheduled)
            deadline = _deadline_for_scheduled_index(idx, scheduled, designated)
            if _is_evening_reschedule(idx, scheduled, designated):
                status = "On clock (rescheduled)" if now >= scheduled and now < deadline else "Missed → end of day"
            elif now >= scheduled and now < deadline:
                status = "On clock"
            else:
                status = "Scheduled"

        rows.append({
            "pick_label": pick_label,
            "team": rec["team"],
            "player": None,
            "time_display": time_display,
            "status": status,
        })

    if team_filter:
        rows = [r for r in rows if r["team"] == team_filter]
    return rows


def _run_rulev_queue_enforcement() -> None:
    """Run Rule V queue enforcement opportunistically without importing at module load time."""
    try:
        from rulev_app import enforce_queue_actions  # local import avoids circular imports
        enforce_queue_actions(max_steps=25)
    except Exception as exc:
        try:
            current_app.logger.exception("[rulev/order] enforce_queue_actions failed: %s", exc)
        except Exception:
            pass


@rulev_order_bp.route("/order")
def order_page():
    _run_rulev_queue_enforcement()

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
    _run_rulev_queue_enforcement()

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
