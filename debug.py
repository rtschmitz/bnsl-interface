#!/usr/bin/env python3
"""
debug_set_next_times.py
Sets the next 3 undrafted picks to fire at now+1m, now+2m, now+3m (1 minute gaps),
by inserting/updating rows in pick_overrides (honored by the /order page).
"""

from __future__ import annotations
import sqlite3
from pathlib import Path
from datetime import datetime, timedelta, timezone

APP_DIR = Path(__file__).resolve().parent
DB_PATH = APP_DIR / "draft.db"

def main():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    # Ensure table exists
    cur.execute("""
      CREATE TABLE IF NOT EXISTS pick_overrides (
        draft_order_id INTEGER PRIMARY KEY,
        scheduled_time TEXT NOT NULL
      )
    """)
    conn.commit()

    # Find next 3 undrafted picks in draft order
    cur.execute("""
      SELECT id, round, pick, team
      FROM draft_order
      WHERE player_id IS NULL
      ORDER BY round ASC, pick ASC
      LIMIT 3
    """)
    rows = cur.fetchall()
    if not rows:
        print("No undrafted picks found.")
        return

    now_utc = datetime.now(timezone.utc)
    times = [now_utc +timedelta(minutes=i+1) for i in range(len(rows))]  # +1m, +2m, +3m
    #times = [now_utc + timedelta(hours=1)+timedelta(minutes=i+1) for i in range(len(rows))]  # +1m, +2m, +3m

    # Upsert overrides
    for r, t in zip(rows, times):
        iso = t.isoformat(timespec="seconds")  # store timezone-aware ISO
        cur.execute("""
          INSERT INTO pick_overrides(draft_order_id, scheduled_time)
          VALUES (?, ?)
          ON CONFLICT(draft_order_id) DO UPDATE SET scheduled_time=excluded.scheduled_time
        """, (r["id"], iso))
        print(f"Override set: pick {r['round']}.{r['pick']} ({r['team']}) -> {iso} (UTC)")

    conn.commit()
    conn.close()
    print("Done. Visit /order to verify (will show ET).")

if __name__ == "__main__":
    main()

