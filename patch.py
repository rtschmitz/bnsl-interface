#!/usr/bin/env python3
import os
import sqlite3
from datetime import datetime
from zoneinfo import ZoneInfo

# --- Locate DB ---
DB_PATH = (
    os.environ.get("DB_PATH")
    or ("/data/draft.db" if os.path.exists("/data") else os.path.join(os.path.dirname(__file__), "draft.db"))
)

EASTERN = ZoneInfo("America/New_York")

TEAM_SLOTS = [
    # (team, hour_ET, minute_ET)
    ("Detroit Tigers", 19, 0),  # 7:00 PM ET
    ("New York Yankees", 20, 0),  # 8:00 PM ET
]

# Team -> MLBAMID to assign
TEAM_PLAYER_MLBAMID = {
    "Milwaukee Brewers": 828244,   # Kruz Schoolcraft
    "Texas Rangers":     702652,   # Andrew Fischer
    "Washington Nationals": 700280,# Miguel Mendez
    "Pittsburgh Pirates": 815818,  # Gavin Fien
    "Minnesota Twins":   815832,   # Xavier Neyens
}

def col_exists(cur, table, col):
    cur.execute(f"PRAGMA table_info({table})")
    return any(row[1] == col for row in cur.fetchall())

def table_exists(cur, table):
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table,))
    return cur.fetchone() is not None

def get_player_id_by_mlbamid(cur, mlbamid):
    cur.execute("SELECT id FROM players WHERE mlbamid = ?", (mlbamid,))
    row = cur.fetchone()
    return int(row[0]) if row else None

def get_next_undrafted_pick_id_for_team(cur, team):
    cur.execute("""
        SELECT id FROM draft_order
         WHERE team = ? AND (player_id IS NULL)
         ORDER BY round ASC, pick ASC
         LIMIT 1
    """, (team,))
    row = cur.fetchone()
    return int(row[0]) if row else None

def upsert_pick_override(cur, draft_order_id, dt_et, missed=True):
    # Ensure table exists
    cur.execute("""
        CREATE TABLE IF NOT EXISTS pick_overrides (
            draft_order_id INTEGER PRIMARY KEY,
            scheduled_time TEXT NOT NULL
        )
    """)
    # Add "missed" column safely (ignore if it already exists)
    if not col_exists(cur, "pick_overrides", "missed"):
        cur.execute("ALTER TABLE pick_overrides ADD COLUMN missed INTEGER DEFAULT 0")
    cur.execute("""
        INSERT INTO pick_overrides(draft_order_id, scheduled_time, missed)
        VALUES(?,?,?)
        ON CONFLICT(draft_order_id) DO UPDATE SET
            scheduled_time = excluded.scheduled_time,
            missed = excluded.missed
    """, (draft_order_id, dt_et.isoformat(timespec="minutes"), 1 if missed else 0))

def assign_player_to_team_next_pick(cur, team, player_id):
    # Make sure player exists and is eligible/unowned
    cur.execute("SELECT franchise, COALESCE(eligible,1) FROM players WHERE id = ?", (player_id,))
    prow = cur.fetchone()
    if not prow:
        raise RuntimeError(f"Player id {player_id} not found in players")
    current_owner = prow[0]
    # We will overwrite franchise even if already set; if you want to guard, uncomment next two lines:
    # if current_owner:
    #     raise RuntimeError(f"Player id {player_id} already owned by {current_owner}")

    pick_id = get_next_undrafted_pick_id_for_team(cur, team)
    if not pick_id:
        raise RuntimeError(f"No undrafted pick found for team {team}")

    # Assign player to team + mark pick as drafted now (UTC)
    cur.execute("UPDATE players SET franchise=? WHERE id=?", (team, player_id))
    cur.execute("""
        UPDATE draft_order
           SET player_id = ?, drafted_at = ?
         WHERE id = ?
    """, (player_id, datetime.utcnow().isoformat(timespec="seconds"), pick_id))

    # Remove from all queues
    cur.execute("DELETE FROM draft_queue WHERE player_id = ?", (player_id,))
    return pick_id

def main():
    print(f"[patch] opening DB at {DB_PATH}")
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    try:
        # Basic schema sanity (these tables exist in your app, but create if missing)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS players(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                dob TEXT,
                position TEXT,
                franchise TEXT,
                eligible INTEGER NOT NULL DEFAULT 1,
                mlbamid INTEGER
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS draft_order(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                round INTEGER NOT NULL,
                pick INTEGER NOT NULL,
                team TEXT NOT NULL,
                player_id INTEGER,
                drafted_at TEXT,
                label TEXT,
                UNIQUE(round, pick) ON CONFLICT IGNORE
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS draft_queue(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                team TEXT NOT NULL,
                player_id INTEGER NOT NULL,
                position INTEGER NOT NULL,
                created_at TEXT NOT NULL,
                UNIQUE(team, player_id) ON CONFLICT IGNORE
            )
        """)

        # 1) Schedule overrides + mark as "missed"
        today_et = datetime.now(tz=EASTERN).date()
        for team, hh, mm in TEAM_SLOTS:
            # Find the team’s next undrafted pick (this is what we’re scheduling)
            pick_id = get_next_undrafted_pick_id_for_team(cur, team)
            if not pick_id:
                print(f"[warn] No undrafted pick found for {team}; skipping override")
                continue
            dt_et = datetime(today_et.year, today_et.month, today_et.day, hh, mm, 0, tzinfo=EASTERN)
            upsert_pick_override(cur, pick_id, dt_et, missed=True)
            print(f"[ok] Override {team}: pick_id={pick_id} -> {dt_et.isoformat(timespec='minutes')} (missed=1)")

        # 2) Assign players to teams’ next undrafted picks
        for team, mlbamid in TEAM_PLAYER_MLBAMID.items():
            pid = get_player_id_by_mlbamid(cur, mlbamid)
            if not pid:
                raise RuntimeError(f"Player with MLBAMID {mlbamid} not found for {team}")
            pick_id = assign_player_to_team_next_pick(cur, team, pid)
            print(f"[ok] Assigned MLBAMID {mlbamid} to {team} (draft_order.id={pick_id}) and removed from all queues")

        conn.commit()
        print("[patch] DONE.")
    except Exception as e:
        conn.rollback()
        raise
    finally:
        conn.close()

if __name__ == "__main__":
    main()

