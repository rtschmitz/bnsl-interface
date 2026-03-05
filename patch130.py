# migrate_detroit_laviolette.py
from baseball import get_conn, perform_draft_internal
import sqlite3

TEAM = "Detroit Tigers"
MLBAMID = 702593
ROUND, PICK = 1, 30

conn = get_conn()
cur = conn.cursor()

# find player id
cur.execute("SELECT id, franchise FROM players WHERE mlbamid = ?", (MLBAMID,))
prow = cur.fetchone()
if not prow:
    raise SystemExit("Player with mlbamid=702593 not found")
pid, current_owner = int(prow["id"]), (prow["franchise"] or "")

# find draft_order row
cur.execute("SELECT id, team, player_id FROM draft_order WHERE round=? AND pick=?", (ROUND, PICK))
drow = cur.fetchone()
if not drow:
    raise SystemExit("Draft pick 1.30 not found")
doid, pick_team, pick_player_id = int(drow["id"]), drow["team"], drow["player_id"]

# apply only if safe/idempotent
if pick_player_id:
    print(f"Pick already filled (player_id={pick_player_id}); nothing to do.")
elif current_owner and current_owner != TEAM:
    raise SystemExit(f"Player already owned by {current_owner}; aborting.")
else:
    # assign + stamp + notify + remove from queues (uses your existing helper)
    perform_draft_internal(TEAM, pid, doid)
    print("Done: Detroit drafted Jace LaViolette at 1.30")

# ensure queues are clean even if previously owned by DET
cur.execute("DELETE FROM draft_queue WHERE player_id = ?", (pid,))
conn.commit()
conn.close()
print("Removed from all draft queues.")

