#!/usr/bin/env python3
from __future__ import annotations

import os
import csv
import io
import logging
import re
import sqlite3
from pathlib import Path
from datetime import datetime, timedelta
from typing import Any, Dict, List

from flask import Blueprint, current_app, has_app_context, request, jsonify, session, render_template_string, abort

# Reuse teams + emails from your main draft app
from team_config import MLB_TEAMS, TEAM_EMAILS, team_abbr_for_name, emails_equal
from ui_skin import BNSL_GAME_CSS

SORTABLE_TABLES_ASSETS = r"""
<style>
  table[data-sortable="true"] th.bnsl-sortable-header {
    cursor: pointer;
    user-select: none;
    white-space: nowrap;
  }
  table[data-sortable="true"] th.bnsl-sortable-header:hover {
    filter: brightness(1.18);
  }
  table[data-sortable="true"] th.bnsl-sortable-header:focus {
    outline: 2px solid rgba(140,170,255,.55);
    outline-offset: -2px;
  }
  .bnsl-sort-arrow {
    opacity: 0.78;
    margin-left: 6px;
    font-size: 11px;
  }
</style>
<script>
(function () {
  const DISABLED_HEADER_NAMES = new Set(["action", "actions", "reorder", "controls"]);

  function cellText(row, colIndex) {
    const cell = row.cells[colIndex];
    if (!cell) return "";
    return (cell.getAttribute("data-sort") || cell.textContent || "").trim();
  }

  function valueForSort(text) {
    const raw = String(text || "").trim();
    if (!raw || raw === "—" || raw === "-") return { empty: true, type: "text", value: "" };

    const maybeDate = raw.match(/^\d{4}-\d{2}-\d{2}/) ? Date.parse(raw) : NaN;
    if (Number.isFinite(maybeDate)) return { empty: false, type: "number", value: maybeDate };

    let compact = raw
      .replace(/[,$£€]/g, "")
      .replace(/\s+/g, "")
      .trim();

    let multiplier = 1;
    if (/m$/i.test(compact)) {
      multiplier = 1000000;
      compact = compact.slice(0, -1);
    } else if (/k$/i.test(compact)) {
      multiplier = 1000;
      compact = compact.slice(0, -1);
    } else if (/%$/.test(compact)) {
      compact = compact.slice(0, -1);
    }

    let neg = false;
    const paren = compact.match(/^\((.*)\)$/);
    if (paren) {
      neg = true;
      compact = paren[1];
    }

    if (/^[+-]?\d+(\.\d+)?$/.test(compact)) {
      let num = Number(compact) * multiplier;
      if (neg) num = -num;
      if (Number.isFinite(num)) return { empty: false, type: "number", value: num };
    }

    return { empty: false, type: "text", value: raw.toLocaleLowerCase() };
  }

  function compareSortValues(a, b, dir) {
    if (a.value.empty && !b.value.empty) return 1;
    if (!a.value.empty && b.value.empty) return -1;
    if (a.value.empty && b.value.empty) return a.index - b.index;

    let cmp = 0;
    if (a.value.type === "number" && b.value.type === "number") {
      cmp = a.value.value - b.value.value;
    } else {
      cmp = String(a.value.value).localeCompare(String(b.value.value), undefined, {
        numeric: true,
        sensitivity: "base"
      });
    }

    if (cmp === 0) return a.index - b.index;
    return dir === "desc" ? -cmp : cmp;
  }

  function clearIndicators(table) {
    table.querySelectorAll("th.bnsl-sortable-header").forEach(th => {
      th.setAttribute("aria-sort", "none");
      const arrow = th.querySelector(".bnsl-sort-arrow");
      if (arrow) arrow.remove();
    });
  }

  function markIndicator(table, th, dir) {
    clearIndicators(table);
    th.setAttribute("aria-sort", dir === "asc" ? "ascending" : "descending");
    const arrow = document.createElement("span");
    arrow.className = "bnsl-sort-arrow";
    arrow.textContent = dir === "asc" ? "▲" : "▼";
    th.appendChild(arrow);
  }

  function sortTable(table, colIndex, th, toggle) {
    const tbody = table.tBodies && table.tBodies[0];
    if (!tbody) return;

    let dir = "asc";
    if (toggle && table.dataset.sortCol === String(colIndex)) {
      dir = table.dataset.sortDir === "asc" ? "desc" : "asc";
    } else if (!toggle && table.dataset.sortDir) {
      dir = table.dataset.sortDir;
    }

    table.dataset.bnslSorting = "1";
    table.dataset.bnslIgnoreMutation = "1";
    const rows = Array.from(tbody.rows).map((row, index) => ({
      row,
      index,
      value: valueForSort(cellText(row, colIndex))
    }));
    rows.sort((a, b) => compareSortValues(a, b, dir));
    rows.forEach(item => tbody.appendChild(item.row));
    table.dataset.sortCol = String(colIndex);
    table.dataset.sortDir = dir;
    table.dataset.bnslSorting = "0";

    if (th) markIndicator(table, th, dir);
  }

  function initSortableTable(table) {
    if (!table || table.dataset.bnslSortInit === "1") return;
    const headerRow = table.tHead && table.tHead.rows.length ? table.tHead.rows[0] : null;
    if (!headerRow) return;

    Array.from(headerRow.cells).forEach((th, colIndex) => {
      const label = (th.textContent || "").trim().toLocaleLowerCase();
      if (th.dataset.noSort === "true" || th.dataset.sortDisabled === "true" || DISABLED_HEADER_NAMES.has(label)) return;

      th.classList.add("bnsl-sortable-header");
      th.setAttribute("role", "button");
      th.setAttribute("tabindex", "0");
      th.setAttribute("aria-sort", "none");
      th.addEventListener("click", () => sortTable(table, colIndex, th, true));
      th.addEventListener("keydown", (ev) => {
        if (ev.key === "Enter" || ev.key === " ") {
          ev.preventDefault();
          sortTable(table, colIndex, th, true);
        }
      });
    });

    const tbody = table.tBodies && table.tBodies[0];
    if (tbody && "MutationObserver" in window) {
      let queued = false;
      const observer = new MutationObserver(() => {
        if (table.dataset.bnslIgnoreMutation === "1") {
          table.dataset.bnslIgnoreMutation = "0";
          return;
        }
        if (table.dataset.bnslSorting === "1") return;
        if (!table.dataset.sortCol) return;
        if (queued) return;
        queued = true;
        window.requestAnimationFrame(() => {
          queued = false;
          const colIndex = Number(table.dataset.sortCol);
          const th = headerRow.cells[colIndex];
          if (th) sortTable(table, colIndex, th, false);
        });
      });
      observer.observe(tbody, { childList: true });
    }

    table.dataset.bnslSortInit = "1";
  }

  function initAllSortableTables() {
    document.querySelectorAll('table[data-sortable="true"]').forEach(initSortableTable);
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", initAllSortableTables);
  } else {
    initAllSortableTables();
  }
  window.BNSLSortableTables = { initAll: initAllSortableTables };
})();
</script>
"""

from bnsl_paths import db_path
from discord_notifier import send_discord_message

rulev_bp = Blueprint("rulev", __name__)

APP_DIR = Path(__file__).resolve().parent
DEFAULT_DB = db_path("rulev.db")


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
RULEV_ROSTER_STATUS = "Active"
RULEV_MINIMUM_SALARY = 673_000.0


def canonical_team_abbr(team: Any) -> str:
    return team_abbr_for_name(str(team or "").strip())


def get_db_path() -> Path:
    if has_app_context():
        cfg = current_app.config.get("RULEV_DB_PATH")
        if cfg:
            return Path(cfg)
    return DEFAULT_DB


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
    ensure_column(conn, "rulev_players", "ovr", "ovr INTEGER")
    ensure_column(conn, "rulev_players", "pot", "pot INTEGER")
    ensure_column(conn, "rulev_players", "def", "def INTEGER")

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

    # Rule V draft queue + per-team preference.  The queue lives in rulev.db,
    # separate from the amateur draft queue, but follows the same UX: managers
    # can add available players, reorder them, and optionally let the queue pick
    # at the start of their clock.  Default remains end-of-clock.
    cur.execute("""
      CREATE TABLE IF NOT EXISTS rulev_draft_queue (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        team TEXT NOT NULL,
        player_id INTEGER NOT NULL,
        position INTEGER NOT NULL,
        created_at TEXT NOT NULL,
        UNIQUE(team, player_id) ON CONFLICT IGNORE
      )
    """)
    cur.execute("""
      CREATE TABLE IF NOT EXISTS rulev_team_prefs (
        team TEXT PRIMARY KEY,
        use_queue_at_start INTEGER NOT NULL DEFAULT 0
      )
    """)

    conn.commit()
    conn.close()

def get_roster_db_path() -> Path:
    cfg = current_app.config.get("ROSTER_DB_PATH")
    return Path(cfg) if cfg else db_path("roster.db")


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value or 0.0)
    except Exception:
        return default


def _safe_optional_int(value: Any) -> int | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        return int(float(text))
    except Exception:
        return None


def _row_value(row: sqlite3.Row, key: str, default: Any = None) -> Any:
    try:
        return row[key] if key in row.keys() else default
    except Exception:
        return default


def apply_rulev_pick_to_roster(rulev_player_id: int, picking_team_abbr: str, drafted_at: str) -> dict[str, Any]:
    """Move the selected Rule V player in roster.db to the drafting team.

    The Rule V player pool is synced from roster.db and stores roster_player_id.
    A pick should therefore mutate that original roster row, not merely mark the
    player as drafted inside rulev.db.  Rule V selections are placed on the
    drafting team's Active roster so the financials page immediately counts the
    salary.  If the source row has no salary, fall back to the league minimum.
    """
    picking_team_abbr = canonical_team_abbr(picking_team_abbr)
    if not picking_team_abbr:
        return {"updated": False, "reason": "missing drafting team"}

    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT * FROM rulev_players WHERE id=?", (int(rulev_player_id),))
    rv_row = cur.fetchone()
    conn.close()
    if not rv_row:
        return {"updated": False, "reason": "rulev player not found"}

    roster_player_id = 0
    if "roster_player_id" in rv_row.keys():
        try:
            roster_player_id = int(rv_row["roster_player_id"] or 0)
        except Exception:
            roster_player_id = 0
    if roster_player_id <= 0:
        return {"updated": False, "reason": "rulev player is not linked to roster.db"}

    roster_path = get_roster_db_path()
    rconn = sqlite3.connect(str(roster_path))
    rconn.row_factory = sqlite3.Row
    rcur = rconn.cursor()
    rcur.execute("SELECT * FROM roster_players WHERE id=?", (roster_player_id,))
    roster_row = rcur.fetchone()
    if not roster_row:
        rconn.close()
        return {"updated": False, "reason": f"roster player {roster_player_id} not found"}

    old_team = str(roster_row["franchise"] or "").strip() if "franchise" in roster_row.keys() else ""
    old_status = str(roster_row["roster_status"] or "").strip() if "roster_status" in roster_row.keys() else ""
    existing_contract = str(roster_row["contract_type"] or "").strip().upper() if "contract_type" in roster_row.keys() else ""
    existing_salary = _safe_float(roster_row["salary"] if "salary" in roster_row.keys() else 0.0, 0.0)
    new_contract = existing_contract or "R"
    new_salary = existing_salary if existing_salary > 0 else RULEV_MINIMUM_SALARY

    rcur.execute("""
        UPDATE roster_players
        SET signed=1,
            franchise=?,
            affiliate_team='',
            roster_status=?,
            active_roster=1,
            contract_type=?,
            salary=?
        WHERE id=?
    """, (
        picking_team_abbr,
        RULEV_ROSTER_STATUS,
        new_contract,
        new_salary,
        roster_player_id,
    ))
    rconn.commit()
    rconn.close()

    return {
        "updated": True,
        "roster_player_id": roster_player_id,
        "old_team": old_team,
        "old_status": old_status,
        "new_team": picking_team_abbr,
        "new_status": RULEV_ROSTER_STATUS,
        "salary": new_salary,
        "drafted_at": drafted_at,
    }


def sync_after_rulev_roster_mutation() -> None:
    try:
        sync_rulev_from_roster_db()
    except Exception:
        current_app.logger.exception("Rule V sync failed after Rule V roster mutation")

    try:
        from fa_app import sync_free_agents_from_roster_db
        sync_free_agents_from_roster_db()
    except Exception:
        current_app.logger.exception("FA sync failed after Rule V roster mutation")


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
        ovr = _safe_optional_int(_row_value(r, "ovr", None))
        pot = _safe_optional_int(_row_value(r, "pot", None))
        def_rating = _safe_optional_int(_row_value(r, "def", None))

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
                    ovr=?,
                    pot=?,
                    def=?,
                    roster_source='roster_db',
                    rulev_eligible=1,
                    last_seen_roster_sync=?,
                    removed_from_roster_sync=NULL
                WHERE id=?
            """, (
                name, position, org, dob, contract_type, roster_status, ovr, pot, def_rating,
                now, int(existing["id"]),
            ))
        else:
            cur.execute("""
                INSERT INTO rulev_players(
                    name, position, org,
                    drafted_by, drafted_at,
                    roster_player_id, dob, contract_type, roster_status,
                    ovr, pot, def, roster_source, rulev_eligible,
                    last_seen_roster_sync, removed_from_roster_sync
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """, (
                name, position, org,
                None, None,
                roster_player_id, dob, contract_type, roster_status,
                ovr, pot, def_rating, "roster_db", 1,
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
    """Time-aware Rule V current pick.

    The order-page scheduler owns missed-pick behavior.  Normal missed windows
    roll once to an end-of-day slot; missed end-of-day slots are skipped.
    """
    from rulev_order_page import get_current_on_clock_pick

    cur_pick = get_current_on_clock_pick()
    if cur_pick is None:
        return None

    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM rulev_order")
    total = int(cur.fetchone()[0] or 0)
    cur.execute("SELECT COUNT(*) FROM rulev_order WHERE player_id IS NOT NULL")
    made = int(cur.fetchone()[0] or 0)
    skipped = 0
    try:
        cur.execute("SELECT COUNT(*) FROM rulev_pick_miss_state WHERE skipped_at IS NOT NULL")
        skipped = int(cur.fetchone()[0] or 0)
    except sqlite3.Error:
        skipped = 0
    conn.close()

    cur_pick.update({
        "picks_made": made,
        "skipped_picks": skipped,
        "resolved_picks": made + skipped,
        "total_picks": total,
    })
    return cur_pick


def notify_discord_rulev_pick(rulev_order_id: int) -> None:
    """Post a Rule V pick message to the shared draft-picks webhook."""
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
      SELECT o.round, o.pick, o.team, p.name, p.position
      FROM rulev_order o
      JOIN rulev_players p ON p.id = o.player_id
      WHERE o.id=?
    """, (rulev_order_id,))
    row = cur.fetchone()
    conn.close()
    if not row:
        return

    pick_label = f"Rule V {int(row['round'])}.{int(row['pick'])}"
    team = row["team"] or ""
    abbr = canonical_team_abbr(team) or team
    pos = (row["position"] or "").strip()
    player = (row["name"] or "").strip()
    player_text = f"{pos} {player}".strip()
    send_discord_message(
        "BNSL_DISCORD_DRAFT_PICKS_WEBHOOK_URL",
        f"{pick_label} [{abbr}]: {player_text}",
        fallback_label="draft-picks",
        legacy_env_vars=("DISCORD_WEBHOOK_URL",),
    )


def _require_authed_team() -> str:
    team = session.get("authed_team")
    if not team:
        abort(401, "Not logged in")
    return team


def remove_player_from_all_queues(player_id: int) -> None:
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("DELETE FROM rulev_draft_queue WHERE player_id=?", (int(player_id),))
    conn.commit()
    conn.close()


def get_team_queue(team: str) -> list[sqlite3.Row]:
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT
            dq.player_id,
            dq.position AS qpos,
            p.name,
            p.position,
            p.org,
            p.dob,
            COALESCE(p.rulev_eligible, 1) AS rulev_eligible,
            p.ovr,
            p.pot,
            p.def,
            p.drafted_by,
            p.drafted_at
        FROM rulev_draft_queue dq
        JOIN rulev_players p ON p.id = dq.player_id
        WHERE dq.team = ?
        ORDER BY dq.position ASC, dq.id ASC
    """, (team,))
    rows = cur.fetchall()
    conn.close()
    return rows


def get_team_queue_top_available(team: str) -> int | None:
    """Return the first still-available Rule V player in this team's queue."""
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT dq.player_id
          FROM rulev_draft_queue dq
         WHERE dq.team = ?
         ORDER BY dq.position ASC, dq.id ASC
    """, (team,))
    pids = [int(r[0]) for r in cur.fetchall()]
    if not pids:
        conn.close()
        return None

    qmarks = ",".join("?" for _ in pids)
    cur.execute(f"""
        SELECT id, drafted_by, COALESCE(rulev_eligible, 1) AS rulev_eligible
          FROM rulev_players
         WHERE id IN ({qmarks})
    """, pids)
    by_id = {int(r["id"]): r for r in cur.fetchall()}

    for pid in pids:
        row = by_id.get(pid)
        if not row:
            continue
        if not (row["drafted_by"] or "").strip() and int(row["rulev_eligible"] or 0) == 1:
            conn.close()
            return pid
    conn.close()
    return None


def set_queue_mode(team: str, use_at_start: bool) -> None:
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
      INSERT INTO rulev_team_prefs(team, use_queue_at_start) VALUES(?, ?)
      ON CONFLICT(team) DO UPDATE SET use_queue_at_start=excluded.use_queue_at_start
    """, (team, 1 if use_at_start else 0))
    conn.commit()
    conn.close()


def get_queue_mode(team: str) -> bool:
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT use_queue_at_start FROM rulev_team_prefs WHERE team=?", (team,))
    row = cur.fetchone()
    conn.close()
    return bool(row[0]) if row else False


def _complete_rulev_pick(team: str, player_id: int, rulev_order_id: int) -> None:
    """Shared Rule V pick implementation for manual picks and queue auto-picks."""
    conn = get_conn()
    c = conn.cursor()

    c.execute("SELECT id, name, org, drafted_by, COALESCE(rulev_eligible, 1) AS rulev_eligible FROM rulev_players WHERE id=?", (int(player_id),))
    r = c.fetchone()
    if not r:
        conn.close()
        raise RuntimeError("Player not found")
    if (r["drafted_by"] or "").strip():
        conn.close()
        raise RuntimeError("Player already picked")
    if int(r["rulev_eligible"] or 0) != 1:
        conn.close()
        raise RuntimeError("Player is not Rule V eligible")

    c.execute("SELECT id, team, player_id FROM rulev_order WHERE id=?", (int(rulev_order_id),))
    order_row = c.fetchone()
    if not order_row:
        conn.close()
        raise RuntimeError("Rule V order row not found")
    if order_row["player_id"]:
        conn.close()
        raise RuntimeError("Rule V pick already filled")
    if str(order_row["team"] or "") != str(team or ""):
        conn.close()
        raise RuntimeError("Team does not match this Rule V pick")

    now = datetime.utcnow().isoformat(timespec="seconds")
    losing_team = canonical_team_abbr(r["org"] or "")
    picking_team = canonical_team_abbr(team)
    player_name = str(r["name"] or "").strip()

    c.execute("UPDATE rulev_players SET drafted_by=?, drafted_at=? WHERE id=?", (team, now, int(player_id)))
    c.execute("UPDATE rulev_order SET player_id=?, drafted_at=? WHERE id=?", (int(player_id), now, int(rulev_order_id)))
    conn.commit()
    conn.close()

    roster_update = apply_rulev_pick_to_roster(int(player_id), picking_team, now)
    if roster_update.get("updated"):
        sync_after_rulev_roster_mutation()
    else:
        current_app.logger.warning(
            "Rule V pick did not update roster.db for rulev_player_id=%s: %s",
            player_id, roster_update.get("reason", "unknown reason")
        )

    try:
        notify_discord_rulev_pick(int(rulev_order_id))
    except Exception:
        current_app.logger.exception("Failed to post Rule V draft-pick Discord notification")

    if losing_team and picking_team:
        try:
            from financials_app import record_finance_payment
            record_finance_payment(
                source_type="rulev_pick_fee",
                source_id=int(rulev_order_id),
                payer_team_abbr=picking_team,
                receiver_team_abbr=losing_team,
                amount=RULEV_PICK_FEE,
                description=f"Rule V draft fee for {player_name}",
                effective_date=now[:10],
            )
        except Exception:
            current_app.logger.exception("Failed to post Rule V draft payment")

    remove_player_from_all_queues(int(player_id))
    try:
        from rulev_order_page import clear_rulev_pick_miss_state
        clear_rulev_pick_miss_state(int(rulev_order_id))
    except Exception:
        current_app.logger.exception("Failed to clear Rule V missed-pick state after completed pick")


def perform_rulev_pick_internal(team: str, player_id: int, rulev_order_id: int) -> None:
    """Bypass session; used for queue auto-picks."""
    _complete_rulev_pick(team, int(player_id), int(rulev_order_id))


def enforce_queue_actions(max_steps: int = 10) -> None:
    """
    Best-effort Rule V queue enforcement.

    Priority:
      1) If a normal pick window expires, draft from that team's queue if
         possible; otherwise persist a one-time end-of-day reschedule.
      2) If that end-of-day window expires, draft from queue if possible;
         otherwise persist the pick as skipped.
      3) If the current team has opted into start-of-clock queue usage, draft
         from queue once the scheduled time has arrived.
    """
    from rulev_order_page import (
        EASTERN,
        _compute_scheduled_times,
        _deadline_for_index,
        _deadline_for_scheduled_index,
        _is_evening_reschedule,
        _load_pick_miss_state,
        _load_picks_overrides_and_designated,
        get_current_pick_info,
        mark_rulev_pick_first_missed,
        mark_rulev_pick_skipped,
    )

    try:
        limit = max(1, int(max_steps))
    except Exception:
        limit = 10

    safety = 0
    while safety < limit:
        safety += 1
        progressed = False
        now = datetime.now(tz=EASTERN)

        try:
            picks, designated = _load_picks_overrides_and_designated()
            if not picks:
                break
            miss_state = _load_pick_miss_state()
            # Include expired evening slots so this enforcement pass can decide
            # whether to draft from queue or mark the pick skipped.  The public
            # scheduler omits expired evening slots afterward.
            scheduled = _compute_scheduled_times(now, include_expired_evening=True)
        except Exception as e:
            current_app.logger.exception("Rule V schedule compute failed during queue enforcement: %s", e)
            break

        # ---------- 1) NORMAL DEADLINE PASSED -> QUEUE OR ONE-TIME EOD ----------
        normal_overdue: list[int] = []
        for i, rec in enumerate(picks):
            if rec["player_id"]:
                continue
            order_id = int(rec["id"])
            state = miss_state.get(order_id, {})
            if state.get("skipped_at") or state.get("rescheduled_time"):
                continue
            if now >= _deadline_for_index(designated, i):
                normal_overdue.append(i)

        normal_overdue.sort(key=lambda i: (_deadline_for_index(designated, i), i))
        for i in normal_overdue:
            rec = picks[i]
            team = rec["team"]
            pid = get_team_queue_top_available(team)
            if pid:
                try:
                    perform_rulev_pick_internal(team, pid, int(rec["id"]))
                except Exception:
                    current_app.logger.exception(
                        "Rule V queue normal-deadline pick failed for team=%s pick_id=%s player_id=%s",
                        team, rec["id"], pid,
                    )
                else:
                    progressed = True
                    break

            # Queue was empty: move this pick to its one allowed end-of-day slot.
            rescheduled = scheduled.get(i)
            if rescheduled is not None and _is_evening_reschedule(i, rescheduled, designated):
                try:
                    mark_rulev_pick_first_missed(int(rec["id"]), rescheduled, now)
                except Exception:
                    current_app.logger.exception(
                        "Failed to persist Rule V first miss for team=%s pick_id=%s",
                        team, rec["id"],
                    )
                progressed = True
                break

        if progressed:
            continue

        # ---------- 2) EOD DEADLINE PASSED -> QUEUE OR SKIP ----------
        eod_overdue: list[int] = []
        for i, rec in enumerate(picks):
            if rec["player_id"] or i not in scheduled:
                continue
            order_id = int(rec["id"])
            state = miss_state.get(order_id, {})
            if state.get("skipped_at"):
                continue
            sched = scheduled[i]
            if not _is_evening_reschedule(i, sched, designated):
                continue
            if now >= _deadline_for_scheduled_index(i, sched, designated):
                eod_overdue.append(i)

        eod_overdue.sort(key=lambda i: (scheduled[i], i))
        for i in eod_overdue:
            rec = picks[i]
            team = rec["team"]
            pid = get_team_queue_top_available(team)
            if pid:
                try:
                    perform_rulev_pick_internal(team, pid, int(rec["id"]))
                except Exception:
                    current_app.logger.exception(
                        "Rule V queue EOD pick failed for team=%s pick_id=%s player_id=%s",
                        team, rec["id"], pid,
                    )
                else:
                    progressed = True
                    break

            try:
                mark_rulev_pick_skipped(int(rec["id"]), now)
            except Exception:
                current_app.logger.exception(
                    "Failed to persist Rule V skipped pick for team=%s pick_id=%s",
                    team, rec["id"],
                )
            progressed = True
            break

        if progressed:
            continue

        # ---------- 3) START-OF-CLOCK QUEUE MODE ----------
        try:
            info = get_current_pick_info(now)
        except Exception as e:
            current_app.logger.exception("Rule V get_current_pick_info failed during queue enforcement: %s", e)
            info = None

        if info:
            team = info["team"]
            scheduled_iso = info.get("scheduled_time_iso")
            try:
                sched_t = datetime.fromisoformat(scheduled_iso) if scheduled_iso else now
                if sched_t.tzinfo is None:
                    sched_t = sched_t.replace(tzinfo=EASTERN)
                else:
                    sched_t = sched_t.astimezone(EASTERN)
            except Exception:
                sched_t = now

            if now >= sched_t and get_queue_mode(team):
                pid = get_team_queue_top_available(team)
                if pid:
                    try:
                        perform_rulev_pick_internal(team, pid, int(info["id"]))
                    except Exception:
                        current_app.logger.exception(
                            "Rule V queue start-of-clock pick failed for team=%s pick_id=%s player_id=%s",
                            team, info["id"], pid,
                        )
                    else:
                        progressed = True

        if not progressed:
            break


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
        <a class="btn" href="/rulev/export.csv">Export CSV</a>
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
        <a id="queue-link" class="btn" href="/rulev/queue" style="margin-left:4px; display:none;">View Rule V Queue</a>

        <div class="pill" style="margin-left:auto;">
          <span>Search:</span>
          <input id="search" type="text" placeholder="Type a player name…" style="min-width:260px;" />
          <label class="muted" style="margin-left:10px;">
            <input id="hide-drafted" type="checkbox" /> Hide drafted players
          </label>
        </div>
      </div>

      <div class="pill" style="margin-top:10px;">
        <span id="login-status">🔒 Not logged in</span>
      </div>

      <hr class="sep"/>

      <div class="table-wrap">
        <table data-sortable="true">
          <thead>
            <tr>
              <th style="width:26%;">Name</th>
              <th style="width:8%;">Pos</th>
              <th style="width:10%;">Org</th>
              <th style="width:6%;">OVR</th>
              <th style="width:6%;">POT</th>
              <th style="width:6%;">DEF</th>
              <th style="width:22%;">Picked By</th>
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
const hideDrafted = document.getElementById('hide-drafted');
const queueLink = document.getElementById('queue-link');

let state = { search:'', hideDrafted:false, selectedTeam:'', authed:false, authedEmail:'', current:null, players:[] };

function fmtIso(s){ if(!s) return ''; try { return new Date(s).toLocaleString(); } catch { return s; } }
function rating(x){ return (x === null || x === undefined || x === '') ? '—' : String(x); }

function setTeams(teams){
  teamSel.innerHTML = '<option value="">— Select Team —</option>' + teams.map(t => `<option value="${t}">${t}</option>`).join('');
}

async function fetchJson(url, opts){
  const r = await fetch(url, opts);
  if (!r.ok){
    const text = await r.text();
    throw new Error(`${url} failed (${r.status}): ${text.slice(0, 300)}`);
  }
  return await r.json();
}

async function fetchStatus(){
  const d = await fetchJson('/rulev/api/status');
  setTeams(d.teams || []);
  if (d.selected_team) teamSel.value = d.selected_team;

  state.selectedTeam = d.selected_team || '';
  state.authed = !!d.authed_for_selected;
  state.authedEmail = d.authed_email || '';
  state.current = d.current || null;

  loginBtn.disabled = !teamSel.value;
  queueLink.style.display = (state.authed && state.selectedTeam) ? 'inline-block' : 'none';

  if (state.authed && state.selectedTeam){
    loginStatus.textContent = `🔓 Logged in as ${state.authedEmail} for ${state.selectedTeam}`;
  } else {
    loginStatus.textContent = '🔒 Not logged in';
  }

  if (!state.current){
    curSpan.textContent = 'Draft complete';
    prog.textContent = `${d.resolved_picks ?? d.picks_made}/${d.total_picks}`;
  } else {
    curSpan.textContent = `Round ${state.current.round}, Pick ${state.current.pick} — ${state.current.team}`;
    prog.textContent = `${d.resolved_picks ?? d.picks_made}/${d.total_picks}`;
  }
}

async function fetchPlayers(){
  const params = new URLSearchParams({ search: state.search, hide_drafted: state.hideDrafted ? '1' : '0' });
  const d = await fetchJson('/rulev/api/players?' + params.toString());
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
    } else if (!p.drafted_by && p.rulev_eligible && state.authed && state.selectedTeam) {
      if (p.in_queue) {
        action.innerHTML = '<span class="muted">Queued</span>';
      } else {
        const qbtn = document.createElement('button');
        qbtn.className = 'btn';
        qbtn.textContent = 'Add to queue';
        qbtn.onclick = async () => {
          qbtn.disabled = true;
          const resp = await fetch('/rulev/api/queue/add', {
            method:'POST',
            headers:{'Content-Type':'application/json'},
            body: JSON.stringify({ player_id: p.id })
          });
          if (!resp.ok){
            alert('Could not add to queue: ' + await resp.text());
          }
          await fetchPlayers();
        };
        action.appendChild(qbtn);
      }
    } else if (p.drafted_by) {
      action.innerHTML = '<span class="muted">Picked</span>';
    } else {
      action.innerHTML = '<span class="muted">—</span>';
    }

    tr.innerHTML = `
      <td><b>${p.name}</b></td>
      <td>${p.position || '—'}</td>
      <td>${p.org || '—'}</td>
      <td>${rating(p.ovr)}</td>
      <td>${rating(p.pot)}</td>
      <td>${rating(p.def)}</td>
      <td>${p.drafted_by || ''}</td>
      <td>${fmtIso(p.drafted_at) || ''}</td>
    `;
    tr.appendChild(action);
    tbody.appendChild(tr);
  }
}

function debounce(fn, ms){ let t; return (...a)=>{ clearTimeout(t); t=setTimeout(()=>fn(...a), ms); } }

search.addEventListener('input', debounce(()=>{ state.search = search.value || ''; fetchPlayers(); }, 120));
hideDrafted.addEventListener('change', ()=>{ state.hideDrafted = hideDrafted.checked; fetchPlayers(); });

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
  try {
    await fetchStatus();
    await fetchPlayers();
  } catch (err) {
    console.error(err);
    curSpan.textContent = 'Could not load Rule V status';
    tbody.innerHTML = `<tr><td colspan="9" class="muted">${String(err.message || err)}</td></tr>`;
  }
})();
</script>

    </div> <!-- /panel -->
  </div>   <!-- /page -->
</body>
</html>
"""
INDEX_HTML = INDEX_HTML.replace("__BNSL_GAME_CSS__", BNSL_GAME_CSS + SORTABLE_TABLES_ASSETS)


QUEUE_HTML = r"""
<!doctype html>
<html>
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Rule V Draft Queue</title>
__BNSL_GAME_CSS__
<style>
  .row { display:flex; gap:12px; align-items:center; flex-wrap:wrap; }
  .controls { display:flex; gap:8px; align-items:center; }
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
      <span class="badge">DRAFT ROOM</span>
      <span class="badge">QUEUE</span>
    </div>
  </div>

  <div class="panel pad">
    <div class="row">
      <a class="btn" href="/rulev/">← Back</a>
      <span class="pill">Rule V Queue</span>
      <span id="team-pill" class="pill"></span>
    </div>

    <div class="row">
      <label class="pill" style="background: rgba(0,0,0,.16);">
        <input type="radio" name="mode" id="mode-start"> Use queue at start of clock
      </label>
      <label class="pill" style="background: rgba(0,0,0,.16);">
        <input type="radio" name="mode" id="mode-end"> Use queue at end of clock (default)
      </label>
      <button id="save-mode" class="btn primary">Save Mode</button>
      <span class="muted">Queue order is top → bottom.</span>
    </div>

    <hr class="sep"/>

    <div class="table-wrap">
      <table data-sortable="true">
        <thead>
          <tr>
            <th style="width:6%;">#</th>
            <th style="width:26%;">Name</th>
            <th style="width:8%;">Pos</th>
            <th style="width:10%;">Org</th>
            <th style="width:8%;">DOB</th>
            <th style="width:6%;">OVR</th>
            <th style="width:6%;">POT</th>
            <th style="width:6%;">DEF</th>
            <th style="width:14%;">Picked By</th>
            <th style="width:10%;" data-no-sort="true">Actions</th>
          </tr>
        </thead>
        <tbody id="queue-body"></tbody>
      </table>
    </div>

    <script>
      const tbody = document.getElementById('queue-body');
      const saveModeBtn = document.getElementById('save-mode');
      const modeStart = document.getElementById('mode-start');
      const modeEnd = document.getElementById('mode-end');
      const teamPill = document.getElementById('team-pill');

      let queue = [];
      let team = "";
      let useStart = false;

      function show(x){ return (x === null || x === undefined || x === '') ? '—' : x; }
      function fmtIso(s){ if(!s) return ''; try { return new Date(s).toLocaleString(); } catch { return s; } }

      async function load() {
        const res = await fetch('/rulev/api/queue');
        if (!res.ok) {
          if (res.status === 401) {
            alert('Please login from the Rule V draft page first.');
            location.href = '/rulev/';
            return;
          }
          alert('Failed to load queue: ' + await res.text());
          return;
        }
        const data = await res.json();
        team = data.team || '';
        useStart = !!data.use_at_start;
        queue = data.items || [];
        render();
      }

      function render() {
        teamPill.textContent = team ? ('Team: ' + team) : '';
        modeStart.checked = useStart;
        modeEnd.checked = !useStart;
        tbody.innerHTML = '';

        queue.forEach((p, idx) => {
          const tr = document.createElement('tr');
          if (p.drafted_by) tr.classList.add('taken');

          const tdIdx = document.createElement('td');
          tdIdx.textContent = String(idx + 1);

          const tdName = document.createElement('td');
          tdName.innerHTML = `<b>${p.name || ''}</b>`;

          const tdPos = document.createElement('td');
          tdPos.textContent = p.position || '—';

          const tdOrg = document.createElement('td');
          tdOrg.textContent = p.org || '—';

          const tdDob = document.createElement('td');
          tdDob.textContent = p.dob || '—';

          const tdOvr = document.createElement('td');
          tdOvr.textContent = show(p.ovr);

          const tdPot = document.createElement('td');
          tdPot.textContent = show(p.pot);

          const tdDef = document.createElement('td');
          tdDef.textContent = show(p.def);

          const tdPicked = document.createElement('td');
          tdPicked.textContent = p.drafted_by ? `${p.drafted_by} ${fmtIso(p.drafted_at)}` : '';

          const tdAct = document.createElement('td');
          const ctrls = document.createElement('div');
          ctrls.className = 'controls';

          const up = document.createElement('button');
          up.className = 'btn';
          up.textContent = '↑';
          up.disabled = idx === 0;
          up.onclick = () => move(idx, -1);

          const down = document.createElement('button');
          down.className = 'btn';
          down.textContent = '↓';
          down.disabled = idx === queue.length - 1;
          down.onclick = () => move(idx, +1);

          const del = document.createElement('button');
          del.className = 'btn danger';
          del.textContent = 'Remove';
          del.onclick = () => remove(p.player_id);

          ctrls.appendChild(up);
          ctrls.appendChild(down);
          ctrls.appendChild(del);
          tdAct.appendChild(ctrls);

          tr.appendChild(tdIdx);
          tr.appendChild(tdName);
          tr.appendChild(tdPos);
          tr.appendChild(tdOrg);
          tr.appendChild(tdDob);
          tr.appendChild(tdOvr);
          tr.appendChild(tdPot);
          tr.appendChild(tdDef);
          tr.appendChild(tdPicked);
          tr.appendChild(tdAct);
          tbody.appendChild(tr);
        });
      }

      function move(i, delta) {
        const j = i + delta;
        if (j < 0 || j >= queue.length) return;
        [queue[i], queue[j]] = [queue[j], queue[i]];
        render();
        saveOrder();
      }

      async function saveOrder() {
        const order = queue.map(x => x.player_id);
        await fetch('/rulev/api/queue/reorder', {
          method: 'POST',
          headers: {'Content-Type':'application/json'},
          body: JSON.stringify({ order })
        });
      }

      async function remove(pid) {
        const res = await fetch('/rulev/api/queue/remove', {
          method: 'POST',
          headers: {'Content-Type':'application/json'},
          body: JSON.stringify({ player_id: pid })
        });
        if (res.ok) {
          queue = queue.filter(x => x.player_id !== pid);
          render();
        } else {
          alert('Could not remove player: ' + await res.text());
        }
      }

      saveModeBtn.onclick = async () => {
        useStart = modeStart.checked;
        const res = await fetch('/rulev/api/queue/mode', {
          method: 'POST',
          headers: {'Content-Type':'application/json'},
          body: JSON.stringify({ use_at_start: useStart })
        });
        if (!res.ok) {
          alert('Could not save queue mode: ' + await res.text());
          return;
        }
        alert('Queue mode saved.');
      };

      load();
    </script>
  </div>
</div>
</body>
</html>
"""
QUEUE_HTML = QUEUE_HTML.replace("__BNSL_GAME_CSS__", BNSL_GAME_CSS + SORTABLE_TABLES_ASSETS)



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



def _csv_response_for_table(conn: sqlite3.Connection, table: str, filename: str):
    cur = conn.cursor()
    cur.execute(f"PRAGMA table_info({table})")
    columns = [row[1] for row in cur.fetchall()]
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(columns)
    order_clause = " ORDER BY id" if "id" in columns else ""
    cur.execute(f"SELECT * FROM {table}{order_clause}")
    for row in cur.fetchall():
        writer.writerow([row[col] for col in columns])
    conn.close()
    return current_app.response_class(
        output.getvalue(),
        mimetype="text/csv",
        headers={"Content-Disposition": f"attachment; filename={filename}"},
    )


@rulev_bp.get("/export.csv")
def export_rulev_csv():
    return _csv_response_for_table(get_conn(), "rulev_players", "rulev_players.csv")

@rulev_bp.get("/")
def index():
    return render_template_string(INDEX_HTML)


@rulev_bp.get("/queue")
def queue_page():
    return render_template_string(QUEUE_HTML)


@rulev_bp.get("/api/status")
def api_status():
    # Keep the page responsive.  The full queue task can still process a backlog,
    # but the first status poll should never sit on many historical misses.
    try:
        enforce_queue_actions(max_steps=3)
    except Exception:
        current_app.logger.exception("Rule V queue enforcement failed during status poll")
    try:
        cur = current_pick()
    except Exception:
        current_app.logger.exception("Rule V current-pick lookup failed during status poll")
        cur = None
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
    skipped = 0
    try:
        c.execute("SELECT COUNT(*) FROM rulev_pick_miss_state WHERE skipped_at IS NOT NULL")
        skipped = int(c.fetchone()[0] or 0)
    except Exception:
        skipped = 0
    conn.close()

    return jsonify({
        "teams": MLB_TEAMS,
        "selected_team": selected_team,
        "authed_team": authed_team,
        "authed_email": authed_email,
        "authed_for_selected": bool(selected_team) and (authed_team == selected_team),
        "current": cur,
        "picks_made": made,
        "skipped_picks": skipped,
        "resolved_picks": made + skipped,
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
    hide_drafted = request.args.get("hide_drafted") == "1"

    conn = get_conn()
    cur = conn.cursor()

    authed_team = session.get("authed_team", "") or ""
    in_queue = set()
    if authed_team:
        try:
            cur.execute("SELECT player_id FROM rulev_draft_queue WHERE team=?", (authed_team,))
            in_queue = {int(r[0]) for r in cur.fetchall()}
        except sqlite3.OperationalError:
            # Older deployments may have the Rule V DB before the queue table existed.
            # before_app_request normally creates it, but keep this endpoint fail-open.
            current_app.logger.exception("Rule V queue table lookup failed while loading players")
            in_queue = set()

    clauses = ["(COALESCE(rulev_eligible, 1)=1 OR COALESCE(drafted_by, '') != '')"]
    params: List[Any] = []
    if q:
        clauses.append("LOWER(name) LIKE ?")
        params.append(f"%{q}%")
    if hide_drafted:
        clauses.append("COALESCE(drafted_by, '') = ''")

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
            "ovr": _row_value(r, "ovr", None),
            "pot": _row_value(r, "pot", None),
            "def": _row_value(r, "def", None),
            "drafted_by": r["drafted_by"],
            "drafted_at": r["drafted_at"],
            "in_queue": (int(r["id"]) in in_queue),
        })
    return jsonify({"players": players})


@rulev_bp.get("/api/queue")
def api_queue_get():
    team = _require_authed_team()
    rows = get_team_queue(team)
    items = []
    for r in rows:
        items.append({
            "player_id": int(r["player_id"]),
            "qpos": int(r["qpos"] or 0),
            "name": r["name"],
            "position": r["position"],
            "org": r["org"],
            "dob": r["dob"],
            "rulev_eligible": int(r["rulev_eligible"] or 1),
            "ovr": r["ovr"],
            "pot": r["pot"],
            "def": r["def"],
            "drafted_by": r["drafted_by"],
            "drafted_at": r["drafted_at"],
        })
    return jsonify({
        "team": team,
        "use_at_start": get_queue_mode(team),
        "items": items,
    })


@rulev_bp.post("/api/queue/add")
def api_queue_add():
    team = _require_authed_team()
    data = request.get_json(force=True, silent=True) or {}
    pid = int(data.get("player_id") or 0)
    if pid <= 0:
        return ("missing player_id", 400)

    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT drafted_by, COALESCE(rulev_eligible, 1) AS rulev_eligible FROM rulev_players WHERE id=?", (pid,))
    pr = cur.fetchone()
    if not pr:
        conn.close()
        return ("player not found", 404)
    if (pr["drafted_by"] or "").strip() or int(pr["rulev_eligible"] or 0) != 1:
        conn.close()
        return ("player not addable", 409)

    cur.execute("SELECT COALESCE(MAX(position), 0) FROM rulev_draft_queue WHERE team=?", (team,))
    next_pos = int(cur.fetchone()[0] or 0) + 1
    cur.execute("""
        INSERT OR IGNORE INTO rulev_draft_queue(team, player_id, position, created_at)
        VALUES(?,?,?,?)
    """, (team, pid, next_pos, datetime.utcnow().isoformat(timespec="seconds")))
    conn.commit()
    conn.close()
    return ("", 204)


@rulev_bp.post("/api/queue/remove")
def api_queue_remove():
    team = _require_authed_team()
    data = request.get_json(force=True, silent=True) or {}
    pid = int(data.get("player_id") or 0)
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("DELETE FROM rulev_draft_queue WHERE team=? AND player_id=?", (team, pid))
    conn.commit()
    conn.close()
    return ("", 204)


@rulev_bp.post("/api/queue/reorder")
def api_queue_reorder():
    team = _require_authed_team()
    data = request.get_json(force=True, silent=True) or {}
    order = data.get("order") or []
    if not isinstance(order, list) or not all(isinstance(x, int) for x in order):
        return ("invalid order", 400)

    conn = get_conn()
    cur = conn.cursor()
    for idx, pid in enumerate(order, start=1):
        cur.execute("UPDATE rulev_draft_queue SET position=? WHERE team=? AND player_id=?", (idx, team, pid))
    conn.commit()
    conn.close()
    return ("", 204)


@rulev_bp.post("/api/queue/mode")
def api_queue_mode():
    team = _require_authed_team()
    data = request.get_json(force=True, silent=True) or {}
    set_queue_mode(team, bool(data.get("use_at_start")))
    return ("", 204)


@rulev_bp.post("/tasks/enforce_queue")
def task_enforce_queue():
    try:
        enforce_queue_actions(max_steps=200)
        return ("", 204)
    except Exception as e:
        current_app.logger.exception("Rule V queue enforcement task failed")
        return (f"enforce failed: {e}", 500)


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

    try:
        _complete_rulev_pick(team, player_id, int(cur["id"]))
    except RuntimeError as e:
        msg = str(e)
        status = 409 if "already" in msg.lower() or "eligible" in msg.lower() else 404 if "not found" in msg.lower() else 500
        return (msg, status)
    except Exception as e:
        current_app.logger.exception("Failed to make Rule V pick")
        return (f"Failed to pick: {e}", 500)

    try:
        enforce_queue_actions()
    except Exception:
        current_app.logger.exception("Rule V queue enforcement failed after manual pick")

    return ("", 204)


if __name__ == "__main__":
    import argparse
    from flask import Flask

    parser = argparse.ArgumentParser(description="Rule V roster-sync utilities")
    parser.add_argument("--rulev-db", default=str(DEFAULT_DB), help="Path to rulev.db")
    parser.add_argument("--roster-db", default=str(db_path("roster.db")), help="Path to roster.db")
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
