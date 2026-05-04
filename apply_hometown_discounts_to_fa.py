#!/usr/bin/env python3
"""
Apply the compressed hometown-discount reference DB to the current FA table.

This script does not read the large batting/pitching stats files. It only reads:
  - hometown_discounts.db, produced by build_hometown_discounts.py
  - fa.db/free_agents
  - roster.db/roster_players, when free_agents.roster_player_id is available

It updates free_agents.hometown_team and free_agents.hometown_seasons, which are
already used by the FA bid preview/place-bid code to apply the 1.05x/1.10x
hometown multiplier.
"""
from __future__ import annotations

import argparse
import json
import re
import sqlite3
import unicodedata
from pathlib import Path
from typing import Any

APP_DIR = Path(__file__).resolve().parent

ABBR_TO_TEAM = {
    "ARI": "Arizona Diamondbacks", "ATL": "Atlanta Braves", "BAL": "Baltimore Orioles", "BOS": "Boston Red Sox",
    "CHC": "Chicago Cubs", "CHW": "Chicago White Sox", "CIN": "Cincinnati Reds", "CLE": "Cleveland Guardians",
    "COL": "Colorado Rockies", "DET": "Detroit Tigers", "HOU": "Houston Astros", "KC": "Kansas City Royals",
    "KCR": "Kansas City Royals", "LAA": "Los Angeles Angels", "LAD": "Los Angeles Dodgers", "MIA": "Miami Marlins",
    "MIL": "Milwaukee Brewers", "MIN": "Minnesota Twins", "NYM": "New York Mets", "NYY": "New York Yankees",
    "OAK": "Oakland Athletics", "PHI": "Philadelphia Phillies", "PIT": "Pittsburgh Pirates", "SD": "San Diego Padres",
    "SDP": "San Diego Padres", "SF": "San Francisco Giants", "SFG": "San Francisco Giants", "SEA": "Seattle Mariners",
    "STL": "St. Louis Cardinals", "TB": "Tampa Bay Rays", "TBR": "Tampa Bay Rays", "TEX": "Texas Rangers",
    "TOR": "Toronto Blue Jays", "WSH": "Washington Nationals", "WAS": "Washington Nationals", "WSN": "Washington Nationals",
}


def norm_token(value: Any) -> str:
    text = "".join(
        ch for ch in unicodedata.normalize("NFKD", str(value or ""))
        if not unicodedata.combining(ch)
    ).lower()
    return re.sub(r"[^a-z0-9]", "", text)


def table_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    cur = conn.cursor()
    cur.execute(f"PRAGMA table_info({table})")
    return {str(r[1]) for r in cur.fetchall()}


def ensure_column(conn: sqlite3.Connection, table: str, col: str, coldef: str) -> None:
    if col not in table_columns(conn, table):
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {coldef}")
        conn.commit()


def split_name(name: str) -> tuple[str, str]:
    bits = [b for b in re.split(r"\s+", (name or "").strip()) if b]
    if not bits:
        return "", ""
    if len(bits) == 1:
        return "", bits[0]
    return bits[0], bits[-1]


def roster_code_to_team(value: Any) -> str:
    code = str(value or "").strip().upper()
    m = re.search(r"\(([A-Z]{2,3})\)", code)
    if m:
        code = m.group(1)
    code = re.sub(r"[^A-Z0-9]", "", code)
    return ABBR_TO_TEAM.get(code, str(value or "").strip())


def get_row_value(row: sqlite3.Row | None, key: str, default: Any = "") -> Any:
    if row is None:
        return default
    return row[key] if key in row.keys() else default


def candidate_keys_for_player(fa_row: sqlite3.Row, roster_row: sqlite3.Row | None) -> list[str]:
    name = str(get_row_value(roster_row, "name") or fa_row["name"] or "").strip()
    first = str(get_row_value(roster_row, "first_name") or "").strip()
    last = str(get_row_value(roster_row, "last_name") or "").strip()
    if not first or not last:
        f2, l2 = split_name(name)
        first = first or f2
        last = last or l2

    first_n = norm_token(first)
    last_n = norm_token(last)
    if not last_n:
        return []

    keys: list[str] = []

    def add(key: str) -> None:
        if key and key not in keys:
            keys.append(key)

    # Exact/cross aliases for the bbref-ish IDs. The OOTP files use these labels
    # inconsistently, so keep the LASTNAME guard but try both key prefixes.
    for raw in (
        get_row_value(roster_row, "bbref_id"),
        get_row_value(roster_row, "bbrefminors_id"),
        fa_row["mlbam_id"] if "mlbam_id" in fa_row.keys() else "",
    ):
        val = norm_token(raw)
        if val:
            add(f"bbref:{val}:{last_n}")
            add(f"bbrefminors:{val}:{last_n}")

    # OOTP string pid often lives in roster bbrefminors_id for OOTP exports; the
    # numeric ootp_id is tried too, but usually the string pid is the useful one.
    for raw in (
        get_row_value(roster_row, "ootp_id"),
        get_row_value(roster_row, "bbrefminors_id"),
    ):
        val = norm_token(raw)
        if val:
            add(f"ootp:{val}:{last_n}")

    team_candidates = []
    if "last_team" in fa_row.keys():
        team_candidates.append(fa_row["last_team"])
    team_candidates.append(get_row_value(roster_row, "franchise"))
    for tm in team_candidates:
        tm_text = roster_code_to_team(tm)
        tm_norm = norm_token(tm_text)
        if first_n and tm_norm:
            add(f"first_last_team:{first_n}:{last_n}:{tm_norm}")
        raw_norm = norm_token(tm)
        if first_n and raw_norm:
            add(f"first_last_team:{first_n}:{last_n}:{raw_norm}")

    if first_n:
        add(f"first_last:{first_n}:{last_n}")

    return keys


def load_hometown_key_map(htd_db: Path) -> dict[str, dict[str, Any]]:
    conn = sqlite3.connect(str(htd_db))
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    cur.execute("""
        SELECT k.key, k.rank, k.method,
               d.id AS discount_id, d.full_name, d.team_abbr, d.team_name,
               d.hometown_seasons, d.multiplier, d.stats_player_id
        FROM hometown_discount_keys k
        JOIN hometown_discounts d ON d.id = k.discount_id
    """)
    out = {str(r["key"]): dict(r) for r in cur.fetchall()}
    conn.close()
    return out


def apply_hometown_discounts(
    fa_db: Path,
    roster_db: Path,
    htd_db: Path,
    clear_missing: bool = True,
    dry_run: bool = False,
) -> dict[str, int]:
    if not fa_db.exists():
        raise FileNotFoundError(f"FA DB not found: {fa_db}")
    if not htd_db.exists():
        raise FileNotFoundError(f"hometown discount DB not found: {htd_db}")

    htd_by_key = load_hometown_key_map(htd_db)

    fa_conn = sqlite3.connect(str(fa_db))
    fa_conn.row_factory = sqlite3.Row
    ensure_column(fa_conn, "free_agents", "franchise_abbr", "franchise_abbr TEXT")
    ensure_column(fa_conn, "free_agents", "htd", "htd INTEGER")

    roster_conn = None
    if roster_db.exists():
        roster_conn = sqlite3.connect(str(roster_db))
        roster_conn.row_factory = sqlite3.Row

    fa_cols = table_columns(fa_conn, "free_agents")
    clauses = ["(signed_team IS NULL OR signed_team='')"]
    if "is_roster_unrostered" in fa_cols:
        clauses.append("COALESCE(is_roster_unrostered, 1) = 1")
    sql = "SELECT * FROM free_agents WHERE " + " AND ".join(clauses)

    cur = fa_conn.cursor()
    rows = cur.execute(sql).fetchall()

    checked = len(rows)
    matched = 0
    cleared = 0
    unmatched = 0

    roster_cache: dict[int, sqlite3.Row | None] = {}

    for fa_row in rows:
        roster_row = None
        rid = int(fa_row["roster_player_id"] or 0) if "roster_player_id" in fa_row.keys() and fa_row["roster_player_id"] else 0
        if rid and roster_conn is not None:
            if rid not in roster_cache:
                roster_cache[rid] = roster_conn.execute("SELECT * FROM roster_players WHERE id=?", (rid,)).fetchone()
            roster_row = roster_cache[rid]

        match = None
        for key in candidate_keys_for_player(fa_row, roster_row):
            if key in htd_by_key:
                match = htd_by_key[key]
                break

        if match:
            matched += 1
            if not dry_run:
                cur.execute("""
                    UPDATE free_agents
                    SET hometown_team=?, hometown_seasons=?, franchise_abbr=?, htd=?
                    WHERE id=?
                """, (
                    match["team_name"],
                    int(match["hometown_seasons"] or 0),
                    match["team_abbr"],
                    int(match["hometown_seasons"] or 0),
                    int(fa_row["id"]),
                ))
        else:
            unmatched += 1
            if clear_missing:
                cleared += 1
                if not dry_run:
                    cur.execute("""
                        UPDATE free_agents
                        SET hometown_team='', hometown_seasons=0, franchise_abbr=NULL, htd=0
                        WHERE id=?
                    """, (int(fa_row["id"]),))

    if not dry_run:
        fa_conn.commit()
    fa_conn.close()
    if roster_conn is not None:
        roster_conn.close()

    return {
        "checked": checked,
        "matched": matched,
        "unmatched": unmatched,
        "cleared": cleared if clear_missing else 0,
        "available_htd_keys": len(htd_by_key),
    }


def main() -> None:
    ap = argparse.ArgumentParser(description="Apply hometown discount claims to fa.db/free_agents.")
    ap.add_argument("--fa-db", type=Path, default=APP_DIR / "fa.db")
    ap.add_argument("--roster-db", type=Path, default=APP_DIR / "roster.db")
    ap.add_argument("--htd-db", type=Path, default=APP_DIR / "hometown_discounts.db")
    ap.add_argument("--keep-missing", action="store_true", help="Do not clear rows with no HTD match.")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    summary = apply_hometown_discounts(
        fa_db=args.fa_db,
        roster_db=args.roster_db,
        htd_db=args.htd_db,
        clear_missing=not args.keep_missing,
        dry_run=args.dry_run,
    )
    print(json.dumps(summary, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
