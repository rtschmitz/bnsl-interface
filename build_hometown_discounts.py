#!/usr/bin/env python3
"""
Build the BNSL hometown-discount reference database from OOTP batting/pitching stats.

Eligibility implemented:
  - Only regular-season MLB overall-stat rows count:
      league_level_id == 1, league_abbr == MLB, split_id == 1
    This intentionally ignores minor leagues, vsL/vsR split rows, and playoffs.
  - If the player has exactly one MLB franchise in CURRENT_YEAR:
      * same single franchise in PREVIOUS_YEAR => 2 HTD seasons, 1.10x bid value
      * otherwise                            => 1 HTD season, 1.05x bid value
  - If the player has zero or multiple MLB franchises in CURRENT_YEAR:
      * no hometown discount claim.

The resulting SQLite database is small and can be queried by the FA app without
re-reading the large stats files.
"""
from __future__ import annotations

import argparse
import csv
import json
import re
import sqlite3
import unicodedata
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

APP_DIR = Path(__file__).resolve().parent

from bnsl_paths import generated_path, input_path

ABBR_TO_TEAM = {
    "ARI": "Arizona Diamondbacks",
    "ATL": "Atlanta Braves",
    "BAL": "Baltimore Orioles",
    "BOS": "Boston Red Sox",
    "CHW": "Chicago White Sox",
    "CHC": "Chicago Cubs",
    "CIN": "Cincinnati Reds",
    "CLE": "Cleveland Guardians",
    "COL": "Colorado Rockies",
    "DET": "Detroit Tigers",
    "MIA": "Miami Marlins",
    "HOU": "Houston Astros",
    "KC": "Kansas City Royals",
    "KCR": "Kansas City Royals",
    "LAA": "Los Angeles Angels",
    "LAD": "Los Angeles Dodgers",
    "MIL": "Milwaukee Brewers",
    "MIN": "Minnesota Twins",
    "NYY": "New York Yankees",
    "NYM": "New York Mets",
    "OAK": "Oakland Athletics",
    "PHI": "Philadelphia Phillies",
    "PIT": "Pittsburgh Pirates",
    "SD": "San Diego Padres",
    "SDP": "San Diego Padres",
    "SEA": "Seattle Mariners",
    "SF": "San Francisco Giants",
    "SFG": "San Francisco Giants",
    "STL": "St. Louis Cardinals",
    "TB": "Tampa Bay Rays",
    "TBR": "Tampa Bay Rays",
    "TEX": "Texas Rangers",
    "TOR": "Toronto Blue Jays",
    "WSH": "Washington Nationals",
    "WAS": "Washington Nationals",
    "WSN": "Washington Nationals",
}

CANON_ABBR = {
    "KCR": "KC",
    "SDP": "SD",
    "SFG": "SF",
    "TBR": "TB",
    "WAS": "WSH",
    "WSN": "WSH",
}


def utc_now_text() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def norm_token(value: Any) -> str:
    text = "".join(
        ch for ch in unicodedata.normalize("NFKD", str(value or ""))
        if not unicodedata.combining(ch)
    ).lower()
    return re.sub(r"[^a-z0-9]", "", text)


def clean(value: Any) -> str:
    return str(value or "").strip()


def as_int(value: Any, default: int = 0) -> int:
    try:
        return int(float(str(value or "").strip()))
    except Exception:
        return default


def canonical_abbr(value: Any) -> str:
    abbr = re.sub(r"[^A-Z0-9]", "", str(value or "").upper())
    return CANON_ABBR.get(abbr, abbr)


@dataclass
class PlayerSeen:
    stats_player_id: int
    last_name: str = ""
    first_name: str = ""
    bbref_id: str = ""
    bbrefminors_id: str = ""
    ootp_pid: str = ""
    teams_by_year: dict[int, set[str]] = field(default_factory=lambda: defaultdict(set))
    source_files: set[str] = field(default_factory=set)

    @property
    def full_name(self) -> str:
        return f"{self.first_name} {self.last_name}".strip()


def iter_stat_rows(path: Path, kind: str) -> Iterable[dict[str, Any]]:
    """Yield normalized OOTP stat records from a batting or pitching export."""
    # The exports have comment headers but data rows have no normal CSV header.
    # Keep the positional maps explicit so this is insensitive to cosmetic comments.
    if kind == "batting":
        idx = {
            "player_id": 0,
            "last_name": 1,
            "first_name": 2,
            "year": 3,
            "team_id": 4,
            "split_id": 27,
            "team_abbr": 28,
            "league_abbr": 29,
            "team_name": 30,
            "league_name": 31,
            "league_level_id": 32,
            "bbref_id": 33,
            "bbrefminors_id": 34,
            "ootp_pid": 35,
        }
    elif kind == "pitching":
        idx = {
            "player_id": 0,
            "last_name": 1,
            "first_name": 2,
            "year": 3,
            "team_id": 4,
            "split_id": 45,
            "team_abbr": 46,
            "league_abbr": 47,
            "team_name": 48,
            "league_name": 49,
            "league_level_id": 50,
            "bbref_id": 51,
            "bbrefminors_id": 52,
            "ootp_pid": 53,
        }
    else:
        raise ValueError(f"unknown stats kind: {kind}")

    if not path.exists():
        return

    with path.open(newline="", encoding="utf-8-sig") as f:
        for line_no, raw in enumerate(f, start=1):
            line = raw.strip()
            if not line or line.startswith("//"):
                continue
            try:
                cols = next(csv.reader([line], skipinitialspace=True))
            except Exception:
                continue
            if len(cols) <= max(idx.values()):
                continue
            yield {key: clean(cols[pos]) for key, pos in idx.items()} | {
                "source_path": str(path),
                "source_kind": kind,
                "line_no": line_no,
            }


def is_mlb_regular_overall(row: dict[str, Any]) -> bool:
    return (
        as_int(row.get("split_id"), -1) == 1
        and as_int(row.get("league_level_id"), -1) == 1
        and clean(row.get("league_abbr")).upper() == "MLB"
        and clean(row.get("league_name")).lower() == "major league baseball"
    )


def collect_players(batting_path: Path, pitching_path: Path, years: set[int]) -> dict[int, PlayerSeen]:
    players: dict[int, PlayerSeen] = {}
    for kind, path in (("batting", batting_path), ("pitching", pitching_path)):
        for row in iter_stat_rows(path, kind):
            if not is_mlb_regular_overall(row):
                continue
            year = as_int(row.get("year"), 0)
            if year not in years:
                continue
            pid = as_int(row.get("player_id"), 0)
            if pid <= 0:
                continue
            abbr = canonical_abbr(row.get("team_abbr"))
            if abbr not in ABBR_TO_TEAM:
                continue

            p = players.setdefault(pid, PlayerSeen(stats_player_id=pid))
            p.last_name = p.last_name or clean(row.get("last_name"))
            p.first_name = p.first_name or clean(row.get("first_name"))
            # Keep the first non-empty IDs we see, but do not overwrite later.
            p.bbref_id = p.bbref_id or clean(row.get("bbref_id"))
            p.bbrefminors_id = p.bbrefminors_id or clean(row.get("bbrefminors_id"))
            p.ootp_pid = p.ootp_pid or clean(row.get("ootp_pid"))
            p.teams_by_year[year].add(abbr)
            p.source_files.add(Path(path).name)
    return players


def identity_keys(first_name: str, last_name: str, team_abbr: str, team_name: str,
                  bbref_id: str, bbrefminors_id: str, ootp_pid: str) -> list[tuple[int, str, str]]:
    """
    Generate unique-match keys using the same identity spirit as the roster/OOTP import.
    The stats exports sometimes put bbref-like values in the minors-id column, so ID-like
    strings are exposed under both bbref and bbrefminors prefixes.
    """
    first = norm_token(first_name)
    last = norm_token(last_name)
    tm_abbr = norm_token(team_abbr)
    tm_name = norm_token(team_name)
    bbref = norm_token(bbref_id)
    bbrefm = norm_token(bbrefminors_id)
    oid = norm_token(ootp_pid)

    out: list[tuple[int, str, str]] = []
    if not last:
        return out

    def add(rank: int, method: str, key: str):
        if key:
            out.append((rank, method, key))

    # 1/2. ID+LASTNAME. Include cross-prefix aliases because the OOTP/stat exports
    # are inconsistent about whether a given string is called bbref or bbrefminors.
    for val, label_rank in ((bbref, 1), (bbrefm, 2)):
        if val:
            add(label_rank, "bbref+lastname", f"bbref:{val}:{last}")
            add(label_rank, "bbrefminors+lastname", f"bbrefminors:{val}:{last}")

    # 3. OOTP pID + LASTNAME. This is the string OOTP pID from stats, not the numeric row id.
    if oid:
        add(3, "ootp_pid+lastname", f"ootp:{oid}:{last}")
        # Useful when the roster export stores the OOTP pID in bbrefminors_id.
        add(3, "bbrefminors_alias_ootp_pid+lastname", f"bbrefminors:{oid}:{last}")

    # 4. firstname + LASTNAME + team. Store both abbreviation and full-name versions.
    if first and tm_abbr:
        add(4, "first+last+team_abbr", f"first_last_team:{first}:{last}:{tm_abbr}")
    if first and tm_name:
        add(4, "first+last+team_name", f"first_last_team:{first}:{last}:{tm_name}")

    # 5/6 need DOB, which the stat exports do not contain.

    # 7. firstname + LASTNAME.
    if first:
        add(7, "first+last", f"first_last:{first}:{last}")

    # De-dupe while preserving order.
    seen: set[str] = set()
    deduped: list[tuple[int, str, str]] = []
    for item in out:
        if item[2] not in seen:
            seen.add(item[2])
            deduped.append(item)
    return deduped


def build_database(
    db_path: Path,
    batting_path: Path,
    pitching_path: Path,
    current_year: int,
    previous_year: int,
) -> dict[str, int]:
    players = collect_players(batting_path, pitching_path, {current_year, previous_year})
    created_at = utc_now_text()

    db_path.parent.mkdir(parents=True, exist_ok=True)
    if db_path.exists():
        db_path.unlink()

    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    cur.executescript("""
        PRAGMA journal_mode=WAL;

        CREATE TABLE hometown_discounts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            stats_player_id INTEGER NOT NULL,
            last_name TEXT NOT NULL,
            first_name TEXT NOT NULL,
            full_name TEXT NOT NULL,
            team_abbr TEXT NOT NULL,
            team_name TEXT NOT NULL,
            hometown_seasons INTEGER NOT NULL,
            multiplier REAL NOT NULL,
            current_year INTEGER NOT NULL,
            previous_year INTEGER NOT NULL,
            teams_current_year TEXT NOT NULL,
            teams_previous_year TEXT NOT NULL,
            bbref_id TEXT,
            bbrefminors_id TEXT,
            ootp_pid TEXT,
            source_files TEXT,
            created_at TEXT NOT NULL,
            UNIQUE(stats_player_id, current_year)
        );

        CREATE TABLE hometown_discount_keys (
            key TEXT PRIMARY KEY,
            discount_id INTEGER NOT NULL,
            rank INTEGER NOT NULL,
            method TEXT NOT NULL,
            FOREIGN KEY(discount_id) REFERENCES hometown_discounts(id)
        );

        CREATE TABLE ambiguous_hometown_discount_keys (
            key TEXT NOT NULL,
            discount_id INTEGER NOT NULL,
            rank INTEGER NOT NULL,
            method TEXT NOT NULL,
            FOREIGN KEY(discount_id) REFERENCES hometown_discounts(id)
        );

        CREATE TABLE build_metadata (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        );
    """)

    eligible = 0
    two_year = 0
    one_year = 0
    excluded_multi_current = 0
    excluded_no_current = 0

    key_candidates: dict[str, list[tuple[int, int, str]]] = defaultdict(list)

    for p in players.values():
        current_teams = sorted(p.teams_by_year.get(current_year, set()))
        previous_teams = sorted(p.teams_by_year.get(previous_year, set()))

        if len(current_teams) == 0:
            excluded_no_current += 1
            continue
        if len(current_teams) > 1:
            excluded_multi_current += 1
            continue

        team_abbr = current_teams[0]
        team_name = ABBR_TO_TEAM[team_abbr]
        if len(previous_teams) == 1 and previous_teams[0] == team_abbr:
            seasons = 2
            multiplier = 1.10
            two_year += 1
        else:
            seasons = 1
            multiplier = 1.05
            one_year += 1

        cur.execute("""
            INSERT INTO hometown_discounts(
                stats_player_id, last_name, first_name, full_name,
                team_abbr, team_name, hometown_seasons, multiplier,
                current_year, previous_year,
                teams_current_year, teams_previous_year,
                bbref_id, bbrefminors_id, ootp_pid, source_files, created_at
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            p.stats_player_id,
            p.last_name,
            p.first_name,
            p.full_name,
            team_abbr,
            team_name,
            seasons,
            multiplier,
            current_year,
            previous_year,
            json.dumps(current_teams),
            json.dumps(previous_teams),
            p.bbref_id or None,
            p.bbrefminors_id or None,
            p.ootp_pid or None,
            json.dumps(sorted(p.source_files)),
            created_at,
        ))
        discount_id = int(cur.lastrowid)
        eligible += 1

        for rank, method, key in identity_keys(
            p.first_name, p.last_name, team_abbr, team_name,
            p.bbref_id, p.bbrefminors_id, p.ootp_pid,
        ):
            key_candidates[key].append((discount_id, rank, method))

    # Only expose unique keys for automatic matching. Ambiguous keys are kept for audit.
    unique_keys = 0
    ambiguous_keys = 0
    for key, vals in key_candidates.items():
        unique_discount_ids = {v[0] for v in vals}
        if len(unique_discount_ids) == 1:
            # If the same discount produced aliases with different ranks, keep the best rank.
            discount_id, rank, method = sorted(vals, key=lambda x: x[1])[0]
            cur.execute("""
                INSERT INTO hometown_discount_keys(key, discount_id, rank, method)
                VALUES(?,?,?,?)
            """, (key, discount_id, rank, method))
            unique_keys += 1
        else:
            for discount_id, rank, method in vals:
                cur.execute("""
                    INSERT INTO ambiguous_hometown_discount_keys(key, discount_id, rank, method)
                    VALUES(?,?,?,?)
                """, (key, discount_id, rank, method))
            ambiguous_keys += 1

    cur.executemany(
        "INSERT INTO build_metadata(key, value) VALUES (?, ?)",
        [
            ("created_at", created_at),
            ("current_year", str(current_year)),
            ("previous_year", str(previous_year)),
            ("batting_path", str(batting_path)),
            ("pitching_path", str(pitching_path)),
            ("eligible", str(eligible)),
            ("two_year", str(two_year)),
            ("one_year", str(one_year)),
            ("excluded_multi_current", str(excluded_multi_current)),
            ("excluded_no_current", str(excluded_no_current)),
            ("unique_keys", str(unique_keys)),
            ("ambiguous_keys", str(ambiguous_keys)),
        ],
    )

    cur.execute("CREATE INDEX htd_team_idx ON hometown_discounts(team_abbr)")
    cur.execute("CREATE INDEX htd_name_idx ON hometown_discounts(last_name, first_name)")
    cur.execute("CREATE INDEX htd_key_discount_idx ON hometown_discount_keys(discount_id)")

    conn.commit()
    conn.close()

    return {
        "eligible": eligible,
        "two_year": two_year,
        "one_year": one_year,
        "excluded_multi_current": excluded_multi_current,
        "excluded_no_current": excluded_no_current,
        "unique_keys": unique_keys,
        "ambiguous_keys": ambiguous_keys,
    }


def main() -> None:
    ap = argparse.ArgumentParser(description="Build BNSL hometown-discount reference DB from OOTP stat exports.")
    ap.add_argument("--batting", type=Path, default=input_path("player_batting_stats.txt"))
    ap.add_argument("--pitching", type=Path, default=input_path("player_pitching_stats.txt"))
    ap.add_argument("--out", type=Path, default=generated_path("hometown_discounts.db"))
    ap.add_argument("--current-year", type=int, default=2025)
    ap.add_argument("--previous-year", type=int, default=2024)
    args = ap.parse_args()

    summary = build_database(args.out, args.batting, args.pitching, args.current_year, args.previous_year)
    print(f"Wrote {args.out}")
    print(json.dumps(summary, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
