#!/usr/bin/env python3
"""
Inject OVR/POT/DEF ratings into the BNSL roster, Rule V, and free agency DBs.

Defaults are intentionally simple:
  ratings export: ~/bnsl/bnsl_ootp2027_allratingsexport.csv
                  fallback ~/bnsldata/bnsl_ootp2027_allratingsexport.csv
  DBs:            ~/bnsldata/roster.db
                  ~/bnsldata/rulev.db
                  ~/bnsldata/fa.db

Typical use:
  python ~/bnsl/inject_player_ratings_ovr_pot_def.py

Recommended first pass:
  python ~/bnsl/inject_player_ratings_ovr_pot_def.py --dry-run

The script:
  1. updates roster_players.ovr / .pot / .def from the ratings export
  2. mirrors those ratings into rulev_players and free_agents through roster_player_id
  3. writes a CSV report of matches/skips/ambiguities
  4. makes timestamped DB backups unless --no-backup is used
"""

from __future__ import annotations

import argparse
import csv
import re
import shutil
import sqlite3
import sys
import unicodedata
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

# Ordered to match the identity fallback used elsewhere in the app.
MATCH_ORDER = (
    "bbref_last",
    "bbrefminors_last",
    "ootp_last",
    "first_last_team",
    "first_last_dob",
    "last_dob",
    "first_last",
)

RATING_SPECS = {
    "ovr": {
        "db_col_arg": "db_ovr_col",
        "source_arg": "ovr_col",
        "candidates": ("OVR", "Overall", "Overall Rating", "Current Overall", "Current Rating", "Current Ability", "Ability"),
    },
    "pot": {
        "db_col_arg": "db_pot_col",
        "source_arg": "pot_col",
        "candidates": ("POT", "Pot", "Potential", "Potential Rating", "Overall Potential", "Potential Ability"),
    },
    "def": {
        "db_col_arg": "db_def_col",
        "source_arg": "def_col",
        "candidates": ("DEF", "Defense", "Defensive Rating", "Current Defense"),
    },
}

TEAM_ABBR_TO_NAME = {
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
    "SEA": "Seattle Mariners",
    "SF": "San Francisco Giants",
    "STL": "St. Louis Cardinals",
    "TB": "Tampa Bay Rays",
    "TEX": "Texas Rangers",
    "TOR": "Toronto Blue Jays",
    "WAS": "Washington Nationals",
    "WSH": "Washington Nationals",
}

TEAM_NAME_TO_ABBR = {v: k for k, v in TEAM_ABBR_TO_NAME.items()}
TEAM_NAME_TO_ABBR.update({
    "Athletics": "OAK",
    "Oakland Athletics": "OAK",
    "Kansas City Royals": "KC",
    "San Diego Padres": "SD",
    "San Francisco Giants": "SF",
    "Tampa Bay Rays": "TB",
    "Washington Nationals": "WAS",
})

# The new OOTP CSV uses city-style TM values.  These are safe only where the city
# uniquely identifies one BNSL club in the export.  Chicago / Los Angeles /
# New York remain intentionally unmapped and will fall through to DOB/ID matching.
OOTP_CITY_TO_ABBR = {
    "Arizona": "ARI",
    "Atlanta": "ATL",
    "Baltimore": "BAL",
    "Boston": "BOS",
    "Cincinnati": "CIN",
    "Cleveland": "CLE",
    "Colorado": "COL",
    "Detroit": "DET",
    "Houston": "HOU",
    "Kansas City": "KC",
    "Miami": "MIA",
    "Milwaukee": "MIL",
    "Minnesota": "MIN",
    "Oakland": "OAK",
    "Philadelphia": "PHI",
    "Pittsburgh": "PIT",
    "San Diego": "SD",
    "San Francisco": "SF",
    "Seattle": "SEA",
    "St. Louis": "STL",
    "Tampa Bay": "TB",
    "Texas": "TEX",
    "Toronto": "TOR",
    "Washington": "WAS",
}


def quote_ident(name: str) -> str:
    return '"' + str(name).replace('"', '""') + '"'


def norm_token(value: Any) -> str:
    if value is None:
        return ""
    text = str(value).strip().casefold()
    text = unicodedata.normalize("NFKD", text)
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    return re.sub(r"[^a-z0-9]+", "", text)


def row_get_ci(row: Dict[str, Any], *names: str) -> str:
    lower = {str(k).strip().casefold(): v for k, v in row.items()}
    for name in names:
        val = lower.get(str(name).strip().casefold())
        if val is not None:
            return str(val).strip()
    return ""


def canonical_header_name(header: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", str(header or "").casefold())


def find_column(headers: Iterable[str], requested: Optional[str], candidates: Iterable[str]) -> Optional[str]:
    headers = list(headers)
    by_exact = {str(h).strip(): h for h in headers}
    by_norm: Dict[str, str] = {}
    for h in headers:
        by_norm.setdefault(canonical_header_name(h), h)

    if requested:
        if requested in by_exact:
            return by_exact[requested]
        found = by_norm.get(canonical_header_name(requested))
        if found:
            return found
        raise RuntimeError(f"Requested ratings column not found: {requested!r}")

    for cand in candidates:
        found = by_norm.get(canonical_header_name(cand))
        if found:
            return found
    return None


def parse_intish(value: Any) -> Optional[int]:
    text = "" if value is None else str(value).strip()
    if text == "" or text in {"-", "—", "NA", "N/A", "nan", "None"}:
        return None
    try:
        val = float(text.replace(",", ""))
    except ValueError as exc:
        raise ValueError(f"Non-numeric rating value: {value!r}") from exc
    return int(val) if val.is_integer() else int(round(val))


def infer_two_digit_year(yy: int) -> int:
    # Baseball DOBs with 00-26 are 2000-2026; everything else is 1900s.
    return 2000 + yy if yy <= 26 else 1900 + yy


def normalize_date(value: Any, *, assume_mdy: bool = False) -> str:
    """Normalize YYYY-MM-DD, MM/DD/YYYY, DD-MM-YYYY, or DD-MM-YY into YYYY-MM-DD."""
    text = str(value or "").strip()
    if not text or text in {"-", "—"}:
        return ""

    parts = [p for p in re.split(r"\D+", text) if p]
    if len(parts) >= 3:
        try:
            if len(parts[0]) == 4:
                y, m, d = int(parts[0]), int(parts[1]), int(parts[2])
            else:
                a, b = int(parts[0]), int(parts[1])
                y = int(parts[2])
                if y < 100:
                    y = infer_two_digit_year(y)

                # OOTP's new ratings CSV is MM/DD/YYYY.  Older roster imports can be
                # DD-MM-YY.  If the first number is >12, it cannot be month.
                if assume_mdy or a <= 12:
                    m, d = a, b
                else:
                    d, m = a, b

            if 1 <= m <= 12 and 1 <= d <= 31 and 1900 <= y <= 2100:
                return f"{y:04d}-{m:02d}-{d:02d}"
        except Exception:
            pass

    # A last-resort year-only fallback is useful for diagnostics but not matching.
    m = re.search(r"(19\d{2}|20\d{2})", text)
    return m.group(1) if m else text


def split_name(name: Any) -> tuple[str, str]:
    bits = [b for b in re.split(r"\s+", str(name or "").strip()) if b]
    if not bits:
        return "", ""
    if len(bits) == 1:
        return "", bits[0]
    return bits[0], bits[-1]


def full_name_from_source(row: Dict[str, Any]) -> str:
    existing = row_get_ci(row, "Name", "name", "full_name", "fullname", "Player", "Player Name")
    if existing:
        return existing
    first = row_get_ci(row, "First Name", "FirstName", "first_name")
    last = row_get_ci(row, "Last Name", "LastName", "last_name")
    return " ".join(x for x in (first, last) if x).strip()


def first_name_from_row(row: Dict[str, Any]) -> str:
    first = row_get_ci(row, "first_name", "First Name", "FirstName")
    if first:
        return first
    f, _ = split_name(row_get_ci(row, "name", "Name"))
    return f


def last_name_from_row(row: Dict[str, Any]) -> str:
    last = row_get_ci(row, "last_name", "Last Name", "LastName")
    if last:
        return last
    _, l = split_name(row_get_ci(row, "name", "Name"))
    return l


def read_ootp_export(path: Path) -> tuple[List[str], List[Dict[str, str]]]:
    """
    Read either:
      - normal CSV with a first-row header
      - OOTP text export with a comment header line beginning //id,...
    """
    raw_lines = path.read_text(encoding="utf-8-sig", errors="replace").splitlines()

    header_line = None
    data_lines: List[str] = []
    for line in raw_lines:
        stripped = line.strip()
        if not stripped:
            continue
        if stripped.startswith("//id,") or stripped.startswith("//ID,"):
            header_line = stripped[2:]
            continue
        if stripped.startswith("//"):
            continue
        data_lines.append(line)

    if header_line is None:
        with path.open(newline="", encoding="utf-8-sig", errors="replace") as f:
            reader = csv.DictReader(f)
            if not reader.fieldnames:
                raise RuntimeError(f"No header found in {path}")
            return list(reader.fieldnames), [dict(r) for r in reader]

    headers = next(csv.reader([header_line]))
    rows: List[Dict[str, str]] = []
    for line_no, line in enumerate(data_lines, start=1):
        values = next(csv.reader([line]))
        if len(values) == len(headers) + 1 and values[-1].strip().casefold() == "eol":
            values = values[:-1]
        if len(values) != len(headers):
            continue
        row = dict(zip(headers, values))
        row["_source_line"] = str(line_no)
        rows.append(row)
    return headers, rows


def team_tokens(value: Any) -> list[str]:
    raw = str(value or "").strip()
    if not raw or raw == "-":
        return []

    out: list[str] = []

    def add(x: Any) -> None:
        tok = norm_token(x)
        if tok and tok not in out:
            out.append(tok)

    add(raw)

    upper = re.sub(r"[^A-Z0-9]", "", raw.upper())
    if upper in TEAM_ABBR_TO_NAME:
        add(upper)
        add(TEAM_ABBR_TO_NAME[upper])
        # Also add city-only aliases where unique in the OOTP export.
        for city, abbr in OOTP_CITY_TO_ABBR.items():
            if abbr == upper:
                add(city)

    if raw in TEAM_NAME_TO_ABBR:
        abbr = TEAM_NAME_TO_ABBR[raw]
        add(abbr)
        add(TEAM_ABBR_TO_NAME.get(abbr, raw))

    if raw in OOTP_CITY_TO_ABBR:
        abbr = OOTP_CITY_TO_ABBR[raw]
        add(abbr)
        add(TEAM_ABBR_TO_NAME.get(abbr, raw))

    return out


def player_key_entries(
    *,
    first_name: Any,
    last_name: Any,
    dob: Any,
    team: Any,
    bbref_id: Any = "",
    bbrefminors_id: Any = "",
    ootp_id: Any = "",
) -> list[tuple[str, str]]:
    """
    Ordered exactly like the requested cross-file identity fallback:
      1. bbrefidLASTNAME
      2. bbrefminorsidLASTNAME
      3. ootpidLASTNAME
      4. firstnameLASTNAMEteam
      5. firstnameLASTNAMEDOB
      6. LASTNAMEDOB
      7. firstnameLASTNAME
    """
    first = norm_token(first_name)
    last = norm_token(last_name)
    birth = norm_token(normalize_date(dob))
    bbref = norm_token(bbref_id)
    bbrefm = norm_token(bbrefminors_id)
    oid = norm_token(ootp_id)

    if not last:
        return []

    out: list[tuple[str, str]] = []

    def add(method: str, key: str) -> None:
        item = (method, key)
        if key and item not in out:
            out.append(item)

    if bbref:
        add("bbref_last", f"bbref:{bbref}:{last}")
    if bbrefm:
        add("bbrefminors_last", f"bbrefminors:{bbrefm}:{last}")
    if oid:
        add("ootp_last", f"ootp:{oid}:{last}")
    if first:
        for tt in team_tokens(team):
            add("first_last_team", f"first_last_team:{first}:{last}:{tt}")
        if birth:
            add("first_last_dob", f"first_last_dob:{first}:{last}:{birth}")
    if birth:
        add("last_dob", f"last_dob:{last}:{birth}")
    if first:
        add("first_last", f"first_last:{first}:{last}")

    return out


def source_team_value(row: Dict[str, Any]) -> str:
    return row_get_ci(row, "franchise", "TM", "Team", "Team Name", "team_name", "Org", "Organization")


def source_dob_value(row: Dict[str, Any]) -> str:
    # New ratings export uses DOB.  Older OOTP text exports sometimes use DayOB/MonthOB/YearOB.
    dob = row_get_ci(row, "DOB", "date_of_birth", "birth_date", "dob")
    if dob:
        return normalize_date(dob, assume_mdy=True)

    day = row_get_ci(row, "DayOB", "day_of_birth", "day")
    month = row_get_ci(row, "MonthOB", "month_of_birth", "month")
    year = row_get_ci(row, "YearOB", "year_of_birth", "year")
    if day and month and year:
        return normalize_date(f"{year}-{month}-{day}")
    return ""


def prepare_source_rows(args: argparse.Namespace) -> tuple[List[Dict[str, Any]], Dict[str, str], List[str]]:
    headers, raw_rows = read_ootp_export(args.ratings)

    source_cols: Dict[str, str] = {}
    for logical, spec in RATING_SPECS.items():
        requested = getattr(args, spec["source_arg"])
        found = find_column(headers, requested, spec["candidates"])
        if not found:
            interesting = [h for h in headers if re.search(r"ovr|overall|pot|potential|def|defense|rating|ability", h, re.I)]
            raise RuntimeError(
                f"Could not auto-detect source column for {logical.upper()}.\n"
                f"Ratings file: {args.ratings}\n"
                f"Columns with rating-ish names:\n  - " + "\n  - ".join(interesting[:100]) + "\n\n"
                f"Rerun with --{logical}-col \"EXACT HEADER\" if needed."
            )
        source_cols[logical] = found

    source_rows: List[Dict[str, Any]] = []
    for raw in raw_rows:
        name = full_name_from_source(raw)
        first = row_get_ci(raw, "First Name", "FirstName", "first_name")
        last = row_get_ci(raw, "Last Name", "LastName", "last_name")
        if not first or not last:
            f2, l2 = split_name(name)
            first = first or f2
            last = last or l2

        row = dict(raw)
        row["name"] = name
        row["first_name"] = first
        row["last_name"] = last
        row["franchise"] = source_team_value(raw)
        row["date_of_birth"] = source_dob_value(raw)
        row["_source_id"] = row_get_ci(raw, "ID", "id", "player_id", "ootp_id")

        for logical, col in source_cols.items():
            row[f"_source_{logical}"] = parse_intish(raw.get(col))

        source_rows.append(row)

    return source_rows, source_cols, headers


def build_source_maps(source_rows: Iterable[Dict[str, Any]]) -> dict[str, dict[str, list[Dict[str, Any]]]]:
    maps: dict[str, dict[str, list[Dict[str, Any]]]] = {m: defaultdict(list) for m in MATCH_ORDER}
    for row in source_rows:
        entries = player_key_entries(
            first_name=row.get("first_name"),
            last_name=row.get("last_name"),
            dob=row.get("date_of_birth"),
            team=row.get("franchise"),
            ootp_id=row.get("_source_id"),
        )
        for method, key in entries:
            maps[method][key].append(row)
    return maps


def get_columns(con: sqlite3.Connection, table: str) -> list[str]:
    rows = con.execute(f"PRAGMA table_info({quote_ident(table)})").fetchall()
    return [r[1] for r in rows]


def ensure_column(con: sqlite3.Connection, table: str, col: str, coldef: str, dry_run: bool = False) -> None:
    if col in set(get_columns(con, table)):
        return
    if dry_run:
        return
    con.execute(f"ALTER TABLE {quote_ident(table)} ADD COLUMN {coldef}")


def select_expr(col: str, cols: set[str]) -> str:
    return quote_ident(col) if col in cols else f"NULL AS {quote_ident(col)}"


def fetch_roster_players(con: sqlite3.Connection, table: str, db_cols: dict[str, str]) -> list[Dict[str, Any]]:
    cols = set(get_columns(con, table))
    if "id" not in cols or "name" not in cols:
        raise RuntimeError(f"{table} must have at least id and name columns")

    identity_cols = [
        "id", "name", "first_name", "last_name", "franchise", "date_of_birth",
        "bbref_id", "bbrefminors_id", "ootp_id",
    ]
    rendered = [select_expr(c, cols) for c in identity_cols]
    rendered += [select_expr(c, cols) for c in db_cols.values()]

    rows = con.execute(
        f"SELECT {', '.join(rendered)} FROM {quote_ident(table)} ORDER BY {quote_ident('id')}"
    ).fetchall()
    return [dict(r) for r in rows]


def roster_key_entries(player: Dict[str, Any]) -> list[tuple[str, str]]:
    first = player.get("first_name")
    last = player.get("last_name")
    if not first or not last:
        f2, l2 = split_name(player.get("name", ""))
        first = first or f2
        last = last or l2

    return player_key_entries(
        first_name=first,
        last_name=last,
        dob=normalize_date(player.get("date_of_birth")),
        team=player.get("franchise"),
        bbref_id=player.get("bbref_id"),
        bbrefminors_id=player.get("bbrefminors_id"),
        ootp_id=player.get("ootp_id") or player.get("id"),
    )


def values_differ(a: Any, b: Any) -> bool:
    if a == "":
        a = None
    if b == "":
        b = None
    return a != b


def write_report(path: Path, rows: list[dict[str, Any]], db_cols: dict[str, str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    rating_fields: list[str] = []
    for logical in ("ovr", "pot", "def"):
        rating_fields.extend([f"old_{logical}", f"new_{logical}"])

    fieldnames = [
        "status", "match_method", "match_key",
        "db_id", "db_name", "db_franchise", "db_date_of_birth",
        *rating_fields,
        "source_id", "source_name", "source_franchise", "source_date_of_birth",
        "reason",
    ]
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def update_roster(args: argparse.Namespace, source_rows: list[Dict[str, Any]], db_cols: dict[str, str]) -> Counter:
    con = sqlite3.connect(str(args.roster_db))
    con.row_factory = sqlite3.Row
    report_rows: list[dict[str, Any]] = []
    updates: list[tuple[Any, Any, Any, int]] = []
    summary: Counter = Counter(csv_rows=len(source_rows))

    try:
        for logical, db_col in db_cols.items():
            ensure_column(con, args.roster_table, db_col, f"{quote_ident(db_col)} INTEGER", dry_run=args.dry_run)

        players = fetch_roster_players(con, args.roster_table, db_cols)
        source_maps = build_source_maps(source_rows)
        summary["roster_rows"] = len(players)

        for player in players:
            entries = roster_key_entries(player)
            matched = None
            match_method = ""
            match_key = ""
            ambiguous_candidates: Optional[list[Dict[str, Any]]] = None
            ambiguous_method = ""
            ambiguous_key = ""

            for method, key in entries:
                candidates = source_maps.get(method, {}).get(key, [])
                if len(candidates) == 1:
                    matched = candidates[0]
                    match_method = method
                    match_key = key
                    break
                if len(candidates) > 1 and ambiguous_candidates is None:
                    ambiguous_candidates = candidates
                    ambiguous_method = method
                    ambiguous_key = key

            base_report: dict[str, Any] = {
                "db_id": player.get("id"),
                "db_name": player.get("name", ""),
                "db_franchise": player.get("franchise", ""),
                "db_date_of_birth": normalize_date(player.get("date_of_birth")),
                "match_method": match_method or ambiguous_method,
                "match_key": match_key or ambiguous_key,
                "source_id": "",
                "source_name": "",
                "source_franchise": "",
                "source_date_of_birth": "",
                "reason": "",
            }
            for logical, db_col in db_cols.items():
                base_report[f"old_{logical}"] = player.get(db_col)
                base_report[f"new_{logical}"] = ""

            if matched is None:
                if ambiguous_candidates is not None:
                    summary[f"ambiguous_{ambiguous_method}"] += 1
                    report_rows.append({
                        **base_report,
                        "status": "skipped_ambiguous",
                        "source_id": ";".join(str(c.get("_source_id", "")) for c in ambiguous_candidates[:10]),
                        "source_name": ";".join(str(c.get("name", "")) for c in ambiguous_candidates[:10]),
                        "source_franchise": ";".join(str(c.get("franchise", "")) for c in ambiguous_candidates[:10]),
                        "reason": "multiple source candidates for this key",
                    })
                else:
                    summary["no_match"] += 1
                    report_rows.append({**base_report, "status": "no_match"})
                continue

            summary[f"matched_{match_method}"] += 1

            new_values = {
                logical: matched.get(f"_source_{logical}")
                for logical in ("ovr", "pot", "def")
            }
            for logical, value in new_values.items():
                base_report[f"new_{logical}"] = "" if value is None else value

            base_report.update({
                "source_id": matched.get("_source_id", ""),
                "source_name": matched.get("name", ""),
                "source_franchise": matched.get("franchise", ""),
                "source_date_of_birth": matched.get("date_of_birth", ""),
            })

            if all(v is None for v in new_values.values()):
                summary["skipped_blank_ratings"] += 1
                report_rows.append({**base_report, "status": "skipped_blank_ratings", "reason": "source ratings were blank"})
                continue

            changed = any(values_differ(player.get(db_cols[logical]), new_values[logical]) for logical in ("ovr", "pot", "def"))
            if not changed:
                summary["matched_no_change"] += 1
                continue

            updates.append((new_values["ovr"], new_values["pot"], new_values["def"], int(player["id"])))
            report_rows.append({**base_report, "status": "updated"})

        if not args.dry_run:
            con.executemany(
                f"""
                UPDATE {quote_ident(args.roster_table)}
                SET {quote_ident(db_cols['ovr'])} = ?,
                    {quote_ident(db_cols['pot'])} = ?,
                    {quote_ident(db_cols['def'])} = ?
                WHERE id = ?
                """,
                updates,
            )
            con.commit()
        else:
            con.rollback()
    finally:
        con.close()

    write_report(args.report, report_rows, db_cols)
    summary["updated_roster"] = len(updates)
    summary["ambiguous_total"] = sum(v for k, v in summary.items() if k.startswith("ambiguous_"))
    return summary


def mirror_linked_table(
    roster_db: Path,
    target_db: Path,
    table: str,
    target_id_col: str,
    roster_id_col: str,
    db_cols: dict[str, str],
    dry_run: bool,
) -> Counter:
    summary: Counter = Counter()
    if not target_db.exists():
        summary["missing_db"] = 1
        return summary

    rcon = sqlite3.connect(str(roster_db))
    rcon.row_factory = sqlite3.Row
    tcon = sqlite3.connect(str(target_db))
    tcon.row_factory = sqlite3.Row

    try:
        roster_cols = set(get_columns(rcon, "roster_players"))
        missing_roster_cols = [c for c in db_cols.values() if c not in roster_cols]
        if missing_roster_cols:
            if dry_run:
                summary["skipped_dry_run_missing_roster_rating_columns"] = 1
                return summary
            raise RuntimeError(f"roster_players is missing rating columns: {missing_roster_cols}")

        for db_col in db_cols.values():
            ensure_column(tcon, table, db_col, f"{quote_ident(db_col)} INTEGER", dry_run=dry_run)

        tcols = set(get_columns(tcon, table))
        if roster_id_col not in tcols:
            summary["missing_roster_player_id_column"] = 1
            return summary

        rendered_rating_cols = [select_expr(c, tcols) for c in db_cols.values()]
        target_rows = tcon.execute(
            f"""
            SELECT {quote_ident(target_id_col)} AS target_id,
                   {quote_ident(roster_id_col)} AS roster_player_id,
                   {', '.join(rendered_rating_cols)}
            FROM {quote_ident(table)}
            WHERE {quote_ident(roster_id_col)} IS NOT NULL
            ORDER BY {quote_ident(target_id_col)}
            """
        ).fetchall()

        roster_ids = sorted({int(r["roster_player_id"]) for r in target_rows if r["roster_player_id"] is not None})
        summary["linked_rows"] = len(target_rows)
        if not roster_ids:
            return summary

        rating_select = ", ".join(f"{quote_ident(c)} AS {quote_ident(logical)}" for logical, c in db_cols.items())
        roster_rating_rows = rcon.execute(
            f"""
            SELECT id, {rating_select}
            FROM roster_players
            WHERE id IN ({','.join('?' for _ in roster_ids)})
            """,
            roster_ids,
        ).fetchall()
        ratings_by_id = {
            int(r["id"]): {logical: r[logical] for logical in ("ovr", "pot", "def")}
            for r in roster_rating_rows
        }

        updates: list[tuple[Any, Any, Any, int]] = []
        for row in target_rows:
            rid = int(row["roster_player_id"])
            if rid not in ratings_by_id:
                summary["missing_roster_link"] += 1
                continue
            new = ratings_by_id[rid]
            changed = any(values_differ(row[db_cols[logical]], new[logical]) for logical in ("ovr", "pot", "def"))
            if changed:
                updates.append((new["ovr"], new["pot"], new["def"], int(row["target_id"])))

        summary["updated_rows"] = len(updates)
        if updates and not dry_run:
            tcon.executemany(
                f"""
                UPDATE {quote_ident(table)}
                SET {quote_ident(db_cols['ovr'])} = ?,
                    {quote_ident(db_cols['pot'])} = ?,
                    {quote_ident(db_cols['def'])} = ?
                WHERE {quote_ident(target_id_col)} = ?
                """,
                updates,
            )
            tcon.commit()
        elif dry_run:
            tcon.rollback()
    finally:
        rcon.close()
        tcon.close()

    return summary


def make_backups(paths: Iterable[Path]) -> list[Path]:
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backups: list[Path] = []
    for path in paths:
        if not path.exists():
            continue
        backup = path.with_name(f"{path.name}.bak_{stamp}")
        shutil.copy2(path, backup)
        backups.append(backup)
    return backups


def default_ratings_path(app_dir: Path, data_dir: Path) -> Path:
    for name in (
        "bnsl_ootp2027_allratingsexport.csv",
        "bnsl_ootp27_allratingsexport.csv",
        "bnsl_ootp27_fixed_rosters_oldids_optionsupdated.txt",
    ):
        preferred = app_dir / name
        if preferred.exists():
            return preferred
        fallback = data_dir / name
        if fallback.exists():
            return fallback
    # Best error message if none exists.
    return app_dir / "bnsl_ootp2027_allratingsexport.csv"


def parse_args(argv: list[str]) -> argparse.Namespace:
    home = Path.home()
    parser = argparse.ArgumentParser(description="Inject OVR/POT/DEF ratings into BNSL roster, Rule V, and FA DBs.")
    parser.add_argument("--data-dir", type=Path, default=home / "bnsldata")
    parser.add_argument("--app-dir", type=Path, default=home / "bnsl")
    parser.add_argument("--ratings", type=Path, default=None, help="Ratings export CSV. Default: ~/bnsl/bnsl_ootp2027_allratingsexport.csv")

    parser.add_argument("--roster-db", type=Path, default=None)
    parser.add_argument("--rulev-db", type=Path, default=None)
    parser.add_argument("--fa-db", type=Path, default=None)

    parser.add_argument("--roster-table", default="roster_players")
    parser.add_argument("--rulev-table", default="rulev_players")
    parser.add_argument("--fa-table", default="free_agents")

    parser.add_argument("--db-ovr-col", default="ovr")
    parser.add_argument("--db-pot-col", default="pot")
    parser.add_argument("--db-def-col", default="def")

    parser.add_argument("--ovr-col", default=None, help="Exact source OVR column if auto-detect fails")
    parser.add_argument("--pot-col", default=None, help="Exact source POT column if auto-detect fails")
    parser.add_argument("--def-col", default=None, help="Exact source DEF column if auto-detect fails")

    parser.add_argument("--report", type=Path, default=None)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--no-backup", action="store_true", help="Do not create timestamped DB backups before updating")
    return parser.parse_args(argv)


def resolve_paths(args: argparse.Namespace) -> None:
    args.roster_db = args.roster_db or (args.data_dir / "roster.db")
    args.rulev_db = args.rulev_db or (args.data_dir / "rulev.db")
    args.fa_db = args.fa_db or (args.data_dir / "fa.db")
    args.ratings = args.ratings or default_ratings_path(args.app_dir, args.data_dir)
    args.report = args.report or (args.data_dir / "ratings_injection_report.csv")


def main(argv: list[str]) -> int:
    args = parse_args(argv)
    resolve_paths(args)

    db_cols = {
        "ovr": args.db_ovr_col,
        "pot": args.db_pot_col,
        "def": args.db_def_col,
    }

    try:
        for path in (args.roster_db, args.ratings):
            if not path.exists():
                raise FileNotFoundError(path)

        source_rows, source_cols, _headers = prepare_source_rows(args)

        if not args.dry_run and not args.no_backup:
            backups = make_backups([args.roster_db, args.rulev_db, args.fa_db])
            for backup in backups:
                print(f"Backup written: {backup}")

        summary: Counter = Counter()
        summary["source_rows"] = len(source_rows)
        for logical, source_col in source_cols.items():
            summary[f"source_{logical}_col"] = source_col

        roster_summary = update_roster(args, source_rows, db_cols)
        summary.update({f"roster_{k}": v for k, v in roster_summary.items()})

        rulev_summary = mirror_linked_table(
            roster_db=args.roster_db,
            target_db=args.rulev_db,
            table=args.rulev_table,
            target_id_col="id",
            roster_id_col="roster_player_id",
            db_cols=db_cols,
            dry_run=args.dry_run,
        )
        summary.update({f"rulev_{k}": v for k, v in rulev_summary.items()})

        fa_summary = mirror_linked_table(
            roster_db=args.roster_db,
            target_db=args.fa_db,
            table=args.fa_table,
            target_id_col="id",
            roster_id_col="roster_player_id",
            db_cols=db_cols,
            dry_run=args.dry_run,
        )
        summary.update({f"fa_{k}": v for k, v in fa_summary.items()})

    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1

    print("\nRating injection summary")
    print("========================")
    print(f"ratings file: {args.ratings}")
    print(f"roster DB: {args.roster_db}")
    print(f"rule V DB: {args.rulev_db}")
    print(f"FA DB: {args.fa_db}")
    print(f"report: {args.report}")
    print(f"dry run: {args.dry_run}")
    for key in sorted(summary):
        print(f"{key}: {summary[key]}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
