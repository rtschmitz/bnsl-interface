#!/usr/bin/env python3
"""
Build an OOTP roster import from a BNSL roster export.

Inputs
------
1) OOTP player export CSV, e.g. mlb_rosters.csv
2) BNSL roster/player export CSV, e.g. roster_players.csv
3) OOTP league_structure.xml

Outputs
-------
1) Updated OOTP player import CSV with team_id, ML service, 40-man service,
   and options used overwritten for successfully matched BNSL players.
2) BNSL id -> OOTP id mapping CSV for future runs.
3) Audit CSV listing low-quality matches, ambiguous matches, duplicate targets,
   missing franchises/affiliates, bad service/options inputs, and OOTP players
   cleared from franchises because they were not matched to any BNSL player.

Matching model
--------------
Hard-way matching compares five normalized qualities:
  first_name, last_name-with-suffix-removed, DOB, bbref_id, bbrefminors_id.
A unique candidate with match_score >= MATCH_THRESHOLD is accepted. Low-score or ambiguous candidates can also be rescued by BNSL/OOTP id + exact normalized first+last, and ambiguous ties are resolved by a unique normalized first+last match.

After matching and updating successful BNSL players, any OOTP player row that still has
team_id > 0 but was not successfully matched to a BNSL player is cleared to team_id=0
by default. Use --no-clear-unmatched-franchised to disable that cleanup.

Future id-map mode can be enabled either by setting USE_ID_MAP=True below or
by passing --use-id-map. It will use --input-id-map if supplied, otherwise it
uses the BNSL export's ootp_id column.
"""

from __future__ import annotations

import argparse
import csv
import re
import sys
import unicodedata
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

# Flip this to True once you trust a previously produced map.
USE_ID_MAP = False
MATCH_THRESHOLD = 3
SERVICE_DAYS_PER_YEAR = 172

# BNSL abbreviations that differ from OOTP/XML abbreviations.
FRANCHISE_ALIASES = {
    "CHW": "CWS",
    "CWS": "CWS",
    "WAS": "WSH",
    "WSN": "WSH",
    "TBR": "TB",
    "KCR": "KC",
    "SFG": "SF",
    "SDP": "SD",
}

SUFFIX_TOKENS = {"jr", "sr", "ii", "iii", "iv", "v"}
NULL_ID_VALUES = {"", "0", "none", "null", "nan", "n/a", "na", "-"}


def clean_header_name(name: str) -> str:
    """Canonicalize an OOTP header while keeping the original header row untouched."""
    name = name.strip()
    if name.startswith("//"):
        name = name[2:]
    return re.sub(r"\s+", " ", name).strip().lower()


def read_csv_rows(path: Path, encoding: str = "utf-8-sig") -> List[List[str]]:
    with path.open("r", newline="", encoding=encoding, errors="replace") as f:
        return list(csv.reader(f))


def write_csv_rows(path: Path, rows: Iterable[Iterable[str]], encoding: str = "utf-8") -> None:
    with path.open("w", newline="", encoding=encoding) as f:
        writer = csv.writer(f)
        writer.writerows(rows)


def read_dict_csv(path: Path, encoding: str = "utf-8-sig") -> Tuple[List[str], List[Dict[str, str]]]:
    with path.open("r", newline="", encoding=encoding, errors="replace") as f:
        reader = csv.DictReader(f)
        return list(reader.fieldnames or []), list(reader)


def strip_accents(value: str) -> str:
    return "".join(
        c for c in unicodedata.normalize("NFKD", value or "")
        if not unicodedata.combining(c)
    )


def normalize_name_part(value: str, remove_suffix: bool = False) -> str:
    """Normalize name pieces for matching; removes accents/punctuation/suffixes."""
    value = strip_accents(value or "").lower()
    value = value.replace("’", "'").replace("`", "'")
    # Keep token boundaries long enough to strip suffixes, then collapse.
    tokens = re.findall(r"[a-z0-9]+", value)
    if remove_suffix:
        while tokens and tokens[-1] in SUFFIX_TOKENS:
            tokens.pop()
    return "".join(tokens)


def normalize_id(value: str) -> str:
    value = (value or "").strip().lower()
    return "" if value in NULL_ID_VALUES else value


def parse_int(value: str, default: Optional[int] = None) -> Optional[int]:
    try:
        if value is None or str(value).strip() == "":
            return default
        return int(float(str(value).strip()))
    except ValueError:
        return default


def bns_dob(row: Dict[str, str]) -> str:
    raw = (row.get("date_of_birth") or "").strip()
    if not raw:
        return ""
    # The BNSL export currently uses YYYY-MM-DD. Keep this tolerant.
    m = re.match(r"^(\d{4})[-/](\d{1,2})[-/](\d{1,2})$", raw)
    if not m:
        return raw
    y, mo, d = map(int, m.groups())
    return f"{y:04d}-{mo:02d}-{d:02d}"


def ootp_dob(row: Dict[str, str]) -> str:
    y = parse_int(row.get("yearob", ""))
    m = parse_int(row.get("monthob", ""))
    d = parse_int(row.get("dayob", ""))
    if not (y and m and d):
        return ""
    return f"{y:04d}-{m:02d}-{d:02d}"


def split_bns_name(row: Dict[str, str]) -> Tuple[str, str]:
    first = row.get("first_name") or ""
    last = row.get("last_name") or ""
    if not first or not last:
        parts = (row.get("name") or "").split()
        if parts and not first:
            first = parts[0]
        if len(parts) > 1 and not last:
            last = parts[-1]
    # If suffix is stored separately, last_name normally already excludes it;
    # normalize_name_part(remove_suffix=True) handles either case.
    return first, last


@dataclass
class PlayerIdentity:
    source_index: int
    row: Dict[str, str]
    source_row_number: int = 0
    player_id: str = ""
    first_raw: str = ""
    last_raw: str = ""
    first: str = ""
    last: str = ""
    dob: str = ""
    bbref_id: str = ""
    bbrefminors_id: str = ""

    @property
    def birth_year(self) -> Optional[int]:
        if not self.dob:
            return None
        try:
            return int(self.dob[:4])
        except ValueError:
            return None

    @property
    def display_name(self) -> str:
        name = f"{self.first_raw} {self.last_raw}".strip()
        return re.sub(r"\s+", " ", name)


def build_bns_identity(row: Dict[str, str], idx: int) -> PlayerIdentity:
    first_raw, last_raw = split_bns_name(row)
    return PlayerIdentity(
        source_index=idx,
        row=row,
        player_id=(row.get("id") or "").strip(),
        first_raw=first_raw,
        last_raw=last_raw,
        first=normalize_name_part(first_raw),
        last=normalize_name_part(last_raw, remove_suffix=True),
        dob=bns_dob(row),
        bbref_id=normalize_id(row.get("bbref_id", "")),
        bbrefminors_id=normalize_id(row.get("bbrefminors_id", "")),
    )


def build_ootp_identity(row: Dict[str, str], idx: int, source_row_number: int) -> PlayerIdentity:
    first_raw = row.get("firstname", "")
    last_raw = row.get("lastname", "")
    return PlayerIdentity(
        source_index=idx,
        source_row_number=source_row_number,
        row=row,
        player_id=(row.get("id") or "").strip(),
        first_raw=first_raw,
        last_raw=last_raw,
        first=normalize_name_part(first_raw),
        last=normalize_name_part(last_raw, remove_suffix=True),
        dob=ootp_dob(row),
        bbref_id=normalize_id(row.get("bbref_id", "")),
        bbrefminors_id=normalize_id(row.get("bbrefminors_id", "")),
    )


@dataclass
class FeatureScore:
    first: bool = False
    last: bool = False
    dob: bool = False
    bbref_id: bool = False
    bbrefminors_id: bool = False

    @property
    def score(self) -> int:
        return sum([self.first, self.last, self.dob, self.bbref_id, self.bbrefminors_id])

    def as_dict(self) -> Dict[str, str]:
        return {
            "first_match": str(int(self.first)),
            "last_match": str(int(self.last)),
            "dob_match": str(int(self.dob)),
            "bbref_id_match": str(int(self.bbref_id)),
            "bbrefminors_id_match": str(int(self.bbrefminors_id)),
        }


def compare_features(bns: PlayerIdentity, ootp: PlayerIdentity) -> FeatureScore:
    return FeatureScore(
        first=bool(bns.first and ootp.first and bns.first == ootp.first),
        last=bool(bns.last and ootp.last and bns.last == ootp.last),
        dob=bool(bns.dob and ootp.dob and bns.dob == ootp.dob),
        bbref_id=bool(bns.bbref_id and ootp.bbref_id and bns.bbref_id == ootp.bbref_id),
        bbrefminors_id=bool(
            bns.bbrefminors_id and ootp.bbrefminors_id and bns.bbrefminors_id == ootp.bbrefminors_id
        ),
    )


@dataclass
class MatchResult:
    bns: PlayerIdentity
    ootp: Optional[PlayerIdentity] = None
    features: FeatureScore = field(default_factory=FeatureScore)
    success: bool = False
    match_mode: str = "hard"
    code: str = ""
    detail: str = ""
    target_team_id: str = ""
    service_days: str = ""
    options_used: str = ""

    @property
    def score(self) -> int:
        return self.features.score


def make_indexes(ootp_players: List[PlayerIdentity]) -> Dict[str, Dict[object, List[int]]]:
    indexes: Dict[str, Dict[object, List[int]]] = {
        "by_id": defaultdict(list),
        "by_bbref_id": defaultdict(list),
        "by_bbrefminors_id": defaultdict(list),
        "by_dob_last": defaultdict(list),
        "by_first_last": defaultdict(list),
        "by_last_year": defaultdict(list),
        "by_last": defaultdict(list),
    }
    for i, p in enumerate(ootp_players):
        if p.player_id:
            indexes["by_id"][p.player_id].append(i)
        if p.bbref_id:
            indexes["by_bbref_id"][p.bbref_id].append(i)
        if p.bbrefminors_id:
            indexes["by_bbrefminors_id"][p.bbrefminors_id].append(i)
        if p.dob and p.last:
            indexes["by_dob_last"][(p.dob, p.last)].append(i)
        if p.first and p.last:
            indexes["by_first_last"][(p.first, p.last)].append(i)
        if p.last and p.birth_year is not None:
            indexes["by_last_year"][(p.last, p.birth_year)].append(i)
        if p.last:
            indexes["by_last"][p.last].append(i)
    return indexes


def candidate_indices_for_bns(bns: PlayerIdentity, indexes: Dict[str, Dict[object, List[int]]]) -> List[int]:
    candidates = set()
    if bns.bbref_id:
        candidates.update(indexes["by_bbref_id"].get(bns.bbref_id, []))
    if bns.bbrefminors_id:
        candidates.update(indexes["by_bbrefminors_id"].get(bns.bbrefminors_id, []))
    if bns.dob and bns.last:
        candidates.update(indexes["by_dob_last"].get((bns.dob, bns.last), []))
    if bns.first and bns.last:
        candidates.update(indexes["by_first_last"].get((bns.first, bns.last), []))
    if bns.last and bns.birth_year is not None:
        candidates.update(indexes["by_last_year"].get((bns.last, bns.birth_year), []))
    # Fallback only if nothing else found. This can catch bad DOB/ID cases, but
    # the score threshold still prevents lastname-only matches from succeeding.
    if not candidates and bns.last:
        candidates.update(indexes["by_last"].get(bns.last, []))
    return sorted(candidates)


def same_normalized_full_name(bns: PlayerIdentity, ootp: PlayerIdentity) -> bool:
    return bool(bns.first and bns.last and bns.first == ootp.first and bns.last == ootp.last)


def same_source_id(bns: PlayerIdentity, ootp: PlayerIdentity) -> bool:
    return bool(bns.player_id and ootp.player_id and bns.player_id == ootp.player_id)


def hard_match_player(
    bns: PlayerIdentity,
    ootp_players: List[PlayerIdentity],
    indexes: Dict[str, Dict[object, List[int]]],
    threshold: int,
) -> MatchResult:
    candidate_idxs = candidate_indices_for_bns(bns, indexes)
    if not candidate_idxs:
        return MatchResult(bns=bns, success=False, code="NO_CANDIDATE", detail="No plausible OOTP candidate found")

    scored: List[Tuple[int, int, FeatureScore]] = []
    for idx in candidate_idxs:
        feat = compare_features(bns, ootp_players[idx])
        scored.append((feat.score, idx, feat))
    scored.sort(key=lambda x: x[0], reverse=True)
    best_score, best_idx, best_feat = scored[0]
    best_player = ootp_players[best_idx]

    def id_plus_name_rescue(pool: List[Tuple[int, FeatureScore]]) -> Optional[Tuple[int, FeatureScore]]:
        hits = [
            (idx, feat)
            for idx, feat in pool
            if same_source_id(bns, ootp_players[idx]) and same_normalized_full_name(bns, ootp_players[idx])
        ]
        if len(hits) == 1:
            return hits[0]
        return None

    if best_score < threshold:
        # Low-score rescue: if one potential match has exact normalized first+last
        # AND BNSL id == OOTP id, accept it despite the low feature score.
        rescued = id_plus_name_rescue([(idx, feat) for _, idx, feat in scored])
        if rescued is not None:
            idx, feat = rescued
            player = ootp_players[idx]
            return MatchResult(
                bns=bns,
                ootp=player,
                features=feat,
                success=True,
                code="MATCHED_LOW_SCORE_BY_ID_AND_NAME_TIEBREAK",
                detail=f"Low-score candidate accepted by BNSL id == OOTP id plus normalized first+last match; identity score {feat.score}/5 among {len(scored)} candidates",
            )

        top = "; ".join(
            f"{ootp_players[idx].player_id}:{ootp_players[idx].display_name}:score={score}"
            for score, idx, _ in scored[:5]
        )
        return MatchResult(
            bns=bns,
            ootp=best_player,
            features=best_feat,
            success=False,
            code="LOW_SCORE",
            detail=f"Best score {best_score}/{5}; top candidates: {top}",
        )

    tied_best = [(idx, feat) for score, idx, feat in scored if score == best_score]
    if len(tied_best) > 1:
        # Ambiguity rescue 1: if one of the tied candidates has exact normalized
        # first+last AND BNSL id == OOTP id, accept it.
        rescued = id_plus_name_rescue(tied_best)
        if rescued is not None:
            idx, feat = rescued
            player = ootp_players[idx]
            return MatchResult(
                bns=bns,
                ootp=player,
                features=feat,
                success=True,
                code="MATCHED_AMBIGUOUS_BY_ID_AND_NAME_TIEBREAK",
                detail=f"Ambiguous tie accepted by BNSL id == OOTP id plus normalized first+last match; identity score {feat.score}/5 among {len(tied_best)} tied candidates",
            )

        # Ambiguity rescue 2: otherwise, prefer the uniquely tied candidate whose
        # normalized first+last exactly matches the BNSL player.
        tied_name_matches = [(idx, feat) for idx, feat in tied_best if same_normalized_full_name(bns, ootp_players[idx])]
        if len(tied_name_matches) == 1:
            idx, feat = tied_name_matches[0]
            player = ootp_players[idx]
            return MatchResult(
                bns=bns,
                ootp=player,
                features=feat,
                success=True,
                code="MATCHED_BY_NAME_TIEBREAK",
                detail=f"Accepted unique normalized first+last match among {len(tied_best)} candidates tied at score {best_score}/5",
            )

        tied = "; ".join(f"{ootp_players[idx].player_id}:{ootp_players[idx].display_name}" for idx, _ in tied_best[:10])
        if len(tied_name_matches) > 1:
            return MatchResult(
                bns=bns,
                ootp=ootp_players[tied_name_matches[0][0]],
                features=tied_name_matches[0][1],
                success=False,
                code="AMBIGUOUS_NAME_TIEBREAK",
                detail=f"Multiple tied candidates also share normalized first+last at score {best_score}/{5}: {tied}",
            )
        return MatchResult(
            bns=bns,
            ootp=best_player,
            features=best_feat,
            success=False,
            code="AMBIGUOUS_MATCH",
            detail=f"Multiple OOTP candidates tied at score {best_score}/{5}: {tied}",
        )

    return MatchResult(
        bns=bns,
        ootp=best_player,
        features=best_feat,
        success=True,
        code="MATCHED",
        detail=f"Unique best score {best_score}/5 among {len(scored)} candidates",
    )

def load_id_map(path: Optional[Path]) -> Dict[str, str]:
    if path is None:
        return {}
    _, rows = read_dict_csv(path)
    mapping = {}
    for r in rows:
        # If this is one of this script's own map files, only trust rows that
        # were accepted matches. For a bare two-column map without match_success,
        # assume the user intentionally supplied every row.
        success_flag = (r.get("match_success") or "").strip().lower()
        if success_flag and success_flag not in {"1", "true", "yes", "y"}:
            continue
        bns_id = (r.get("bns_id") or r.get("id") or "").strip()
        ootp_id = (r.get("ootp_id") or "").strip()
        if bns_id and ootp_id:
            mapping[bns_id] = ootp_id
    return mapping


def id_map_match_player(
    bns: PlayerIdentity,
    ootp_players: List[PlayerIdentity],
    indexes: Dict[str, Dict[object, List[int]]],
    external_map: Dict[str, str],
    allow_bns_ootp_column: bool = True,
) -> MatchResult:
    target_id = external_map.get(bns.player_id)
    if not target_id and allow_bns_ootp_column:
        target_id = (bns.row.get("ootp_id") or "").strip()
    if not target_id:
        return MatchResult(bns=bns, success=False, match_mode="id_map", code="NO_ID_MAP", detail="No mapped OOTP id")
    hits = indexes["by_id"].get(target_id, [])
    if not hits:
        return MatchResult(
            bns=bns,
            success=False,
            match_mode="id_map",
            code="ID_NOT_FOUND",
            detail=f"Mapped OOTP id {target_id} not found in OOTP export",
        )
    if len(hits) > 1:
        return MatchResult(
            bns=bns,
            success=False,
            match_mode="id_map",
            code="DUPLICATE_OOTP_ID_IN_EXPORT",
            detail=f"Mapped OOTP id {target_id} appears {len(hits)} times in OOTP export",
        )
    ootp = ootp_players[hits[0]]
    features = compare_features(bns, ootp)
    return MatchResult(
        bns=bns,
        ootp=ootp,
        features=features,
        success=True,
        match_mode="id_map",
        code="MATCHED_BY_ID",
        detail=f"Matched by OOTP id {target_id}; identity score {features.score}/5",
    )


def parse_team_attrs(tag: str) -> Dict[str, str]:
    return dict(re.findall(r"([A-Za-z_][A-Za-z0-9_]*)=\"([^\"]*)\"", tag))


def parse_league_structure(path: Path) -> Dict[str, Dict[str, object]]:
    """Parse MLB franchise id + affiliate ids from league_structure.xml.

    This intentionally uses TEAM-tag regex instead of strict XML parsing because
    OOTP league_structure.xml exports or copied snippets can be missing final
    closing tags; self-contained TEAM tags are still enough for this task.
    """
    text = path.read_text(encoding="ISO-8859-1", errors="replace")
    teams: Dict[str, Dict[str, object]] = {}
    for tag in re.findall(r"<TEAM\b[^>]*>", text):
        attrs = parse_team_attrs(tag)
        if not attrs.get("abbr") or not attrs.get("id") or not attrs.get("affiliated_team_ids"):
            continue
        if attrs.get("parent_team_id", "0") not in ("0", ""):
            continue
        abbr = attrs["abbr"].strip().upper()
        affiliates = [x.strip() for x in attrs.get("affiliated_team_ids", "").split(",") if x.strip()]
        teams[abbr] = {
            "abbr": abbr,
            "team_id": attrs["id"].strip(),
            "name": attrs.get("name", "").strip(),
            "nick_name": attrs.get("nick_name", "").strip(),
            "affiliates": affiliates,
        }
    return teams


def canonical_franchise_abbr(value: str) -> str:
    value = (value or "").strip().upper()
    return FRANCHISE_ALIASES.get(value, value)


def service_years_to_days(value: str) -> Tuple[Optional[int], Optional[str]]:
    raw = (value or "").strip()
    if raw == "":
        return 0, "blank service_time; using 0"
    try:
        years = float(raw)
    except ValueError:
        return None, f"invalid service_time {raw!r}"
    if years < 0:
        return None, f"negative service_time {raw!r}"
    return int(round(years * SERVICE_DAYS_PER_YEAR)), None


def options_remaining_to_used(value: str) -> Tuple[Optional[int], Optional[str]]:
    raw = (value or "").strip()
    if raw == "":
        return 0, "blank options_remaining; using 0 options used"
    try:
        remaining = int(float(raw))
    except ValueError:
        return None, f"invalid options_remaining {raw!r}"
    used = 3 - remaining
    if used < 0 or used > 3:
        return max(0, min(3, used)), f"options_remaining {remaining} outside expected 0-3; clamped options_used to {max(0, min(3, used))}"
    return used, None


def target_team_for_bns(bns: PlayerIdentity, franchises: Dict[str, Dict[str, object]]) -> Tuple[Optional[str], Optional[str]]:
    row = bns.row
    franchise_raw = (row.get("franchise") or "").strip()
    roster_status = (row.get("roster_status") or "").strip().lower()
    active_roster = (row.get("active_roster") or "").strip()

    if not franchise_raw:
        return "0", None

    abbr = canonical_franchise_abbr(franchise_raw)
    if abbr not in franchises:
        return None, f"unknown franchise abbreviation {franchise_raw!r} (canonical {abbr!r})"

    info = franchises[abbr]
    affiliates = list(info.get("affiliates", []))

    if active_roster == "1" or roster_status == "active":
        return str(info["team_id"]), None

    if roster_status == "40-man":
        if len(affiliates) < 1:
            return None, f"franchise {abbr} has no AAA affiliate id in league_structure.xml"
        return affiliates[0], None

    if roster_status == "reserve":
        if bns.birth_year is None:
            return None, "reserve player has missing/unparseable birth year"
        if bns.birth_year <= 2002:
            if len(affiliates) < 2:
                return None, f"franchise {abbr} has no AA affiliate id in league_structure.xml"
            return affiliates[1], None
        if len(affiliates) < 3:
            return None, f"franchise {abbr} has no A+ affiliate id in league_structure.xml"
        return affiliates[2], None

    return None, f"franchised player has unknown roster_status {row.get('roster_status')!r}"


def locate_ootp_header(rows: List[List[str]]) -> Tuple[int, Dict[str, int], List[str]]:
    for i, row in enumerate(rows):
        cleaned = [clean_header_name(x) for x in row]
        if "id" in cleaned and "team_id" in cleaned and "lastname" in cleaned and "firstname" in cleaned:
            colmap = {name: idx for idx, name in enumerate(cleaned) if name}
            return i, colmap, row
    raise RuntimeError("Could not locate OOTP header row with id/team_id/LastName/FirstName")


def row_to_ootp_dict(row: List[str], colmap: Dict[str, int], width: int) -> Dict[str, str]:
    if len(row) < width:
        row = row + [""] * (width - len(row))
    return {name: row[idx] if idx < len(row) else "" for name, idx in colmap.items()}


def set_row_value(row: List[str], colmap: Dict[str, int], key: str, value: object, width: int) -> None:
    idx = colmap[key]
    while len(row) < width:
        row.append("")
    row[idx] = str(value)


def find_player_rows(ootp_rows: List[List[str]], header_idx: int, width: int) -> List[Tuple[int, List[str]]]:
    player_rows = []
    for row_number in range(header_idx + 1, len(ootp_rows)):
        row = ootp_rows[row_number]
        if not row:
            continue
        first = (row[0] if row else "").strip()
        if not first or first.startswith("//"):
            continue
        # OOTP player rows have an integer id in first col.
        if parse_int(first) is None:
            continue
        if len(row) < width:
            row.extend([""] * (width - len(row)))
        player_rows.append((row_number, row))
    return player_rows


def write_audit(path: Path, audit_rows: List[Dict[str, str]]) -> None:
    fields = [
        "severity", "code", "bns_id", "bns_name", "ootp_id", "ootp_name", "match_mode",
        "match_score", "franchise", "roster_status", "target_team_id", "details",
    ]
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        for row in audit_rows:
            writer.writerow({k: row.get(k, "") for k in fields})


def write_map(path: Path, matches: List[MatchResult]) -> None:
    fields = [
        "bns_id", "ootp_id", "match_mode", "match_success", "match_code", "match_score",
        "bns_name", "ootp_name", "bns_dob", "ootp_dob", "bns_first", "ootp_first",
        "bns_last", "ootp_last", "bns_bbref_id", "ootp_bbref_id", "bns_bbrefminors_id",
        "ootp_bbrefminors_id", "first_match", "last_match", "dob_match", "bbref_id_match",
        "bbrefminors_id_match", "bns_franchise", "bns_roster_status", "target_team_id",
        "service_days", "options_used", "details",
    ]
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        for m in matches:
            o = m.ootp
            row = {
                "bns_id": m.bns.player_id,
                "ootp_id": o.player_id if o else "",
                "match_mode": m.match_mode,
                "match_success": str(int(m.success)),
                "match_code": m.code,
                "match_score": str(m.score),
                "bns_name": m.bns.display_name or (m.bns.row.get("name") or ""),
                "ootp_name": o.display_name if o else "",
                "bns_dob": m.bns.dob,
                "ootp_dob": o.dob if o else "",
                "bns_first": m.bns.first_raw,
                "ootp_first": o.first_raw if o else "",
                "bns_last": m.bns.last_raw,
                "ootp_last": o.last_raw if o else "",
                "bns_bbref_id": m.bns.bbref_id,
                "ootp_bbref_id": o.bbref_id if o else "",
                "bns_bbrefminors_id": m.bns.bbrefminors_id,
                "ootp_bbrefminors_id": o.bbrefminors_id if o else "",
                "bns_franchise": m.bns.row.get("franchise", ""),
                "bns_roster_status": m.bns.row.get("roster_status", ""),
                "target_team_id": m.target_team_id,
                "service_days": m.service_days,
                "options_used": m.options_used,
                "details": m.detail,
            }
            row.update(m.features.as_dict())
            writer.writerow(row)


def audit_row(m: MatchResult, severity: str, code: Optional[str] = None, detail: Optional[str] = None) -> Dict[str, str]:
    o = m.ootp
    return {
        "severity": severity,
        "code": code or m.code,
        "bns_id": m.bns.player_id,
        "bns_name": m.bns.display_name or (m.bns.row.get("name") or ""),
        "ootp_id": o.player_id if o else "",
        "ootp_name": o.display_name if o else "",
        "match_mode": m.match_mode,
        "match_score": str(m.score),
        "franchise": m.bns.row.get("franchise", ""),
        "roster_status": m.bns.row.get("roster_status", ""),
        "target_team_id": m.target_team_id,
        "details": detail or m.detail,
    }


def audit_unmatched_ootp_clear_row(ootp: PlayerIdentity, original_team_id: str) -> Dict[str, str]:
    return {
        "severity": "WARN",
        "code": "OOTP_UNMATCHED_FRANCHISE_PLAYER_CLEARED",
        "bns_id": "",
        "bns_name": "",
        "ootp_id": ootp.player_id,
        "ootp_name": ootp.display_name,
        "match_mode": "ootp_unmatched_cleanup",
        "match_score": "",
        "franchise": "",
        "roster_status": "",
        "target_team_id": "0",
        "details": f"OOTP player had team_id={original_team_id} but was not successfully matched to any BNSL player; cleared to team_id=0",
    }


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Create an OOTP roster import from a BNSL roster export.")
    parser.add_argument("--ootp-export", default="mlb_rosters.csv", type=Path)
    parser.add_argument("--bnsl-export", default="roster_players.csv", type=Path)
    parser.add_argument("--league-structure", default="league_structure.xml", type=Path)
    parser.add_argument("--output", default="ootp_player_import_updated.csv", type=Path)
    parser.add_argument("--id-map-output", default="bns_ootp_id_map.csv", type=Path)
    parser.add_argument("--audit-output", default="bns_ootp_audit_report.csv", type=Path)
    parser.add_argument("--input-id-map", default=None, type=Path)
    parser.add_argument("--use-id-map", action="store_true", default=USE_ID_MAP)
    parser.add_argument("--match-threshold", default=MATCH_THRESHOLD, type=int)
    parser.add_argument("--dry-run", action="store_true", help="write map/audit only; do not write updated OOTP import")
    parser.add_argument("--no-clear-unmatched-franchised", dest="clear_unmatched_franchised", action="store_false", default=True, help="do not clear OOTP players with team_id>0 who were not matched to BNSL")
    args = parser.parse_args(argv)

    ootp_rows = read_csv_rows(args.ootp_export)
    header_idx, colmap, header = locate_ootp_header(ootp_rows)
    width = len(header)
    required_cols = [
        "id", "team_id", "lastname", "firstname", "dayob", "monthob", "yearob",
        "ml service", "40 man roster service", "options used", "bbref_id", "bbrefminors_id",
    ]
    missing = [c for c in required_cols if c not in colmap]
    if missing:
        raise RuntimeError(f"OOTP export missing required columns: {missing}")

    _, bns_rows = read_dict_csv(args.bnsl_export)
    for col in ["id", "name", "date_of_birth", "franchise", "roster_status", "service_time", "options_remaining"]:
        if bns_rows and col not in bns_rows[0]:
            raise RuntimeError(f"BNSL export missing required column: {col}")

    franchises = parse_league_structure(args.league_structure)
    if len(franchises) < 30:
        print(
            f"WARNING: only parsed {len(franchises)} MLB franchise TEAM entries from {args.league_structure}; "
            "check league_structure.xml if team assignment audits look wrong.",
            file=sys.stderr,
        )

    player_row_refs = find_player_rows(ootp_rows, header_idx, width)
    ootp_players: List[PlayerIdentity] = []
    ootp_row_by_player_index: Dict[int, int] = {}
    for idx, (row_number, row) in enumerate(player_row_refs):
        d = row_to_ootp_dict(row, colmap, width)
        p = build_ootp_identity(d, idx, row_number)
        ootp_players.append(p)
        ootp_row_by_player_index[idx] = row_number

    bns_players = [build_bns_identity(row, idx) for idx, row in enumerate(bns_rows)]
    indexes = make_indexes(ootp_players)
    external_id_map = load_id_map(args.input_id_map)
    allow_bns_ootp_column = args.input_id_map is None

    matches: List[MatchResult] = []
    audit: List[Dict[str, str]] = []

    for bns in bns_players:
        if args.use_id_map:
            m = id_map_match_player(bns, ootp_players, indexes, external_id_map, allow_bns_ootp_column)
        else:
            m = hard_match_player(bns, ootp_players, indexes, args.match_threshold)

        team_id, team_problem = target_team_for_bns(bns, franchises)
        service_days, service_problem = service_years_to_days(bns.row.get("service_time", ""))
        options_used, options_problem = options_remaining_to_used(bns.row.get("options_remaining", ""))

        if team_id is not None:
            m.target_team_id = str(team_id)
        if service_days is not None:
            m.service_days = str(service_days)
        if options_used is not None:
            m.options_used = str(options_used)

        problems = []
        if team_problem:
            problems.append(team_problem)
        if service_problem:
            problems.append(service_problem)
        if options_problem:
            problems.append(options_problem)

        if not m.success:
            audit.append(audit_row(m, "ERROR" if m.code not in {"LOW_SCORE", "NO_CANDIDATE"} else "WARN"))
        if problems:
            if team_problem or service_days is None or options_used is None:
                m.success = False
            audit.append(audit_row(m, "ERROR" if not m.success else "WARN", code="INPUT_OR_ASSIGNMENT_ISSUE", detail="; ".join(problems)))

        matches.append(m)

    # Prevent two BNSL players from updating the same OOTP row.
    successful_ootp_ids = [m.ootp.player_id for m in matches if m.success and m.ootp]
    dup_ids = {pid for pid, count in Counter(successful_ootp_ids).items() if count > 1}
    if dup_ids:
        for m in matches:
            if m.success and m.ootp and m.ootp.player_id in dup_ids:
                m.success = False
                m.code = "DUPLICATE_OOTP_TARGET"
                m.detail = f"OOTP id {m.ootp.player_id} was matched by multiple BNSL players; no update applied"
                audit.append(audit_row(m, "ERROR"))

    # Apply successful BNSL-driven updates.
    updated_count = 0
    matched_ootp_ids = {m.ootp.player_id for m in matches if m.success and m.ootp and m.ootp.player_id}
    for m in matches:
        if not (m.success and m.ootp):
            continue
        row_number = m.ootp.source_row_number
        row = ootp_rows[row_number]
        set_row_value(row, colmap, "team_id", m.target_team_id, width)
        set_row_value(row, colmap, "ml service", m.service_days, width)
        set_row_value(row, colmap, "40 man roster service", m.service_days, width)
        set_row_value(row, colmap, "options used", m.options_used, width)
        updated_count += 1

    # Cleanup step: remove OOTP players from organizations if BNSL did not claim them.
    # This happens after successful updates, so BNSL matches remain assigned as requested,
    # while every still-franchised non-match is moved to team_id=0.
    cleared_unmatched_count = 0
    if args.clear_unmatched_franchised:
        for p in ootp_players:
            if p.player_id in matched_ootp_ids:
                continue
            row_number = p.source_row_number
            row = ootp_rows[row_number]
            current_team_id = row_to_ootp_dict(row, colmap, width).get("team_id", "").strip()
            team_id_int = parse_int(current_team_id, default=0)
            if team_id_int is not None and team_id_int > 0:
                set_row_value(row, colmap, "team_id", "0", width)
                audit.append(audit_unmatched_ootp_clear_row(p, current_team_id))
                cleared_unmatched_count += 1

    write_map(args.id_map_output, matches)
    write_audit(args.audit_output, audit)
    if not args.dry_run:
        write_csv_rows(args.output, ootp_rows)

    print(f"OOTP player rows read: {len(ootp_players)}")
    print(f"BNSL player rows read: {len(bns_players)}")
    print(f"Parsed MLB franchises from XML: {len(franchises)}")
    print(f"Successful updates applied: {updated_count}")
    print(f"Unmatched franchised OOTP players cleared: {cleared_unmatched_count}")
    print(f"Audit rows written: {len(audit)} -> {args.audit_output}")
    print(f"ID map written: {args.id_map_output}")
    if not args.dry_run:
        print(f"Updated OOTP import written: {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
