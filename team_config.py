from __future__ import annotations

# Centralized BNSL team metadata.
#
# Use TEAM_EMAILS for pages keyed by full MLB team name.
# Use TEAM_EMAILS_BY_ABBR for pages keyed by BNSL abbreviations.
# canonical_team_abbr() normalizes common aliases before abbreviation lookup.

MLB_TEAMS = [
    "Arizona Diamondbacks", "Atlanta Braves", "Baltimore Orioles", "Boston Red Sox",
    "Chicago Cubs", "Chicago White Sox", "Cincinnati Reds", "Cleveland Guardians",
    "Colorado Rockies", "Detroit Tigers", "Houston Astros", "Kansas City Royals",
    "Los Angeles Angels", "Los Angeles Dodgers", "Miami Marlins", "Milwaukee Brewers",
    "Minnesota Twins", "New York Mets", "New York Yankees", "Oakland Athletics",
    "Philadelphia Phillies", "Pittsburgh Pirates", "San Diego Padres", "San Francisco Giants",
    "Seattle Mariners", "St. Louis Cardinals", "Tampa Bay Rays", "Texas Rangers",
    "Toronto Blue Jays", "Washington Nationals",
]

TEAM_EMAILS = {
    "Toronto Blue Jays": "daniele.defeo@gmail.com",
    "New York Yankees": "dmsund66@gmail.com",
    "Boston Red Sox": "chris_lawrence@sbcglobal.net",
    "Tampa Bay Rays": "smith.mark.louis@gmail.com",
    "Baltimore Orioles": "bsweis@ptd.net",

    "Detroit Tigers": "brianorr@live.com",
    "Kansas City Royals": "jim@timhafer.com",
    "Minnesota Twins": "jonathan.adelman@gmail.com",
    "Chicago White Sox": "bglover6@gmail.com",
    "Cleveland Guardians": "bonfanti20@gmail.com",

    "Los Angeles Angels": "dsucoff@gmail.com",
    "Seattle Mariners": "daniel_a_fisher@yahoo.com",
    "Oakland Athletics": "bspropp@hotmail.com",
    "Houston Astros": "golk624@protonmail.com",
    "Texas Rangers": "Brianorr@live.com",

    "Washington Nationals": "smsetnor@gmail.com",
    "New York Mets": "kerkhoffc@gmail.com",
    "Philadelphia Phillies": "jdcarney26@gmail.com",
    "Atlanta Braves": "stevegaston@yahoo.com",
    "Miami Marlins": "schmitz@ucsb.edu",

    "St. Louis Cardinals": "parkbench@mac.com",
    "Chicago Cubs": "bryanhartman@gmail.com",
    "Pittsburgh Pirates": "jseiner24@gmail.com",
    "Milwaukee Brewers": "tsurratt@hiaspire.com",
    "Cincinnati Reds": "jpmile@yahoo.com",

    "Los Angeles Dodgers": "jr92@comcast.net",
    "Colorado Rockies": "GypsySon@gmail.com",
    "Arizona Diamondbacks": "mhr4240@gmail.com",
    "San Francisco Giants": "jasonmallet@gmail.com",
    "San Diego Padres": "mattaca77@gmail.com",
}

TEAM_NAME_TO_ABBR = {
    "Arizona Diamondbacks": "ARI",
    "Atlanta Braves": "ATL",
    "Baltimore Orioles": "BAL",
    "Boston Red Sox": "BOS",
    "Chicago Cubs": "CHC",
    "Chicago White Sox": "CHW",
    "Cincinnati Reds": "CIN",
    "Cleveland Guardians": "CLE",
    "Colorado Rockies": "COL",
    "Detroit Tigers": "DET",
    "Houston Astros": "HOU",
    "Kansas City Royals": "KC",
    "Los Angeles Angels": "LAA",
    "Los Angeles Dodgers": "LAD",
    "Miami Marlins": "MIA",
    "Milwaukee Brewers": "MIL",
    "Minnesota Twins": "MIN",
    "New York Mets": "NYM",
    "New York Yankees": "NYY",
    "Oakland Athletics": "OAK",
    "Philadelphia Phillies": "PHI",
    "Pittsburgh Pirates": "PIT",
    "San Diego Padres": "SD",
    "San Francisco Giants": "SF",
    "Seattle Mariners": "SEA",
    "St. Louis Cardinals": "STL",
    "Tampa Bay Rays": "TB",
    "Texas Rangers": "TEX",
    "Toronto Blue Jays": "TOR",
    "Washington Nationals": "WAS",
}

_ABBR_ALIASES = {
    "CWS": "CHW",
    "KCR": "KC",
    "SDP": "SD",
    "SFG": "SF",
    "TBR": "TB",
    "WSH": "WAS",
    "WSN": "WAS",
}

ABBR_TO_TEAM = {abbr: team for team, abbr in TEAM_NAME_TO_ABBR.items()}
ABBR_TO_TEAM.update({alias: ABBR_TO_TEAM[canonical] for alias, canonical in _ABBR_ALIASES.items()})

TEAM_EMAILS_BY_ABBR = {
    TEAM_NAME_TO_ABBR[team]: email
    for team, email in TEAM_EMAILS.items()
}

TEAM_ABBRS = sorted(TEAM_EMAILS_BY_ABBR.keys())


def canonical_team_abbr(team: str | None) -> str:
    code = (team or "").strip().upper()
    return _ABBR_ALIASES.get(code, code)


def email_for_team(team: str | None) -> str | None:
    """Accept either full team name or an abbreviation/alias."""
    if not team:
        return None
    raw = team.strip()
    if raw in TEAM_EMAILS:
        return TEAM_EMAILS[raw]
    return TEAM_EMAILS_BY_ABBR.get(canonical_team_abbr(raw))


def team_name_for_abbr(team: str | None) -> str:
    return ABBR_TO_TEAM.get(canonical_team_abbr(team), (team or "").strip())


def team_abbr_for_name(team: str | None) -> str:
    return TEAM_NAME_TO_ABBR.get((team or "").strip(), canonical_team_abbr(team))


def emails_equal(a: str | None, b: str | None) -> bool:
    if not a or not b:
        return False
    return a.strip().lower() == b.strip().lower()
