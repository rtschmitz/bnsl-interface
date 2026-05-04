from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime
from pathlib import Path
from typing import Any, Optional
import re
import sqlite3

from flask import Blueprint, current_app, jsonify, render_template_string, request, session

try:
    from ui_skin import BNSL_GAME_CSS
except Exception:  # lets the parser/database builder run outside the full app too
    BNSL_GAME_CSS = """
    <style>
      :root{--warn:#FF4D6D;--good:#36F9A2;--muted:rgba(234,240,255,.68);}
      body{font-family:Inter,system-ui,sans-serif;background:#070A12;color:#EAF0FF;margin:0;}
      .page{max-width:1400px;margin:0 auto;padding:18px;}.panel{background:#10182d;border:1px solid rgba(140,170,255,.18);border-radius:18px;padding:14px;}
      table{width:100%;border-collapse:collapse;}th,td{padding:10px;border-bottom:1px solid rgba(255,255,255,.08);vertical-align:top;}.muted{opacity:.72}.btn,.pill{border:1px solid rgba(140,170,255,.22);border-radius:999px;padding:8px 10px;color:inherit;background:rgba(255,255,255,.06);text-decoration:none;}
      input,select,textarea{background:#0b1224;color:#EAF0FF;border:1px solid rgba(140,170,255,.25);border-radius:10px;padding:8px;}
    </style>
    """

try:
    from draft_app import TEAM_EMAILS, emails_equal
except Exception:
    TEAM_EMAILS = {}
    def emails_equal(a: str | None, b: str | None) -> bool:
        return bool(a and b and a.strip().lower() == b.strip().lower())

trades_bp = Blueprint("trades", __name__)

# BNSL trade-log abbreviations. This intentionally uses CHW because that is what
# the uploaded trade log uses for the White Sox.
ABBR_TO_FULL: dict[str, str] = {
    "ARI": "Arizona Diamondbacks",
    "ATL": "Atlanta Braves",
    "BAL": "Baltimore Orioles",
    "BOS": "Boston Red Sox",
    "CHC": "Chicago Cubs",
    "CHW": "Chicago White Sox",
    "CIN": "Cincinnati Reds",
    "CLE": "Cleveland Guardians",
    "COL": "Colorado Rockies",
    "DET": "Detroit Tigers",
    "HOU": "Houston Astros",
    "KC": "Kansas City Royals",
    "LAA": "Los Angeles Angels",
    "LAD": "Los Angeles Dodgers",
    "MIA": "Miami Marlins",
    "MIL": "Milwaukee Brewers",
    "MIN": "Minnesota Twins",
    "NYM": "New York Mets",
    "NYY": "New York Yankees",
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
}
TEAM_ORDER = list(ABBR_TO_FULL.keys())
FULL_TO_ABBR = {v: k for k, v in ABBR_TO_FULL.items()}

HEADER_RE = re.compile(r"^(\d{4}-\d{2}-\d{2}):\s*(.+?)\s*$")
LEG_RE = re.compile(r"^From\s+([A-Z]{2,3})\s+to\s+([A-Z]{2,3}):\s*(.*?)\s*$")
PICK_RE = re.compile(r"\b(20\d{2})\s+(\d{1,2})\.([A-Z]{2,3})\b")

FUTURE_STOCK_START_YEAR = 2026
FUTURE_STOCK_MIN_END_YEAR = 2028
FUTURE_STOCK_ROUNDS = 12
MIN_PICKS_PER_YEAR = 9
MAX_PICKS_PER_YEAR = 15
SALARY_CONTRACT_TYPES = {"A", "X", "FA"}


@dataclass
class TradeLeg:
    from_team: str
    to_team: str
    asset_text: str
    line_no: int


@dataclass
class TradeEvent:
    log_index: int
    trade_date: str
    title: str
    raw_text: str
    legs: list[TradeLeg] = field(default_factory=list)


def _safe_config(name: str) -> Optional[str]:
    try:
        return current_app.config.get(name)
    except RuntimeError:
        return None


def _app_dir() -> Path:
    return Path(__file__).resolve().parent


def get_trades_log_path() -> Path:
    return Path(_safe_config("TRADES_LOG_PATH") or (_app_dir() / "trades.txt"))


def get_draft_stock_db_path() -> Path:
    return Path(_safe_config("DRAFT_STOCK_DB_PATH") or (_app_dir() / "draft_stock.db"))


def get_roster_db_path() -> Path:
    return Path(_safe_config("ROSTER_DB_PATH") or (_app_dir() / "roster.db"))


def connect_stock_db(db_path: Optional[Path] = None) -> sqlite3.Connection:
    path = Path(db_path or get_draft_stock_db_path())
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    return conn


def connect_roster_db() -> sqlite3.Connection:
    conn = sqlite3.connect(get_roster_db_path())
    conn.row_factory = sqlite3.Row
    return conn


def display_team(abbr: str | None) -> str:
    if not abbr:
        return ""
    return ABBR_TO_FULL.get(abbr, abbr)


def team_label(abbr: str | None) -> str:
    if not abbr:
        return ""
    full = display_team(abbr)
    return f"{abbr} — {full}" if full != abbr else abbr


def team_email_lookup() -> dict[str, str]:
    """Return expected manager emails keyed by BNSL abbreviation."""
    lookup: dict[str, str] = {}
    for full, email in TEAM_EMAILS.items():
        abbr = FULL_TO_ABBR.get(full)
        if abbr and email:
            lookup[abbr] = email
    return lookup


def authed_team() -> str:
    t = (session.get("authed_team") or "").strip().upper()
    return t if t in ABBR_TO_FULL else ""


def _now_iso() -> str:
    return datetime.utcnow().isoformat(timespec="seconds")


def _today_iso() -> str:
    return date.today().isoformat()


def _parse_date(s: str) -> date:
    try:
        return datetime.strptime(s, "%Y-%m-%d").date()
    except Exception:
        return date.today()


def money(value: Any) -> str:
    try:
        return f"${float(value):,.0f}"
    except Exception:
        return "$0"


def prorated_salary_payment(full_salary: float, trade_date: str) -> float:
    """
    For A/X/FA players traded with no retained salary, payer owes the receiving
    team the inclusive share from Mar 26 through the trade date divided by 186.
    Outside Mar 26..Sep 27, no automatic payment is attached.
    """
    d = _parse_date(trade_date)
    start = date(d.year, 3, 26)
    end = date(d.year, 9, 27)
    if d < start or d > end:
        return 0.0
    days = max(0, min(186, (d - start).days + 1))
    return round(float(full_salary or 0.0) * (days / 186.0), 2)


def parse_trade_log(text: str) -> list[TradeEvent]:
    """Parse the BNSL trade log into dated trade blocks and From/To legs."""
    blocks: list[list[tuple[int, str]]] = []
    current: list[tuple[int, str]] = []

    for line_no, line in enumerate(text.splitlines(), start=1):
        if HEADER_RE.match(line):
            if current:
                blocks.append(current)
            current = [(line_no, line)]
        elif current:
            current.append((line_no, line))

    if current:
        blocks.append(current)

    events: list[TradeEvent] = []
    for log_index, lines in enumerate(blocks):
        first_line = lines[0][1]
        m = HEADER_RE.match(first_line)
        if not m:
            continue
        event = TradeEvent(
            log_index=log_index,
            trade_date=m.group(1),
            title=m.group(2).strip(),
            raw_text="\n".join(line for _, line in lines).strip(),
        )
        for line_no, line in lines[1:]:
            lm = LEG_RE.match(line)
            if not lm:
                continue
            event.legs.append(
                TradeLeg(
                    from_team=lm.group(1).strip(),
                    to_team=lm.group(2).strip(),
                    asset_text=lm.group(3).strip(),
                    line_no=line_no,
                )
            )
        events.append(event)

    return events


def _has_column(conn: sqlite3.Connection, table: str, column: str) -> bool:
    cur = conn.cursor()
    cur.execute(f"PRAGMA table_info({table})")
    return column in {row[1] for row in cur.fetchall()}


def _ensure_column(conn: sqlite3.Connection, table: str, column: str, definition: str) -> None:
    if not _has_column(conn, table, column):
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}")


def init_proposal_schema(conn: sqlite3.Connection) -> None:
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS trade_proposals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            created_at TEXT NOT NULL,
            updated_at TEXT,
            accepted_at TEXT,
            acted_by_team_abbr TEXT,
            status TEXT NOT NULL DEFAULT 'pending',
            proposer_team_abbr TEXT NOT NULL,
            target_team_abbr TEXT NOT NULL,
            trade_date TEXT NOT NULL,
            notes TEXT
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS trade_proposal_items (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            proposal_id INTEGER NOT NULL,
            side_team_abbr TEXT NOT NULL,
            receiving_team_abbr TEXT NOT NULL,
            asset_type TEXT NOT NULL,
            player_key TEXT,
            player_name TEXT,
            position TEXT,
            roster_status TEXT,
            contract_type TEXT,
            salary REAL NOT NULL DEFAULT 0,
            salary_retained REAL NOT NULL DEFAULT 0,
            prorated_payment REAL NOT NULL DEFAULT 0,
            cash_amount REAL NOT NULL DEFAULT 0,
            pick_year INTEGER,
            pick_round INTEGER,
            original_team_abbr TEXT,
            pick_label TEXT,
            display_text TEXT NOT NULL,
            FOREIGN KEY(proposal_id) REFERENCES trade_proposals(id)
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS trade_proposal_payments (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            proposal_id INTEGER NOT NULL,
            payer_team_abbr TEXT NOT NULL,
            receiver_team_abbr TEXT NOT NULL,
            amount REAL NOT NULL,
            reason TEXT NOT NULL,
            player_key TEXT,
            player_name TEXT,
            status TEXT NOT NULL DEFAULT 'pending',
            FOREIGN KEY(proposal_id) REFERENCES trade_proposals(id)
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS finance_payments (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source_type TEXT NOT NULL,
            source_id INTEGER,
            created_at TEXT NOT NULL,
            effective_date TEXT NOT NULL,
            payer_team_abbr TEXT NOT NULL,
            receiver_team_abbr TEXT NOT NULL,
            amount REAL NOT NULL,
            description TEXT,
            status TEXT NOT NULL DEFAULT 'posted',
            UNIQUE(source_type, source_id, payer_team_abbr, receiver_team_abbr, amount, description) ON CONFLICT IGNORE
        )
    """)

    for table, adds in {
        "trade_proposals": [
            ("updated_at", "TEXT"),
            ("accepted_at", "TEXT"),
            ("acted_by_team_abbr", "TEXT"),
            ("notes", "TEXT"),
        ],
        "trade_proposal_items": [
            ("salary_retained", "REAL NOT NULL DEFAULT 0"),
            ("prorated_payment", "REAL NOT NULL DEFAULT 0"),
            ("cash_amount", "REAL NOT NULL DEFAULT 0"),
            ("receiving_team_abbr", "TEXT NOT NULL DEFAULT ''"),
        ],
        "trade_proposal_payments": [
            ("status", "TEXT NOT NULL DEFAULT 'pending'"),
        ],
        "finance_payments": [
            ("status", "TEXT NOT NULL DEFAULT 'posted'"),
            ("description", "TEXT"),
        ],
    }.items():
        for col, typ in adds:
            _ensure_column(conn, table, col, typ)

    cur.execute("CREATE INDEX IF NOT EXISTS idx_trade_proposals_proposer ON trade_proposals(proposer_team_abbr, status)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_trade_proposals_target ON trade_proposals(target_team_abbr, status)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_trade_proposal_items_proposal ON trade_proposal_items(proposal_id)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_finance_payments_teams ON finance_payments(payer_team_abbr, receiver_team_abbr, status)")
    conn.commit()


def init_stock_schema(conn: sqlite3.Connection) -> None:
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS trade_events (
            id INTEGER PRIMARY KEY,
            log_index INTEGER NOT NULL UNIQUE,
            trade_date TEXT NOT NULL,
            title TEXT NOT NULL,
            raw_text TEXT NOT NULL
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS trade_legs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            trade_event_id INTEGER NOT NULL,
            line_no INTEGER NOT NULL,
            from_team_abbr TEXT NOT NULL,
            to_team_abbr TEXT NOT NULL,
            asset_text TEXT NOT NULL,
            FOREIGN KEY(trade_event_id) REFERENCES trade_events(id)
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS draft_pick_movements (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            trade_event_id INTEGER NOT NULL,
            process_order INTEGER NOT NULL,
            trade_date TEXT NOT NULL,
            title TEXT NOT NULL,
            from_team_abbr TEXT NOT NULL,
            to_team_abbr TEXT NOT NULL,
            pick_year INTEGER NOT NULL,
            pick_round INTEGER NOT NULL,
            original_team_abbr TEXT NOT NULL,
            previous_owner_abbr TEXT NOT NULL,
            new_owner_abbr TEXT NOT NULL,
            owner_chain_ok INTEGER NOT NULL DEFAULT 1,
            pick_label TEXT NOT NULL,
            FOREIGN KEY(trade_event_id) REFERENCES trade_events(id)
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS draft_pick_stock (
            pick_year INTEGER NOT NULL,
            pick_round INTEGER NOT NULL,
            original_team_abbr TEXT NOT NULL,
            current_owner_abbr TEXT NOT NULL,
            last_acquired_from_abbr TEXT,
            last_trade_event_id INTEGER,
            last_trade_date TEXT,
            last_trade_title TEXT,
            PRIMARY KEY(pick_year, pick_round, original_team_abbr),
            FOREIGN KEY(last_trade_event_id) REFERENCES trade_events(id)
        )
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_trade_events_log_index ON trade_events(log_index)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_trade_events_date ON trade_events(trade_date)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_pick_stock_owner ON draft_pick_stock(current_owner_abbr, pick_year, pick_round)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_pick_movements_pick ON draft_pick_movements(pick_year, pick_round, original_team_abbr)")
    conn.commit()
    init_proposal_schema(conn)


def _apply_accepted_proposal_picks(cur: sqlite3.Cursor, *, start_process_order: int) -> tuple[int, list[str]]:
    """Apply accepted proposal draft-pick transfers after rebuilding stock from trades.txt."""
    warnings: list[str] = []
    process_order = start_process_order
    cur.execute("""
        SELECT p.id, p.trade_date, p.proposer_team_abbr, p.target_team_abbr,
               i.side_team_abbr, i.receiving_team_abbr, i.pick_year, i.pick_round,
               i.original_team_abbr, i.pick_label
        FROM trade_proposals p
        JOIN trade_proposal_items i ON i.proposal_id = p.id
        WHERE p.status = 'accepted' AND i.asset_type = 'pick'
        ORDER BY COALESCE(p.accepted_at, p.updated_at, p.created_at), p.id, i.id
    """)
    for row in cur.fetchall():
        cur.execute(
            """
            SELECT current_owner_abbr
            FROM draft_pick_stock
            WHERE pick_year=? AND pick_round=? AND original_team_abbr=?
            """,
            (row["pick_year"], row["pick_round"], row["original_team_abbr"]),
        )
        stock = cur.fetchone()
        previous_owner = stock["current_owner_abbr"] if stock else row["side_team_abbr"]
        if not stock:
            warnings.append(f"Accepted proposal #{row['id']} references missing pick {row['pick_label']}.")
            continue
        if previous_owner != row["side_team_abbr"]:
            warnings.append(
                f"Accepted proposal #{row['id']} pick mismatch for {row['pick_label']}: "
                f"expected {row['side_team_abbr']}, reconstructed {previous_owner}. Applied accepted proposal anyway."
            )
        process_order += 1
        cur.execute(
            """
            INSERT INTO draft_pick_movements(
                trade_event_id, process_order, trade_date, title,
                from_team_abbr, to_team_abbr,
                pick_year, pick_round, original_team_abbr,
                previous_owner_abbr, new_owner_abbr, owner_chain_ok, pick_label
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                -int(row["id"]),
                process_order,
                row["trade_date"],
                f"Accepted Trade Proposal #{row['id']}",
                row["side_team_abbr"],
                row["receiving_team_abbr"],
                row["pick_year"],
                row["pick_round"],
                row["original_team_abbr"],
                previous_owner,
                row["receiving_team_abbr"],
                1 if previous_owner == row["side_team_abbr"] else 0,
                row["pick_label"],
            ),
        )
        cur.execute(
            """
            UPDATE draft_pick_stock
            SET current_owner_abbr=?,
                last_acquired_from_abbr=?,
                last_trade_event_id=?,
                last_trade_date=?,
                last_trade_title=?
            WHERE pick_year=? AND pick_round=? AND original_team_abbr=?
            """,
            (
                row["receiving_team_abbr"],
                row["side_team_abbr"],
                -int(row["id"]),
                row["trade_date"],
                f"Accepted Trade Proposal #{row['id']}",
                row["pick_year"],
                row["pick_round"],
                row["original_team_abbr"],
            ),
        )
    return process_order, warnings


def refresh_from_log(
    log_path: Optional[Path] = None,
    db_path: Optional[Path] = None,
    *,
    start_year: int = FUTURE_STOCK_START_YEAR,
) -> dict[str, Any]:
    """
    Rebuild trade log + future draft-pick stock from trades.txt.

    The log is newest-first, including newest-first ordering within the same date.
    To reconstruct ownership, movements are applied from the bottom of the file
    to the top. Accepted trade-proposal pick movements are then layered on top.
    """
    log_path = Path(log_path or get_trades_log_path())
    conn = connect_stock_db(db_path)
    init_stock_schema(conn)
    cur = conn.cursor()

    for table in ("draft_pick_movements", "draft_pick_stock", "trade_legs", "trade_events"):
        cur.execute(f"DELETE FROM {table}")

    if not log_path.exists():
        conn.commit()
        conn.close()
        return {"events": 0, "future_pick_movements": 0, "stock_rows": 0, "warnings": [f"Missing trade log: {log_path}"]}

    text = log_path.read_text(encoding="utf-8")
    events = parse_trade_log(text)

    for event in events:
        cur.execute(
            "INSERT INTO trade_events(id, log_index, trade_date, title, raw_text) VALUES (?, ?, ?, ?, ?)",
            (event.log_index, event.log_index, event.trade_date, event.title, event.raw_text),
        )
        for leg in event.legs:
            cur.execute(
                """
                INSERT INTO trade_legs(trade_event_id, line_no, from_team_abbr, to_team_abbr, asset_text)
                VALUES (?, ?, ?, ?, ?)
                """,
                (event.log_index, leg.line_no, leg.from_team, leg.to_team, leg.asset_text),
            )

    future_moves: list[tuple[TradeEvent, TradeLeg, int, int, str, str]] = []
    for event in events:
        for leg in event.legs:
            for pm in PICK_RE.finditer(leg.asset_text):
                year = int(pm.group(1))
                if year < start_year:
                    continue
                round_no = int(pm.group(2))
                original_team = pm.group(3).strip()
                future_moves.append((event, leg, year, round_no, original_team, pm.group(0)))

    # Accepted proposals can include seeded years even when the text log does not.
    cur.execute("SELECT COALESCE(MAX(pick_year), ?) FROM trade_proposal_items WHERE asset_type='pick'", (start_year,))
    max_proposal_year = int(cur.fetchone()[0] or start_year)
    max_move_year = max([year for _, _, year, _, _, _ in future_moves] + [start_year, max_proposal_year])
    end_year = max(max_move_year, FUTURE_STOCK_MIN_END_YEAR)
    years = list(range(start_year, end_year + 1))
    cur.execute("SELECT COALESCE(MAX(pick_round), ?) FROM trade_proposal_items WHERE asset_type='pick'", (FUTURE_STOCK_ROUNDS,))
    max_proposal_round = int(cur.fetchone()[0] or FUTURE_STOCK_ROUNDS)
    max_round = max([round_no for _, _, _, round_no, _, _ in future_moves] + [FUTURE_STOCK_ROUNDS, max_proposal_round])

    for year in years:
        for round_no in range(1, max_round + 1):
            for team in TEAM_ORDER:
                cur.execute(
                    """
                    INSERT INTO draft_pick_stock(
                        pick_year, pick_round, original_team_abbr, current_owner_abbr,
                        last_acquired_from_abbr, last_trade_event_id, last_trade_date, last_trade_title
                    ) VALUES (?, ?, ?, ?, NULL, NULL, NULL, NULL)
                    """,
                    (year, round_no, team, team),
                )

    warnings: list[str] = []
    process_order = 0

    for event in reversed(events):
        for leg in event.legs:
            for pm in PICK_RE.finditer(leg.asset_text):
                year = int(pm.group(1))
                if year < start_year:
                    continue
                round_no = int(pm.group(2))
                original_team = pm.group(3).strip()
                pick_label = pm.group(0)

                if original_team not in ABBR_TO_FULL:
                    warnings.append(f"Unknown original team abbreviation {original_team} in {pick_label}")
                    cur.execute(
                        """
                        INSERT OR IGNORE INTO draft_pick_stock(
                            pick_year, pick_round, original_team_abbr, current_owner_abbr
                        ) VALUES (?, ?, ?, ?)
                        """,
                        (year, round_no, original_team, original_team),
                    )

                cur.execute(
                    """
                    SELECT current_owner_abbr
                    FROM draft_pick_stock
                    WHERE pick_year=? AND pick_round=? AND original_team_abbr=?
                    """,
                    (year, round_no, original_team),
                )
                row = cur.fetchone()
                previous_owner = row["current_owner_abbr"] if row else original_team
                owner_chain_ok = 1 if previous_owner == leg.from_team else 0
                if not owner_chain_ok:
                    warnings.append(
                        f"Owner-chain mismatch for {pick_label}: log says {leg.from_team}->{leg.to_team}, "
                        f"but reconstructed owner before trade was {previous_owner}. Applied log result anyway."
                    )

                process_order += 1
                cur.execute(
                    """
                    INSERT INTO draft_pick_movements(
                        trade_event_id, process_order, trade_date, title,
                        from_team_abbr, to_team_abbr,
                        pick_year, pick_round, original_team_abbr,
                        previous_owner_abbr, new_owner_abbr, owner_chain_ok, pick_label
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        event.log_index,
                        process_order,
                        event.trade_date,
                        event.title,
                        leg.from_team,
                        leg.to_team,
                        year,
                        round_no,
                        original_team,
                        previous_owner,
                        leg.to_team,
                        owner_chain_ok,
                        pick_label,
                    ),
                )
                cur.execute(
                    """
                    UPDATE draft_pick_stock
                    SET current_owner_abbr=?,
                        last_acquired_from_abbr=?,
                        last_trade_event_id=?,
                        last_trade_date=?,
                        last_trade_title=?
                    WHERE pick_year=? AND pick_round=? AND original_team_abbr=?
                    """,
                    (
                        leg.to_team,
                        leg.from_team,
                        event.log_index,
                        event.trade_date,
                        event.title,
                        year,
                        round_no,
                        original_team,
                    ),
                )

    process_order, proposal_warnings = _apply_accepted_proposal_picks(cur, start_process_order=process_order)
    warnings.extend(proposal_warnings)

    conn.commit()
    cur.execute("SELECT COUNT(*) FROM draft_pick_stock")
    stock_rows = int(cur.fetchone()[0])
    conn.close()

    return {
        "events": len(events),
        "future_pick_movements": len(future_moves),
        "stock_rows": stock_rows,
        "warnings": warnings,
        "years": years,
    }


def bootstrap_trades() -> dict[str, Any]:
    """Called from app.py at startup."""
    return refresh_from_log()


def _ident(col: str) -> str:
    return '"' + col.replace('"', '""') + '"'


def _table_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    cur = conn.cursor()
    try:
        cur.execute(f"PRAGMA table_info({table})")
        return {row[1] for row in cur.fetchall()}
    except Exception:
        return set()


def _roster_exprs(cols: set[str]) -> dict[str, str]:
    def coalesce(names: list[str], fallback: str = "''") -> str:
        parts = [_ident(n) for n in names if n in cols]
        parts.append(fallback)
        return "COALESCE(" + ", ".join(parts) + ")"

    if "id" in cols:
        key_expr = "CAST(id AS TEXT)"
    elif "player_id" in cols:
        key_expr = "CAST(player_id AS TEXT)"
    elif "mlbamid" in cols:
        key_expr = "CAST(mlbamid AS TEXT)"
    else:
        key_expr = "CAST(rowid AS TEXT)"

    if "name" in cols:
        name_expr = "COALESCE(name, '')"
    elif "player_name" in cols:
        name_expr = "COALESCE(player_name, '')"
    elif "first" in cols and "last" in cols:
        name_expr = "TRIM(COALESCE(first, '') || ' ' || COALESCE(last, ''))"
    else:
        name_expr = "'Unknown player'"

    return {
        "key": key_expr,
        "name": name_expr,
        "position": coalesce(["position", "pos"]),
        "roster_status": coalesce(["roster_status", "status"]),
        "contract_type": coalesce(["contract_type", "contract"]),
        "salary": coalesce(["salary"], "0"),
    }


def _players_for_team(team: str, search: str = "", limit: int = 400) -> list[dict[str, Any]]:
    if team not in ABBR_TO_FULL:
        return []
    conn = connect_roster_db()
    cols = _table_columns(conn, "roster_players")
    if not cols or "franchise" not in cols:
        conn.close()
        return []
    e = _roster_exprs(cols)
    params: list[Any] = [team]
    where = "WHERE franchise = ?"
    if search:
        where += f" AND lower({e['name']}) LIKE ?"
        params.append(f"%{search.lower()}%")
    cur = conn.cursor()
    cur.execute(
        f"""
        SELECT
          {e['key']} AS player_key,
          {e['name']} AS name,
          {e['position']} AS position,
          {e['roster_status']} AS roster_status,
          UPPER({e['contract_type']}) AS contract_type,
          {e['salary']} AS salary
        FROM roster_players
        {where}
        ORDER BY lower(name) ASC
        LIMIT ?
        """,
        (*params, limit),
    )
    rows = []
    for r in cur.fetchall():
        d = dict(r)
        d["salary_eligible"] = (d.get("contract_type") or "").upper() in SALARY_CONTRACT_TYPES
        rows.append(d)
    conn.close()
    return rows


def _player_for_team(team: str, player_key: str) -> dict[str, Any] | None:
    conn = connect_roster_db()
    cols = _table_columns(conn, "roster_players")
    if not cols or "franchise" not in cols:
        conn.close()
        return None
    e = _roster_exprs(cols)
    cur = conn.cursor()
    cur.execute(
        f"""
        SELECT
          {e['key']} AS player_key,
          {e['name']} AS name,
          {e['position']} AS position,
          {e['roster_status']} AS roster_status,
          UPPER({e['contract_type']}) AS contract_type,
          {e['salary']} AS salary
        FROM roster_players
        WHERE franchise = ? AND {e['key']} = ?
        LIMIT 1
        """,
        (team, str(player_key)),
    )
    row = cur.fetchone()
    conn.close()
    if not row:
        return None
    d = dict(row)
    d["salary_eligible"] = (d.get("contract_type") or "").upper() in SALARY_CONTRACT_TYPES
    return d


def _validate_player_items_current(items: list[dict[str, Any]]) -> list[str]:
    """Make sure accepted player assets are still owned by the team offering them."""
    player_items = [i for i in items if i.get("asset_type") == "player"]
    if not player_items:
        return []

    conn = connect_roster_db()
    cols = _table_columns(conn, "roster_players")
    if not cols or "franchise" not in cols:
        conn.close()
        return ["Roster database does not have a usable roster_players.franchise column."]

    e = _roster_exprs(cols)
    cur = conn.cursor()
    errors: list[str] = []
    for item in player_items:
        player_key = str(item.get("player_key") or "").strip()
        side_team = (item.get("side_team_abbr") or "").strip().upper()
        player_name = item.get("player_name") or player_key or "Unknown player"
        if not player_key or not side_team:
            errors.append(f"Incomplete player item for {player_name}.")
            continue

        cur.execute(
            f"""
            SELECT franchise
            FROM roster_players
            WHERE {e['key']} = ?
            LIMIT 1
            """,
            (player_key,),
        )
        row = cur.fetchone()
        if not row:
            errors.append(f"{player_name} is no longer in roster_players.")
            continue
        current_team = (row["franchise"] or "").strip().upper()
        if current_team != side_team:
            errors.append(f"{player_name} is currently on {current_team or 'no team'}, not {side_team}.")
    conn.close()
    return errors


def _apply_accepted_proposal_players(items: list[dict[str, Any]]) -> int:
    """Transfer accepted player assets in roster.db by updating roster_players.franchise."""
    player_items = [i for i in items if i.get("asset_type") == "player"]
    if not player_items:
        return 0

    conn = connect_roster_db()
    cols = _table_columns(conn, "roster_players")
    if not cols or "franchise" not in cols:
        conn.close()
        raise RuntimeError("Roster database does not have a usable roster_players.franchise column.")

    e = _roster_exprs(cols)
    cur = conn.cursor()
    moved = 0
    for item in player_items:
        player_key = str(item.get("player_key") or "").strip()
        side_team = (item.get("side_team_abbr") or "").strip().upper()
        receiving_team = (item.get("receiving_team_abbr") or "").strip().upper()
        if not player_key or not side_team or not receiving_team:
            continue

        cur.execute(
            f"""
            UPDATE roster_players
            SET franchise = ?
            WHERE {e['key']} = ?
              AND franchise = ?
            """,
            (receiving_team, player_key, side_team),
        )
        moved += cur.rowcount or 0

    conn.commit()
    conn.close()

    # Reserve-player ownership affects the Rule V pool. Keep that derived DB in sync
    # when the helper is available, matching the roster tab's own update behavior.
    try:
        from rulev_app import sync_rulev_from_roster_db
        sync_rulev_from_roster_db()
    except Exception:
        try:
            current_app.logger.exception("Rule V sync failed after accepted trade proposal roster transfer")
        except Exception:
            pass

    return moved


def _picks_for_team(conn: sqlite3.Connection, team: str, limit: int = 500) -> list[dict[str, Any]]:
    cur = conn.cursor()
    cur.execute(
        """
        SELECT pick_year, pick_round, original_team_abbr, current_owner_abbr,
               (CAST(pick_year AS TEXT) || ' ' || CAST(pick_round AS TEXT) || '.' || original_team_abbr) AS pick_label
        FROM draft_pick_stock
        WHERE current_owner_abbr = ?
          AND pick_round != 1
        ORDER BY pick_year ASC, pick_round ASC, original_team_abbr ASC
        LIMIT ?
        """,
        (team, limit),
    )
    return [dict(r) for r in cur.fetchall()]


def _pick_for_owner(conn: sqlite3.Connection, owner: str, year: int, round_no: int, original_team: str) -> dict[str, Any] | None:
    cur = conn.cursor()
    cur.execute(
        """
        SELECT pick_year, pick_round, original_team_abbr, current_owner_abbr,
               (CAST(pick_year AS TEXT) || ' ' || CAST(pick_round AS TEXT) || '.' || original_team_abbr) AS pick_label
        FROM draft_pick_stock
        WHERE current_owner_abbr=? AND pick_year=? AND pick_round=? AND original_team_abbr=?
        """,
        (owner, year, round_no, original_team),
    )
    row = cur.fetchone()
    return dict(row) if row else None


def _pick_counts(conn: sqlite3.Connection) -> dict[tuple[str, int], int]:
    cur = conn.cursor()
    cur.execute("SELECT current_owner_abbr, pick_year, COUNT(*) AS n FROM draft_pick_stock GROUP BY current_owner_abbr, pick_year")
    return {(r["current_owner_abbr"], int(r["pick_year"])): int(r["n"] or 0) for r in cur.fetchall()}


def _validate_pick_limits(conn: sqlite3.Connection, pick_transfers: list[dict[str, Any]]) -> list[str]:
    errors: list[str] = []
    for item in pick_transfers:
        try:
            round_no = int(item.get("pick_round") or 0)
        except Exception:
            round_no = 0
        if round_no == 1:
            label = item.get("pick_label") or f"{item.get('pick_year')} {item.get('pick_round')}.{item.get('original_team_abbr')}"
            errors.append(f"{label} is a 1st-round pick; 1st-round picks cannot be traded.")
    if errors:
        return errors

    counts = _pick_counts(conn)
    touched: set[tuple[str, int]] = set()
    for item in pick_transfers:
        year = int(item["pick_year"])
        from_team = item["side_team_abbr"]
        to_team = item["receiving_team_abbr"]
        counts[(from_team, year)] = counts.get((from_team, year), 0) - 1
        counts[(to_team, year)] = counts.get((to_team, year), 0) + 1
        touched.add((from_team, year))
        touched.add((to_team, year))
    for team, year in sorted(touched, key=lambda x: (x[1], x[0])):
        n = counts.get((team, year), 0)
        if n < MIN_PICKS_PER_YEAR:
            errors.append(f"{team} would have only {n} picks in {year}; minimum is {MIN_PICKS_PER_YEAR}.")
        if n > MAX_PICKS_PER_YEAR:
            errors.append(f"{team} would have {n} picks in {year}; maximum is {MAX_PICKS_PER_YEAR}.")
    return errors


def _query_trade_rows(conn: sqlite3.Connection, q: str = "", limit: int = 400) -> list[dict[str, Any]]:
    cur = conn.cursor()
    params: list[Any] = []
    where = ""
    if q:
        where = "WHERE lower(title || char(10) || raw_text) LIKE ?"
        params.append(f"%{q.lower()}%")
    cur.execute(
        f"""
        SELECT te.*
        FROM trade_events te
        {where}
        ORDER BY te.log_index ASC
        LIMIT ?
        """,
        (*params, limit),
    )
    rows = [dict(r) for r in cur.fetchall()]
    for row in rows:
        cur.execute(
            """
            SELECT from_team_abbr, to_team_abbr, asset_text
            FROM trade_legs
            WHERE trade_event_id=?
            ORDER BY line_no ASC, id ASC
            """,
            (row["id"],),
        )
        row["legs"] = [dict(r) for r in cur.fetchall()]
    return rows


def _query_stock_rows(conn: sqlite3.Connection, year: str = "", owner: str = "", limit: int = 5000) -> list[sqlite3.Row]:
    params: list[Any] = []
    clauses: list[str] = []
    if year:
        clauses.append("pick_year=?")
        params.append(int(year))
    if owner:
        clauses.append("current_owner_abbr=?")
        params.append(owner)
    where = "WHERE " + " AND ".join(clauses) if clauses else ""
    cur = conn.cursor()
    cur.execute(
        f"""
        SELECT *
        FROM draft_pick_stock
        {where}
        ORDER BY pick_year ASC, pick_round ASC, current_owner_abbr ASC, original_team_abbr ASC
        LIMIT ?
        """,
        (*params, limit),
    )
    return cur.fetchall()


def _years(conn: sqlite3.Connection) -> list[int]:
    cur = conn.cursor()
    cur.execute("SELECT DISTINCT pick_year FROM draft_pick_stock ORDER BY pick_year ASC")
    return [int(r[0]) for r in cur.fetchall()]


def _load_proposals(conn: sqlite3.Connection, team: str) -> dict[str, list[dict[str, Any]]]:
    cur = conn.cursor()
    result = {"outgoing": [], "incoming": []}
    for key, clause in (("outgoing", "proposer_team_abbr=?"), ("incoming", "target_team_abbr=?")):
        cur.execute(
            f"""
            SELECT *
            FROM trade_proposals
            WHERE {clause}
            ORDER BY CASE status WHEN 'pending' THEN 0 WHEN 'accepted' THEN 1 ELSE 2 END,
                     created_at DESC, id DESC
            LIMIT 100
            """,
            (team,),
        )
        proposals = [dict(r) for r in cur.fetchall()]
        for p in proposals:
            cur.execute("SELECT * FROM trade_proposal_items WHERE proposal_id=? ORDER BY side_team_abbr, id", (p["id"],))
            p["items"] = [dict(r) for r in cur.fetchall()]
            cur.execute("SELECT * FROM trade_proposal_payments WHERE proposal_id=? ORDER BY id", (p["id"],))
            p["payments"] = [dict(r) for r in cur.fetchall()]
        result[key] = proposals
    return result


def _proposal_summary(p: dict[str, Any]) -> dict[str, Any]:
    sides = {p["proposer_team_abbr"]: [], p["target_team_abbr"]: []}
    for item in p.get("items", []):
        sides.setdefault(item["side_team_abbr"], []).append(item["display_text"])
    p["side_assets"] = sides
    return p


def _build_items_and_payments(conn: sqlite3.Connection, proposer: str, target: str, trade_date: str, payload: dict[str, Any]) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[str]]:
    items: list[dict[str, Any]] = []
    payments: list[dict[str, Any]] = []
    errors: list[str] = []

    def handle_player(side_team: str, receiving_team: str, raw: dict[str, Any]) -> None:
        key = str(raw.get("player_key") or "").strip()
        player = _player_for_team(side_team, key)
        if not player:
            errors.append(f"Player {key or 'unknown'} is not currently owned by {side_team}.")
            return
        salary = float(player.get("salary") or 0.0)
        contract_type = (player.get("contract_type") or "").upper()
        eligible = contract_type in SALARY_CONTRACT_TYPES
        retained = 0.0
        try:
            retained = max(0.0, float(raw.get("salary_retained") or 0.0))
        except Exception:
            retained = 0.0
        if not eligible:
            retained = 0.0
        if retained > salary:
            errors.append(f"Retained salary for {player['name']} cannot exceed the player's salary ({money(salary)}).")
            retained = salary
        prorated = 0.0
        if eligible and retained <= 0:
            prorated = prorated_salary_payment(salary, trade_date)
        payment_amount = retained if retained > 0 else prorated
        if payment_amount > 0:
            reason = "Retained salary" if retained > 0 else "Prorated salary"
            payments.append({
                "payer_team_abbr": side_team,
                "receiver_team_abbr": receiving_team,
                "amount": round(payment_amount, 2),
                "reason": f"{reason} for {player['name']}",
                "player_key": key,
                "player_name": player["name"],
            })
        suffix = ""
        if eligible:
            if retained > 0:
                suffix = f" — retained {money(retained)}"
            elif prorated > 0:
                suffix = f" — prorated payment {money(prorated)}"
            else:
                suffix = " — no salary adjustment"
        items.append({
            "side_team_abbr": side_team,
            "receiving_team_abbr": receiving_team,
            "asset_type": "player",
            "player_key": key,
            "player_name": player["name"],
            "position": player.get("position") or "",
            "roster_status": player.get("roster_status") or "",
            "contract_type": contract_type,
            "salary": salary,
            "salary_retained": retained,
            "prorated_payment": prorated,
            "cash_amount": 0,
            "pick_year": None,
            "pick_round": None,
            "original_team_abbr": None,
            "pick_label": None,
            "display_text": f"{player['name']} ({contract_type or '—'}, {money(salary)}){suffix}",
        })

    def handle_pick(side_team: str, receiving_team: str, raw: dict[str, Any]) -> None:
        try:
            year = int(raw.get("pick_year"))
            round_no = int(raw.get("pick_round"))
        except Exception:
            errors.append("Invalid draft pick year/round.")
            return
        original = str(raw.get("original_team_abbr") or "").strip().upper()
        if round_no == 1:
            errors.append(f"1st-round picks cannot be traded ({year} {round_no}.{original}).")
            return
        row = _pick_for_owner(conn, side_team, year, round_no, original)
        if not row:
            errors.append(f"{side_team} does not currently own {year} {round_no}.{original}.")
            return
        label = row["pick_label"]
        items.append({
            "side_team_abbr": side_team,
            "receiving_team_abbr": receiving_team,
            "asset_type": "pick",
            "player_key": None,
            "player_name": None,
            "position": None,
            "roster_status": None,
            "contract_type": None,
            "salary": 0,
            "salary_retained": 0,
            "prorated_payment": 0,
            "cash_amount": 0,
            "pick_year": year,
            "pick_round": round_no,
            "original_team_abbr": original,
            "pick_label": label,
            "display_text": label,
        })

    def handle_cash(side_team: str, receiving_team: str, raw_amount: Any) -> None:
        try:
            amount = round(float(raw_amount or 0.0), 2)
        except Exception:
            errors.append(f"Invalid cash amount from {side_team}.")
            return
        if amount < 0:
            errors.append(f"Cash amount from {side_team} cannot be negative.")
            return
        if amount <= 0:
            return
        payments.append({
            "payer_team_abbr": side_team,
            "receiver_team_abbr": receiving_team,
            "amount": amount,
            "reason": "Cash considerations",
            "player_key": None,
            "player_name": None,
        })
        items.append({
            "side_team_abbr": side_team,
            "receiving_team_abbr": receiving_team,
            "asset_type": "cash",
            "player_key": None,
            "player_name": None,
            "position": None,
            "roster_status": None,
            "contract_type": None,
            "salary": 0,
            "salary_retained": 0,
            "prorated_payment": 0,
            "cash_amount": amount,
            "pick_year": None,
            "pick_round": None,
            "original_team_abbr": None,
            "pick_label": None,
            "display_text": f"{money(amount)} cash",
        })

    for raw in payload.get("from_me", []) or []:
        if raw.get("type") == "player":
            handle_player(proposer, target, raw)
        elif raw.get("type") == "pick":
            handle_pick(proposer, target, raw)
    for raw in payload.get("from_them", []) or []:
        if raw.get("type") == "player":
            handle_player(target, proposer, raw)
        elif raw.get("type") == "pick":
            handle_pick(target, proposer, raw)

    handle_cash(proposer, target, payload.get("cash_from_me"))
    handle_cash(target, proposer, payload.get("cash_from_them"))

    if not items:
        errors.append("Select at least one player, draft pick, or cash amount.")

    pick_errors = _validate_pick_limits(conn, [x for x in items if x["asset_type"] == "pick"])
    errors.extend(pick_errors)
    return items, payments, errors


TRADES_HTML = r"""
<!doctype html>
<html>
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>BNSL Trades</title>
  __BNSL_GAME_CSS__
  <style>
    .controls { display:flex; gap:12px; align-items:center; flex-wrap:wrap; margin: 12px 0; }
    .section-title { display:flex; justify-content:space-between; gap:12px; align-items:flex-end; flex-wrap:wrap; margin-top:16px; }
    .asset-line { margin: 2px 0; }
    .bad { color: var(--warn); }
    .good { color: var(--good); }
    .tiny { font-size: 12px; }
    .tabbar { display:flex; gap:10px; flex-wrap:wrap; margin: 0 0 14px; }
    .subtab { border-radius: 999px; padding: 9px 12px; border: 1px solid rgba(140,170,255,.22); background: rgba(255,255,255,.06); color: inherit; cursor: pointer; }
    .subtab.active { border-color: rgba(46,242,255,.55); box-shadow: 0 0 0 3px rgba(46,242,255,.10); }
    .view { display:none; }
    .view.active { display:block; }
    .login-grid { display:grid; grid-template-columns: minmax(180px, 1fr) minmax(220px, 1.2fr) auto; gap:10px; align-items:end; }
    .proposal-grid { display:grid; grid-template-columns: 1fr 1fr; gap:14px; align-items:start; }
    .asset-panel { background: rgba(0,0,0,.15); border: 1px solid rgba(140,170,255,.16); border-radius: 16px; padding:12px; }
    .asset-scroll { max-height: 330px; overflow:auto; border-top:1px solid rgba(255,255,255,.08); margin-top:10px; padding-top:8px; }
    .asset-row { display:grid; grid-template-columns: 26px 1fr; gap:8px; padding:8px 0; border-bottom:1px solid rgba(255,255,255,.06); }
    .asset-meta { font-size:12px; opacity:.74; margin-top:3px; }
    .retain { width: 120px; margin-top: 6px; }
    .cash-input { width: 150px; margin-top: 6px; }
    .preview { margin-top:14px; background: rgba(0,0,0,.18); border:1px solid rgba(140,170,255,.16); border-radius:16px; padding:12px; }
    .proposal-card { border:1px solid rgba(140,170,255,.16); border-radius:16px; padding:12px; margin: 10px 0; background: rgba(0,0,0,.15); }
    .proposal-card h3 { margin:0 0 6px; }
    .status { text-transform:uppercase; letter-spacing:.4px; }
    textarea { width:100%; min-height:70px; }
    @media (max-width: 980px){ .proposal-grid,.login-grid{grid-template-columns:1fr;} }
  </style>
</head>
<body>
  <div class="page">
    <div class="brand">
      <div>
        <h1>TRADES</h1>
        <div class="sub">Trade log, future pick stock, and manager-to-manager proposals.</div>
      </div>
      <div class="right">
        <a class="btn" href="/draft/pick-stock" target="_self">Future Pick Order</a>
        <span class="badge">TRADE CENTER</span>
      </div>
    </div>

    <div class="panel pad">
      {% if stats.warnings %}
        <div class="pill bad" style="margin-bottom:12px; white-space:normal;">
          {{ stats.warnings|length }} draft-stock warning{{ '' if stats.warnings|length == 1 else 's' }}. Check /trades/api/movements for owner-chain details.
        </div>
      {% endif %}

      <div class="tabbar">
        <button class="subtab active" data-view="log">Trade Log</button>
        <button class="subtab" data-view="propose">Propose Trade</button>
        <button class="subtab" data-view="proposals">Proposals</button>
        <button class="subtab" data-view="stock">Draft Pick Stock</button>
      </div>

      <div id="view-log" class="view active">
        <div class="section-title">
          <div>
            <h2 style="margin:0;">Trade Log</h2>
            <div class="muted tiny">Newest first, matching trades.txt.</div>
          </div>
          <form class="controls" method="get" action="/trades/">
            <input name="q" value="{{ q }}" placeholder="Search title/assets…" />
            <select name="year" onchange="this.form.submit()">
              <option value="">All years</option>
              {% for y in years %}<option value="{{ y }}" {% if selected_year == y|string %}selected{% endif %}>{{ y }}</option>{% endfor %}
            </select>
            <select name="owner" onchange="this.form.submit()">
              <option value="">All teams</option>
              {% for abbr in teams %}<option value="{{ abbr }}" {% if selected_owner == abbr %}selected{% endif %}>{{ team_label(abbr) }}</option>{% endfor %}
            </select>
            <button class="btn" type="submit">Apply</button>
          </form>
        </div>

        <div class="table-wrap">
          <table>
            <thead><tr><th style="width:11%;">Date</th><th style="width:29%;">Trade</th><th>Assets</th></tr></thead>
            <tbody>
              {% for row in trade_rows %}
              <tr class="row-hover">
                <td><b>{{ row.trade_date }}</b></td>
                <td>{{ row.title }}</td>
                <td>{% for leg in row.legs %}<div class="asset-line"><b>{{ leg.from_team_abbr }}</b> → <b>{{ leg.to_team_abbr }}</b>: {{ leg.asset_text }}</div>{% endfor %}</td>
              </tr>
              {% endfor %}
              {% if not trade_rows %}<tr><td colspan="3" class="muted">No matching trades.</td></tr>{% endif %}
            </tbody>
          </table>
        </div>
      </div>

      <div id="view-propose" class="view">
        <div class="section-title">
          <div>
            <h2 style="margin:0;">Propose Trade</h2>
            <div class="muted tiny">Login with the manager email associated with your team, then build each side of the proposal.</div>
          </div>
        </div>

        <div class="asset-panel" id="login-panel">
          <div id="login-status" class="pill" style="margin-bottom:10px;">Loading login state…</div>
          <div class="login-grid">
            <label>Your Team<br><select id="login-team"></select></label>
            <label>Manager Email<br><input id="login-email" type="email" placeholder="manager@example.com"></label>
            <button id="login-btn" class="btn primary" type="button">Login</button>
          </div>
        </div>

        <div id="builder" style="display:none; margin-top:14px;">
          <div class="controls">
            <label class="pill">Date of Trade <input id="trade-date" type="date" value="{{ today }}" style="margin-left:8px;"></label>
            <label class="pill">Target Team <select id="target-team" style="margin-left:8px;"></select></label>
          </div>

          <div class="proposal-grid">
            <div class="asset-panel">
              <h3 id="my-side-title">Your Assets</h3>
              <input id="my-search" placeholder="Filter players…" style="width:100%;">
              <label class="asset-meta">Cash sent in deal<br><input id="my-cash" class="cash-input" type="number" min="0" step="1000" value="0"></label>
              <div class="asset-scroll" id="my-assets"></div>
            </div>
            <div class="asset-panel">
              <h3 id="their-side-title">Target Assets</h3>
              <input id="their-search" placeholder="Filter players…" style="width:100%;">
              <label class="asset-meta">Cash sent in deal<br><input id="their-cash" class="cash-input" type="number" min="0" step="1000" value="0"></label>
              <div class="asset-scroll" id="their-assets"></div>
            </div>
          </div>

          <div class="preview">
            <h3 style="margin-top:0;">Proposal Preview</h3>
            <div id="preview-body" class="muted">Select assets to preview salary payments and pick-count checks.</div>
            <label>Notes<br><textarea id="proposal-notes" placeholder="Optional note for the other manager…"></textarea></label>
            <div class="controls"><button id="submit-proposal" class="btn primary" type="button">Submit Proposal</button><span id="submit-status" class="muted"></span></div>
          </div>
        </div>
      </div>

      <div id="view-proposals" class="view">
        <div class="section-title">
          <div><h2 style="margin:0;">Submitted and Incoming Proposals</h2><div class="muted tiny">Incoming proposals can be accepted or declined by the target team.</div></div>
          <button id="refresh-proposals" class="btn" type="button">Refresh</button>
        </div>
        <div id="proposal-list" class="muted">Login to view proposals.</div>
      </div>

      <div id="view-stock" class="view">
        <div class="section-title">
          <div>
            <h2 style="margin:0;">Draft Pick Stock</h2>
            <div class="muted tiny">Current owner of every original future draft pick.</div>
          </div>
        </div>
        <div class="table-wrap">
          <table>
            <thead><tr><th style="width:10%;">Year</th><th style="width:10%;">Round</th><th style="width:22%;">Original Pick</th><th style="width:22%;">Current Owner</th><th>Last Movement</th></tr></thead>
            <tbody>
              {% for row in stock_rows %}
              <tr class="row-hover">
                <td>{{ row.pick_year }}</td>
                <td>{{ row.pick_round }}</td>
                <td><b>{{ row.original_team_abbr }}</b> <span class="muted">{{ display_team(row.original_team_abbr) }}</span></td>
                <td><b>{{ row.current_owner_abbr }}</b> <span class="muted">{{ display_team(row.current_owner_abbr) }}</span></td>
                <td class="muted">{% if row.last_trade_date %}{{ row.last_trade_date }} — {{ row.last_trade_title }}{% else %}Original owner{% endif %}</td>
              </tr>
              {% endfor %}
              {% if not stock_rows %}<tr><td colspan="5" class="muted">No draft stock rows found.</td></tr>{% endif %}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  </div>

<script>
const teams = {{ teams|tojson }};
const teamLabels = {{ team_labels|tojson }};
let auth = {team: {{ authed|tojson }}, email: {{ authed_email|tojson }}};
let assets = {mine: null, theirs: null};
let selected = {mine: new Map(), theirs: new Map()};

function $(id){ return document.getElementById(id); }
function money(x){ return '$' + Number(x || 0).toLocaleString(undefined, {maximumFractionDigits:0}); }
function esc(s){ return String(s ?? '').replace(/[&<>"']/g, m => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[m])); }

function switchView(name){
  document.querySelectorAll('.subtab').forEach(b => b.classList.toggle('active', b.dataset.view === name));
  document.querySelectorAll('.view').forEach(v => v.classList.toggle('active', v.id === 'view-' + name));
  if (name === 'proposals') loadProposals();
}
document.querySelectorAll('.subtab').forEach(b => b.onclick = () => switchView(b.dataset.view));

function fillTeamSelect(sel, includeBlank=false){
  sel.innerHTML = (includeBlank ? '<option value="">— Select —</option>' : '') + teams.map(t => `<option value="${t}">${teamLabels[t]}</option>`).join('');
}

async function loadAuth(){
  const res = await fetch('/trades/api/auth');
  auth = await res.json();
  updateAuthUi();
}
function updateAuthUi(){
  fillTeamSelect($('login-team'));
  if (auth.team) $('login-team').value = auth.team;
  $('login-status').textContent = auth.team ? `🔓 Logged in as ${auth.email || 'manager'} for ${auth.team}` : '🔒 Not logged in';
  $('builder').style.display = auth.team ? 'block' : 'none';
  if (auth.team) setupBuilder();
}
$('login-btn').onclick = async () => {
  const res = await fetch('/trades/api/login', {method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify({team:$('login-team').value, email:$('login-email').value})});
  if (!res.ok) { alert(await res.text()); return; }
  auth = await res.json();
  $('login-email').value = '';
  updateAuthUi();
};

function setupBuilder(){
  fillTeamSelect($('target-team'));
  if ($('target-team').value === auth.team) $('target-team').value = teams.find(t => t !== auth.team) || '';
  $('target-team').querySelectorAll('option').forEach(o => { o.disabled = (o.value === auth.team); });
  $('my-side-title').textContent = `${auth.team} assets`;
  $('their-side-title').textContent = `${$('target-team').value} assets`;
  selected.mine.clear(); selected.theirs.clear();
  if ($('my-cash')) $('my-cash').value = 0;
  if ($('their-cash')) $('their-cash').value = 0;
  loadAssets('mine'); loadAssets('theirs');
}
$('target-team').onchange = () => { selected.theirs.clear(); $('their-side-title').textContent = `${$('target-team').value} assets`; loadAssets('theirs'); renderPreview(); };
$('trade-date').onchange = renderPreview;
$('my-search').oninput = () => loadAssets('mine');
$('their-search').oninput = () => loadAssets('theirs');
$('my-cash').oninput = renderPreview;
$('their-cash').oninput = renderPreview;

async function loadAssets(side){
  if (!auth.team) return;
  const team = side === 'mine' ? auth.team : $('target-team').value;
  const q = side === 'mine' ? $('my-search').value : $('their-search').value;
  if (!team) return;
  const res = await fetch(`/trades/api/assets/${team}?q=${encodeURIComponent(q || '')}`);
  const data = await res.json();
  assets[side] = data;
  renderAssets(side);
}

function keyFor(asset){ return asset.type === 'player' ? `player:${asset.player_key}` : `pick:${asset.pick_year}:${asset.pick_round}:${asset.original_team_abbr}`; }
function assetPayload(asset){
  if (asset.type === 'player') return {type:'player', player_key: asset.player_key, salary_retained: Number(asset.salary_retained || 0)};
  return {type:'pick', pick_year: asset.pick_year, pick_round: asset.pick_round, original_team_abbr: asset.original_team_abbr};
}
function renderAssets(side){
  const box = side === 'mine' ? $('my-assets') : $('their-assets');
  const data = assets[side] || {players:[], picks:[]};
  const map = selected[side];
  const rows = [];
  rows.push(`<div class="muted tiny">Players</div>`);
  for (const p of data.players) {
    p.type = 'player';
    const k = keyFor(p);
    const checked = map.has(k) ? 'checked' : '';
    const retain = p.salary_eligible ? `<div class="asset-meta">A/X/FA salary rules apply. Retain: <input class="retain" data-side="${side}" data-key="${k}" type="number" min="0" max="${Number(p.salary||0)}" step="1000" value="${esc((map.get(k)||{}).salary_retained || 0)}"></div>` : '';
    rows.push(`<label class="asset-row"><input type="checkbox" data-side="${side}" data-key="${k}" ${checked}><div><b>${esc(p.name)}</b><div class="asset-meta">${esc(p.position||'')} • ${esc(p.roster_status||'')} • ${esc(p.contract_type||'—')} • ${money(p.salary)}</div>${retain}</div></label>`);
  }
  rows.push(`<div class="muted tiny" style="margin-top:12px;">Draft Picks</div>`);
  for (const pick of data.picks) {
    pick.type = 'pick';
    const k = keyFor(pick);
    const checked = map.has(k) ? 'checked' : '';
    rows.push(`<label class="asset-row"><input type="checkbox" data-side="${side}" data-key="${k}" ${checked}><div><b>${esc(pick.pick_label)}</b><div class="asset-meta">Current owner: ${esc(pick.current_owner_abbr)}</div></div></label>`);
  }
  box.innerHTML = rows.join('');
  box.querySelectorAll('input[type=checkbox]').forEach(cb => cb.onchange = onCheckChange);
  box.querySelectorAll('input.retain').forEach(inp => inp.oninput = onRetainChange);
}
function findAsset(side, k){
  const data = assets[side] || {players:[], picks:[]};
  return [...data.players.map(x => ({...x, type:'player'})), ...data.picks.map(x => ({...x, type:'pick'}))].find(a => keyFor(a) === k);
}
function onCheckChange(e){
  const side = e.target.dataset.side, k = e.target.dataset.key;
  if (e.target.checked) selected[side].set(k, {...findAsset(side, k)});
  else selected[side].delete(k);
  renderAssets(side); renderPreview();
}
function onRetainChange(e){
  const side = e.target.dataset.side, k = e.target.dataset.key;
  if (selected[side].has(k)) selected[side].get(k).salary_retained = Number(e.target.value || 0);
  renderPreview();
}
async function renderPreview(){
  if (!auth.team || !$('target-team').value) return;
  const payload = buildPayload();
  const res = await fetch('/trades/api/proposals/preview', {method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify(payload)});
  const data = await res.json();
  if (!res.ok) {
    $('preview-body').innerHTML = `<div class="bad">${esc(data.error || 'Preview failed')}</div>`;
    return;
  }
  let html = '';
  if (data.errors && data.errors.length) html += `<div class="bad"><b>Cannot submit yet:</b><br>${data.errors.map(esc).join('<br>')}</div>`;
  html += `<div class="proposal-grid"><div><b>${esc(auth.team)} sends</b><ul>${data.from_me.map(x => `<li>${esc(x)}</li>`).join('') || '<li class="muted">Nothing selected</li>'}</ul></div><div><b>${esc($('target-team').value)} sends</b><ul>${data.from_them.map(x => `<li>${esc(x)}</li>`).join('') || '<li class="muted">Nothing selected</li>'}</ul></div></div>`;
  if (data.payments.length) html += `<div><b>Payments generated by this proposal:</b><ul>${data.payments.map(p => `<li><b>${esc(p.payer_team_abbr)}</b> pays <b>${esc(p.receiver_team_abbr)}</b> ${money(p.amount)} — ${esc(p.reason)}</li>`).join('')}</ul></div>`;
  else html += `<div class="muted">No payments generated for this proposal.</div>`;
  $('preview-body').innerHTML = html;
}
function buildPayload(){
  return {
    target_team: $('target-team').value,
    trade_date: $('trade-date').value,
    notes: $('proposal-notes').value,
    cash_from_me: Number($('my-cash').value || 0),
    cash_from_them: Number($('their-cash').value || 0),
    from_me: Array.from(selected.mine.values()).map(assetPayload),
    from_them: Array.from(selected.theirs.values()).map(assetPayload),
  };
}
$('submit-proposal').onclick = async () => {
  $('submit-status').textContent = 'Submitting…';
  const res = await fetch('/trades/api/proposals', {method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify(buildPayload())});
  const data = await res.json().catch(() => ({}));
  if (!res.ok) {
    $('submit-status').innerHTML = `<span class="bad">${esc(data.error || 'Could not submit proposal')}</span>`;
    return;
  }
  selected.mine.clear(); selected.theirs.clear();
  $('my-cash').value = 0; $('their-cash').value = 0;
  renderAssets('mine'); renderAssets('theirs'); renderPreview();
  $('submit-status').innerHTML = `<span class="good">Proposal #${data.id} submitted.</span>`;
  loadProposals();
};

async function loadProposals(){
  if (!auth.team) { $('proposal-list').textContent = 'Login to view proposals.'; return; }
  const res = await fetch('/trades/api/proposals');
  if (!res.ok) { $('proposal-list').textContent = await res.text(); return; }
  const data = await res.json();
  const makeCard = (p, incoming) => {
    const actions = incoming && p.status === 'pending' ? `<div class="controls"><button class="btn primary accept" data-id="${p.id}">Accept</button><button class="btn decline" data-id="${p.id}">Decline</button></div>` : '';
    const payments = p.payments && p.payments.length ? `<div class="muted tiny">Payments: ${p.payments.map(x => `${esc(x.payer_team_abbr)} → ${esc(x.receiver_team_abbr)} ${money(x.amount)} (${esc(x.reason)})`).join('; ')}</div>` : '';
    return `<div class="proposal-card"><h3>#${p.id} <span class="status ${p.status === 'accepted' ? 'good' : p.status === 'declined' ? 'bad' : ''}">${esc(p.status)}</span></h3><div class="muted tiny">${esc(p.proposer_team_abbr)} ↔ ${esc(p.target_team_abbr)} • Trade date ${esc(p.trade_date)} • Submitted ${esc(p.created_at)}</div><div class="proposal-grid"><div><b>${esc(p.proposer_team_abbr)} sends</b><ul>${(p.side_assets[p.proposer_team_abbr]||[]).map(x=>`<li>${esc(x)}</li>`).join('') || '<li class="muted">Nothing</li>'}</ul></div><div><b>${esc(p.target_team_abbr)} sends</b><ul>${(p.side_assets[p.target_team_abbr]||[]).map(x=>`<li>${esc(x)}</li>`).join('') || '<li class="muted">Nothing</li>'}</ul></div></div>${payments}${p.notes ? `<div class="muted">Note: ${esc(p.notes)}</div>` : ''}${actions}</div>`;
  };
  $('proposal-list').innerHTML = `<h3>Incoming</h3>${data.incoming.map(p => makeCard(p, true)).join('') || '<div class="muted">No incoming proposals.</div>'}<h3>Submitted</h3>${data.outgoing.map(p => makeCard(p, false)).join('') || '<div class="muted">No submitted proposals.</div>'}`;
  document.querySelectorAll('.accept').forEach(b => b.onclick = () => actProposal(b.dataset.id, 'accept'));
  document.querySelectorAll('.decline').forEach(b => b.onclick = () => actProposal(b.dataset.id, 'decline'));
}
async function actProposal(id, action){
  const res = await fetch(`/trades/api/proposals/${id}/${action}`, {method:'POST'});
  if (!res.ok) alert(await res.text());
  await loadProposals();
}
$('refresh-proposals').onclick = loadProposals;

fillTeamSelect($('login-team'));
loadAuth();
</script>
</body>
</html>
""".replace("__BNSL_GAME_CSS__", BNSL_GAME_CSS)


@trades_bp.route("/")
def trades_page():
    stats = refresh_from_log()
    q = (request.args.get("q") or "").strip()
    selected_year = (request.args.get("year") or "").strip()
    selected_owner = (request.args.get("owner") or "").strip().upper()

    conn = connect_stock_db()
    trade_rows = _query_trade_rows(conn, q=q)
    stock_rows = _query_stock_rows(conn, year=selected_year, owner=selected_owner)
    years = _years(conn)
    conn.close()

    labels = {abbr: team_label(abbr) for abbr in TEAM_ORDER}
    return render_template_string(
        TRADES_HTML,
        stats=stats,
        trade_rows=trade_rows,
        stock_rows=stock_rows,
        years=years,
        selected_year=selected_year,
        selected_owner=selected_owner,
        q=q,
        teams=TEAM_ORDER,
        team_labels=labels,
        authed=authed_team(),
        authed_email=session.get("authed_email", ""),
        today=_today_iso(),
        display_team=display_team,
        team_label=team_label,
    )


@trades_bp.get("/api/auth")
def api_auth():
    return jsonify({"team": authed_team(), "email": session.get("authed_email", "")})


@trades_bp.post("/api/login")
def api_login():
    data = request.get_json(force=True, silent=True) or {}
    team = (data.get("team") or "").strip().upper()
    email = (data.get("email") or "").strip()
    if team not in ABBR_TO_FULL:
        return ("Unknown team", 400)
    expected = team_email_lookup().get(team)
    if not expected:
        return ("No email configured for team", 400)
    if not emails_equal(email, expected):
        return ("Invalid email for this team", 401)
    session["authed_team"] = team
    session["authed_email"] = email
    return jsonify({"team": team, "email": email})


@trades_bp.get("/api/assets/<team>")
def api_assets(team: str):
    refresh_from_log()
    team = team.strip().upper()
    if team not in ABBR_TO_FULL:
        return ("Unknown team", 404)
    q = (request.args.get("q") or "").strip()
    conn = connect_stock_db()
    picks = _picks_for_team(conn, team)
    conn.close()
    players = _players_for_team(team, search=q)
    return jsonify({"team": team, "players": players, "picks": picks})


@trades_bp.post("/api/proposals/preview")
def api_preview_proposal():
    proposer = authed_team()
    if not proposer:
        return jsonify({"error": "Login required"}), 401
    refresh_from_log()
    payload = request.get_json(force=True, silent=True) or {}
    target = (payload.get("target_team") or "").strip().upper()
    trade_date = (payload.get("trade_date") or _today_iso()).strip()
    if target not in ABBR_TO_FULL or target == proposer:
        return jsonify({"error": "Select a valid target team."}), 400
    conn = connect_stock_db()
    items, payments, errors = _build_items_and_payments(conn, proposer, target, trade_date, payload)
    conn.close()
    return jsonify({
        "errors": errors,
        "from_me": [i["display_text"] for i in items if i["side_team_abbr"] == proposer],
        "from_them": [i["display_text"] for i in items if i["side_team_abbr"] == target],
        "payments": payments,
    })


@trades_bp.route("/api/proposals", methods=["GET", "POST"])
def api_proposals():
    team = authed_team()
    if not team:
        return ("Login required", 401)
    refresh_from_log()
    conn = connect_stock_db()
    init_stock_schema(conn)
    if request.method == "GET":
        result = _load_proposals(conn, team)
        result["incoming"] = [_proposal_summary(p) for p in result["incoming"]]
        result["outgoing"] = [_proposal_summary(p) for p in result["outgoing"]]
        conn.close()
        return jsonify(result)

    payload = request.get_json(force=True, silent=True) or {}
    target = (payload.get("target_team") or "").strip().upper()
    trade_date = (payload.get("trade_date") or _today_iso()).strip()
    notes = (payload.get("notes") or "").strip()
    if target not in ABBR_TO_FULL or target == team:
        conn.close()
        return jsonify({"error": "Select a valid target team."}), 400
    items, payments, errors = _build_items_and_payments(conn, team, target, trade_date, payload)
    if errors:
        conn.close()
        return jsonify({"error": "Proposal failed validation.", "errors": errors}), 400

    cur = conn.cursor()
    now = _now_iso()
    cur.execute(
        """
        INSERT INTO trade_proposals(created_at, updated_at, status, proposer_team_abbr, target_team_abbr, trade_date, notes)
        VALUES (?, ?, 'pending', ?, ?, ?, ?)
        """,
        (now, now, team, target, trade_date, notes),
    )
    proposal_id = int(cur.lastrowid)
    for item in items:
        cur.execute(
            """
            INSERT INTO trade_proposal_items(
                proposal_id, side_team_abbr, receiving_team_abbr, asset_type,
                player_key, player_name, position, roster_status, contract_type, salary,
                salary_retained, prorated_payment, cash_amount,
                pick_year, pick_round, original_team_abbr, pick_label, display_text
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                proposal_id,
                item["side_team_abbr"],
                item["receiving_team_abbr"],
                item["asset_type"],
                item["player_key"],
                item["player_name"],
                item["position"],
                item["roster_status"],
                item["contract_type"],
                item["salary"],
                item["salary_retained"],
                item["prorated_payment"],
                item.get("cash_amount", 0),
                item["pick_year"],
                item["pick_round"],
                item["original_team_abbr"],
                item["pick_label"],
                item["display_text"],
            ),
        )
    for payment in payments:
        cur.execute(
            """
            INSERT INTO trade_proposal_payments(
                proposal_id, payer_team_abbr, receiver_team_abbr, amount, reason, player_key, player_name, status
            ) VALUES (?, ?, ?, ?, ?, ?, ?, 'pending')
            """,
            (
                proposal_id,
                payment["payer_team_abbr"],
                payment["receiver_team_abbr"],
                payment["amount"],
                payment["reason"],
                payment["player_key"],
                payment["player_name"],
            ),
        )
    conn.commit()
    conn.close()
    return jsonify({"id": proposal_id, "status": "pending"})


def _get_proposal_for_action(conn: sqlite3.Connection, proposal_id: int, team: str) -> sqlite3.Row | None:
    cur = conn.cursor()
    cur.execute("SELECT * FROM trade_proposals WHERE id=? AND target_team_abbr=?", (proposal_id, team))
    return cur.fetchone()


@trades_bp.post("/api/proposals/<int:proposal_id>/accept")
def api_accept_proposal(proposal_id: int):
    team = authed_team()
    if not team:
        return ("Login required", 401)
    refresh_from_log()
    conn = connect_stock_db()
    init_stock_schema(conn)
    proposal = _get_proposal_for_action(conn, proposal_id, team)
    if not proposal:
        conn.close()
        return ("Proposal not found for your team", 404)
    if proposal["status"] != "pending":
        conn.close()
        return ("Proposal is not pending", 400)

    cur = conn.cursor()
    cur.execute("SELECT * FROM trade_proposal_items WHERE proposal_id=?", (proposal_id,))
    items = [dict(r) for r in cur.fetchall()]
    errors = _validate_pick_limits(conn, [i for i in items if i.get("asset_type") == "pick"])
    errors.extend(_validate_player_items_current(items))
    if errors:
        conn.close()
        return ("Cannot accept: " + "; ".join(errors), 400)

    now = _now_iso()
    cur.execute(
        """
        UPDATE trade_proposals
        SET status='accepted', updated_at=?, accepted_at=?, acted_by_team_abbr=?
        WHERE id=?
        """,
        (now, now, team, proposal_id),
    )
    cur.execute("UPDATE trade_proposal_payments SET status='posted' WHERE proposal_id=?", (proposal_id,))
    cur.execute("SELECT * FROM trade_proposal_payments WHERE proposal_id=?", (proposal_id,))
    for p in cur.fetchall():
        cur.execute(
            """
            INSERT OR IGNORE INTO finance_payments(
                source_type, source_id, created_at, effective_date,
                payer_team_abbr, receiver_team_abbr, amount, description, status
            ) VALUES ('trade_proposal', ?, ?, ?, ?, ?, ?, ?, 'posted')
            """,
            (
                proposal_id,
                now,
                proposal["trade_date"],
                p["payer_team_abbr"],
                p["receiver_team_abbr"],
                float(p["amount"] or 0.0),
                f"Accepted trade proposal #{proposal_id}: {p['reason']}",
            ),
        )
    conn.commit()
    conn.close()

    moved_players = _apply_accepted_proposal_players(items)

    # Rebuild stock so accepted proposal pick transfers layer on top immediately.
    refresh_from_log()
    return jsonify({"id": proposal_id, "status": "accepted", "players_moved": moved_players})


@trades_bp.post("/api/proposals/<int:proposal_id>/decline")
def api_decline_proposal(proposal_id: int):
    team = authed_team()
    if not team:
        return ("Login required", 401)
    conn = connect_stock_db()
    init_stock_schema(conn)
    proposal = _get_proposal_for_action(conn, proposal_id, team)
    if not proposal:
        conn.close()
        return ("Proposal not found for your team", 404)
    if proposal["status"] != "pending":
        conn.close()
        return ("Proposal is not pending", 400)
    conn.execute(
        "UPDATE trade_proposals SET status='declined', updated_at=?, acted_by_team_abbr=? WHERE id=?",
        (_now_iso(), team, proposal_id),
    )
    conn.commit()
    conn.close()
    return jsonify({"id": proposal_id, "status": "declined"})


@trades_bp.get("/api/log")
def api_log():
    refresh_from_log()
    q = (request.args.get("q") or "").strip()
    conn = connect_stock_db()
    rows = _query_trade_rows(conn, q=q, limit=1000)
    conn.close()
    return jsonify({"rows": rows})


@trades_bp.get("/api/draft-stock")
def api_draft_stock():
    stats = refresh_from_log()
    year = (request.args.get("year") or "").strip()
    owner = (request.args.get("owner") or "").strip().upper()
    conn = connect_stock_db()
    rows = [dict(r) for r in _query_stock_rows(conn, year=year, owner=owner, limit=5000)]
    conn.close()
    return jsonify({"stats": stats, "rows": rows})


@trades_bp.get("/api/movements")
def api_movements():
    stats = refresh_from_log()
    conn = connect_stock_db()
    cur = conn.cursor()
    cur.execute("SELECT * FROM draft_pick_movements ORDER BY process_order ASC")
    rows = [dict(r) for r in cur.fetchall()]
    conn.close()
    return jsonify({"stats": stats, "rows": rows})


if __name__ == "__main__":
    result = refresh_from_log()
    print(result)
