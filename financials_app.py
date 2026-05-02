from __future__ import annotations

import sqlite3
from typing import Any, Dict, List

from flask import Blueprint, current_app, jsonify, render_template_string

from ui_skin import BNSL_GAME_CSS

financials_bp = Blueprint("financials", __name__)

TEAM_FINANCIALS: Dict[str, Dict[str, Any]] = {
    "ARI": {"team_name": "Arizona Diamondbacks", "revenue": 154_009_000, "hard_cap": 177_053_000},
    "ATL": {"team_name": "Atlanta Braves", "revenue": 150_246_000, "hard_cap": 172_694_000},
    "BAL": {"team_name": "Baltimore Orioles", "revenue": 132_968_000, "hard_cap": 155_433_000},
    "BOS": {"team_name": "Boston Red Sox", "revenue": 166_977_000, "hard_cap": 192_014_000},
    "CHC": {"team_name": "Chicago Cubs", "revenue": 157_707_000, "hard_cap": 182_510_000},
    "CHW": {"team_name": "Chicago White Sox", "revenue": 152_294_000, "hard_cap": 176_589_000},
    "CIN": {"team_name": "Cincinnati Reds", "revenue": 130_034_000, "hard_cap": 152_710_000},
    "CLE": {"team_name": "Cleveland Guardians", "revenue": 150_921_000, "hard_cap": 173_799_000},
    "COL": {"team_name": "Colorado Rockies", "revenue": 168_188_000, "hard_cap": 193_330_000},
    "DET": {"team_name": "Detroit Tigers", "revenue": 177_492_000, "hard_cap": 203_469_000},
    "HOU": {"team_name": "Houston Astros", "revenue": 156_495_000, "hard_cap": 180_036_000},
    "KC": {"team_name": "Kansas City Royals", "revenue": 159_383_000, "hard_cap": 182_527_000},
    "LAA": {"team_name": "Los Angeles Angels", "revenue": 157_049_000, "hard_cap": 179_403_000},
    "LAD": {"team_name": "Los Angeles Dodgers", "revenue": 121_206_000, "hard_cap": 144_507_000},
    "MIA": {"team_name": "Miami Marlins", "revenue": 176_064_000, "hard_cap": 202_897_000},
    "MIL": {"team_name": "Milwaukee Brewers", "revenue": 137_502_000, "hard_cap": 158_660_000},
    "MIN": {"team_name": "Minnesota Twins", "revenue": 164_220_000, "hard_cap": 186_833_000},
    "NYM": {"team_name": "New York Mets", "revenue": 158_265_000, "hard_cap": 181_632_000},
    "NYY": {"team_name": "New York Yankees", "revenue": 166_831_000, "hard_cap": 191_824_000},
    "OAK": {"team_name": "Oakland Athletics", "revenue": 163_970_000, "hard_cap": 189_106_000},
    "PHI": {"team_name": "Philadelphia Phillies", "revenue": 148_779_000, "hard_cap": 172_352_000},
    "PIT": {"team_name": "Pittsburgh Pirates", "revenue": 144_847_000, "hard_cap": 167_049_000},
    "SD": {"team_name": "San Diego Padres", "revenue": 144_951_000, "hard_cap": 168_353_000},
    "SF": {"team_name": "San Francisco Giants", "revenue": 160_599_000, "hard_cap": 184_668_000},
    "SEA": {"team_name": "Seattle Mariners", "revenue": 147_691_000, "hard_cap": 169_528_000},
    "STL": {"team_name": "St. Louis Cardinals", "revenue": 143_815_000, "hard_cap": 165_110_000},
    "TB": {"team_name": "Tampa Bay Rays", "revenue": 138_430_000, "hard_cap": 159_805_000},
    "TEX": {"team_name": "Texas Rangers", "revenue": 127_445_000, "hard_cap": 148_725_000},
    "TOR": {"team_name": "Toronto Blue Jays", "revenue": 147_043_000, "hard_cap": 169_541_000},
    "WAS": {"team_name": "Washington Nationals", "revenue": 147_924_000, "hard_cap": 169_704_000},
}


def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(current_app.config["ROSTER_DB_PATH"])
    conn.row_factory = sqlite3.Row
    return conn


def _has_column(conn: sqlite3.Connection, table: str, column: str) -> bool:
    cur = conn.cursor()
    cur.execute(f"PRAGMA table_info({table})")
    return column in {row[1] for row in cur.fetchall()}


def money(value: Any) -> str:
    try:
        return f"${float(value):,.0f}"
    except Exception:
        return "$0"


def compute_financial_rows(team: str | None = None) -> List[Dict[str, Any]]:
    """
    Compute live financials from roster.db.

    Expenses are defined as:
      salaries for every player with roster_status='Active'
      + salaries for A/X/FA players with roster_status in ('40-man', 'Reserve')

    R-contract players therefore only count against expenses while Active.
    This helper is intentionally importable so the FA tab can reuse cap_space
    later without duplicating the financial calculation.
    """
    conn = get_conn()
    cur = conn.cursor()

    rows: List[Dict[str, Any]] = []
    codes = [team.upper()] if team else sorted(TEAM_FINANCIALS.keys())

    for code in codes:
        meta = TEAM_FINANCIALS.get(code)
        if not meta:
            continue

        cur.execute("""
            SELECT
                COALESCE(SUM(CASE WHEN roster_status = 'Active' THEN 1 ELSE 0 END), 0) AS active_count,
                COALESCE(SUM(CASE WHEN roster_status IN ('Active', '40-man') THEN 1 ELSE 0 END), 0) AS forty_count,
                COUNT(*) AS total_count,
                COALESCE(SUM(
                    CASE
                        WHEN roster_status = 'Active'
                             OR (
                                roster_status IN ('40-man', 'Reserve')
                                AND UPPER(COALESCE(contract_type, '')) IN ('A', 'X', 'FA')
                             )
                        THEN COALESCE(salary, 0)
                        ELSE 0
                    END
                ), 0) AS expenses
            FROM roster_players
            WHERE franchise = ?
        """, (code,))
        r = cur.fetchone()

        revenue = float(meta["revenue"])
        hard_cap = float(meta["hard_cap"])
        expenses = float(r["expenses"] or 0.0)
        surplus = revenue - expenses
        cap_space = hard_cap - expenses

        rows.append({
            "team_name": meta["team_name"],
            "abbr": code,
            "active": int(r["active_count"] or 0),
            "forty": int(r["forty_count"] or 0),
            "total": int(r["total_count"] or 0),
            "revenue": revenue,
            "expenses": expenses,
            "surplus": surplus,
            "hard_cap": hard_cap,
            "cap_space": cap_space,
        })

    conn.close()
    return rows


def get_cap_space_by_team(team: str) -> float | None:
    """Small helper for future FA-tab checks."""
    rows = compute_financial_rows(team)
    if not rows:
        return None
    return float(rows[0]["cap_space"])


@financials_bp.get("/api/summary")
def api_financial_summary():
    return jsonify({"teams": compute_financial_rows()})


@financials_bp.get("/api/cap_space/<team>")
def api_cap_space(team: str):
    rows = compute_financial_rows(team)
    if not rows:
        return ("Unknown team", 404)
    row = rows[0]
    return jsonify({
        "team": row["abbr"],
        "expenses": row["expenses"],
        "hard_cap": row["hard_cap"],
        "cap_space": row["cap_space"],
    })


FINANCIALS_HTML = f"""
<!doctype html>
<html>
<head>
  <base href="/financials/">
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Financials</title>
  {BNSL_GAME_CSS}
  <style>
    .wrap {{ max-width: 1450px; margin: 0 auto; padding: 18px; }}
    table {{ width: 100%; border-collapse: collapse; }}
    th, td {{ padding: 8px 10px; border-bottom: 1px solid rgba(255,255,255,.08); }}
    th {{ text-align: left; position: sticky; top: 0; }}
    td.num, th.num {{ text-align: right; white-space: nowrap; }}
    .muted {{ opacity: .75; margin: 4px 0 14px; }}
  </style>
</head>
<body>
  <div class="wrap">
    <h1>Financials</h1>
    <p class="muted">Expenses = Active salaries + A/X/FA salaries on 40-man or Reserve. R-contract players only count while Active.</p>

    <table>
      <thead>
        <tr>
          <th>Team Name</th>
          <th>Abbr</th>
          <th class="num">Active</th>
          <th class="num">40</th>
          <th class="num">Total</th>
          <th class="num">Revenue</th>
          <th class="num">Expenses</th>
          <th class="num">Surplus</th>
          <th class="num">Hard Cap</th>
          <th class="num">Cap Space</th>
        </tr>
      </thead>
      <tbody>
        {{% for row in rows %}}
        <tr>
          <td>{{{{ row.team_name }}}}</td>
          <td>{{{{ row.abbr }}}}</td>
          <td class="num">{{{{ row.active }}}}</td>
          <td class="num">{{{{ row.forty }}}}</td>
          <td class="num">{{{{ row.total }}}}</td>
          <td class="num">{{{{ money(row.revenue) }}}}</td>
          <td class="num">{{{{ money(row.expenses) }}}}</td>
          <td class="num">{{{{ money(row.surplus) }}}}</td>
          <td class="num">{{{{ money(row.hard_cap) }}}}</td>
          <td class="num">{{{{ money(row.cap_space) }}}}</td>
        </tr>
        {{% endfor %}}
      </tbody>
    </table>
  </div>
</body>
</html>
"""


@financials_bp.get("/")
def financials_index():
    return render_template_string(
        FINANCIALS_HTML,
        rows=compute_financial_rows(),
        money=money,
    )
