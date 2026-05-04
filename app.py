from pathlib import Path
import os
from flask import Flask, render_template

import fa_app
import draft_app
import roster_app
import rulev_app
import trades_app

from fa_app import fa_bp
from draft_app import draft_bp
from roster_app import roster_bp
from financials_app import financials_bp
from draft_order_page import order_bp
from rulev_app import rulev_bp
from rulev_order_page import rulev_order_bp
from trades_app import trades_bp


def create_app():
    app = Flask(__name__)
    app.secret_key = os.environ.get("FLASK_SECRET_KEY", "your-secret")

    APP_DIR = Path(__file__).resolve().parent

    app.config["DRAFT_DB_PATH"]  = str(APP_DIR / "draft.db")
    app.config["FA_DB_PATH"]     = str(APP_DIR / "fa.db")
    app.config["RULEV_DB_PATH"]  = str(APP_DIR / "rulev.db")
    app.config["ROSTER_DB_PATH"] = str(APP_DIR / "roster.db")
    app.config["ROSTER_CSV_PATH"] = str(APP_DIR / "rostered_2025service.csv")
    app.config["OOTP_FA_ROSTER_PATH"] = str(APP_DIR / "bnsl_ootp27_fixed_rosters_oldids_optionsupdated.txt")
    app.config["HOMETOWN_DISCOUNTS_DB_PATH"] = str(APP_DIR / "hometown_discounts.db")
    app.config["TRADES_LOG_PATH"] = str(APP_DIR / "trades.txt")
    app.config["DRAFT_STOCK_DB_PATH"] = str(APP_DIR / "draft_stock.db")

    app.register_blueprint(draft_bp,  url_prefix="/draft")
    app.register_blueprint(fa_bp,     url_prefix="/fa")
    app.register_blueprint(roster_bp, url_prefix="/roster")
    app.register_blueprint(financials_bp, url_prefix="/financials")
    app.register_blueprint(order_bp,  url_prefix="/draft")
    app.register_blueprint(rulev_bp,  url_prefix="/rulev")
    app.register_blueprint(rulev_order_bp, url_prefix="/rulev")
    app.register_blueprint(trades_bp, url_prefix="/trades")

    with app.app_context():
        # roster.db must exist before FA/Rule V syncs use it as the source of truth.
        roster_app.bootstrap_roster()
        rulev_app.bootstrap_rulev()
        fa_app.bootstrap_fa()
        trades_app.bootstrap_trades()

    @app.get("/")
    def home():
        return render_template("home.html")

    return app


if __name__ == "__main__":
    create_app().run(host="0.0.0.0", port=5000, debug=True)
