import os
from flask import Flask, render_template

from bnsl_paths import db_path, generated_path, input_path

import fa_app
import draft_app
import roster_app
import rulev_app
import trades_app
import waivers_app

from fa_app import fa_bp
from draft_app import draft_bp
from roster_app import roster_bp
from financials_app import financials_bp
from draft_order_page import order_bp
from rulev_app import rulev_bp
from rulev_order_page import rulev_order_bp
from trades_app import trades_bp
from waivers_app import waivers_bp


def create_app():
    app = Flask(__name__)
    app.secret_key = os.environ.get("FLASK_SECRET_KEY", "your-secret")

    # All mutable/runtime state is routed through bnsl_paths.py.
    # On Render, set BNSL_DATA_DIR=/data so these files live on the persistent disk.
    # Locally, leave BNSL_DATA_DIR unset for the old repo-relative behavior, or set
    # BNSL_DATA_DIR=.bnsl_data to test the persistent-disk layout.
    app.config["DRAFT_DB_PATH"] = str(db_path("draft.db"))
    app.config["FA_DB_PATH"] = str(db_path("fa.db"))
    app.config["RULEV_DB_PATH"] = str(db_path("rulev.db"))
    app.config["ROSTER_DB_PATH"] = str(db_path("roster.db"))
    app.config["DRAFT_STOCK_DB_PATH"] = str(db_path("draft_stock.db"))

    app.config["ROSTER_CSV_PATH"] = str(input_path("rostered_2025service.csv"))
    app.config["OOTP_FA_ROSTER_PATH"] = str(input_path("bnsl_ootp27_fixed_rosters_oldids_optionsupdated.txt"))
    app.config["TRADES_LOG_PATH"] = str(input_path("trades.txt"))
    app.config["HOMETOWN_DISCOUNTS_DB_PATH"] = str(generated_path("hometown_discounts.db"))

    app.register_blueprint(draft_bp,  url_prefix="/draft")
    app.register_blueprint(fa_bp,     url_prefix="/fa")
    app.register_blueprint(roster_bp, url_prefix="/roster")
    app.register_blueprint(financials_bp, url_prefix="/financials")
    app.register_blueprint(order_bp,  url_prefix="/draft")
    app.register_blueprint(rulev_bp,  url_prefix="/rulev")
    app.register_blueprint(rulev_order_bp, url_prefix="/rulev")
    app.register_blueprint(trades_bp, url_prefix="/trades")
    app.register_blueprint(waivers_bp, url_prefix="/waivers")

    with app.app_context():
        # roster.db must exist before FA/Rule V syncs use it as the source of truth.
        roster_app.bootstrap_roster()
        waivers_app.bootstrap_waivers()
        rulev_app.bootstrap_rulev()
        fa_app.bootstrap_fa()
        trades_app.bootstrap_trades()

    @app.get("/")
    def home():
        return render_template("home.html")

    return app


if __name__ == "__main__":
    create_app().run(host="0.0.0.0", port=5000, debug=True)
