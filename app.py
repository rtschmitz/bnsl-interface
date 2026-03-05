from pathlib import Path
import os
from flask import Flask, render_template

import fa_app
import draft_app
from fa_app import fa_bp
from draft_app import draft_bp
from draft_order_page import order_bp
from rulev_app import rulev_bp
from rulev_order_page import rulev_order_bp

def create_app():
    app = Flask(__name__)
    app.secret_key = os.environ.get("FLASK_SECRET_KEY", "your-secret")

    APP_DIR = Path(__file__).resolve().parent

    # Separate DB config keys (don’t reuse "DB_PATH" for both)
    app.config["DRAFT_DB_PATH"] = str(APP_DIR / "draft.db")
    app.config["FA_DB_PATH"]    = str(APP_DIR / "fa.db")
    app.config["RULEV_DB_PATH"] = str(APP_DIR / "rulev.db")

    # If your draft blueprint currently reads app.config["DB_PATH"],
    # you can either update it to read DRAFT_DB_PATH, or temporarily set:
    # app.config["DB_PATH"] = app.config["DRAFT_DB_PATH"]

    # Register blueprints
    app.register_blueprint(draft_bp, url_prefix="/draft")
    app.register_blueprint(fa_bp, url_prefix="/fa")
    app.register_blueprint(order_bp, url_prefix="/draft")
    app.register_blueprint(rulev_bp, url_prefix="/rulev")
    app.register_blueprint(rulev_order_bp, url_prefix="/rulev")

# Bootstrap both apps *inside* application context
    with app.app_context():
        # If draft has a similar bootstrap, call it here
        # draft_app.bootstrap_draft()  # (only if you create it)
        fa_app.bootstrap_fa()

    @app.get("/")
    def home():
        return render_template("home.html")

    return app

if __name__ == "__main__":
    create_app().run(host="0.0.0.0", port=5000, debug=True)
