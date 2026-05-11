from __future__ import annotations

"""Small Discord webhook helper for BNSL apps.

Each caller supplies the environment-variable name for the webhook it needs.
If that variable is unset, the notification is written to the app log/stdout
instead of being sent to Discord.
"""

import json
import logging
import os
import time
import urllib.error
import urllib.request
from typing import Iterable

try:
    from flask import current_app, has_app_context
except Exception:  # lets CLI/import-time utilities use this without Flask installed
    current_app = None  # type: ignore[assignment]

    def has_app_context() -> bool:  # type: ignore[no-redef]
        return False


_LOG = logging.getLogger(__name__)


def _emit(message: str) -> None:
    try:
        if has_app_context() and current_app is not None:
            current_app.logger.info(message)
            return
    except Exception:
        pass
    _LOG.info(message)
    print(message)


def _find_webhook_url(env_var: str, legacy_env_vars: Iterable[str] = ()) -> tuple[str, str]:
    for name in (env_var, *legacy_env_vars):
        value = (os.environ.get(name) or "").strip()
        if value:
            return value, name
    return "", env_var


def send_discord_message(
    env_var: str,
    content: str,
    *,
    fallback_label: str = "discord",
    username: str | None = None,
    legacy_env_vars: Iterable[str] = (),
) -> bool:
    """Send a plain Discord webhook message.

    Returns True only when Discord accepted the message. Missing env vars or
    network/API failures are intentionally non-fatal and log the message instead.
    """
    content = str(content or "").strip()
    if not content:
        return False

    # Discord content messages max at 2000 chars. Keep a little room for ellipsis.
    if len(content) > 1900:
        content = content[:1897].rstrip() + "..."

    url, source_env = _find_webhook_url(env_var, legacy_env_vars)
    if not url:
        _emit(f"[DISCORD-DRYRUN:{fallback_label}] {content}")
        return False

    payload: dict[str, str] = {"content": content}
    if username:
        payload["username"] = username

    data = json.dumps(payload).encode("utf-8")

    def post_once() -> tuple[bool, int, bytes]:
        req = urllib.request.Request(
            url,
            data=data,
            headers={
                "Content-Type": "application/json",
                "User-Agent": "bnsl-webapp-discord/1.0",
            },
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=15) as resp:
            body = resp.read()
            return resp.status in (200, 204), int(resp.status), body

    try:
        ok, status, body = post_once()
        if ok:
            _emit(f"[DISCORD:{fallback_label}] sent via {source_env} status={status}")
            return True

        if status == 429:
            retry_after = 1.0
            try:
                retry_after = float(json.loads(body.decode("utf-8", "ignore")).get("retry_after", retry_after))
            except Exception:
                pass
            time.sleep(min(3.0, max(0.5, retry_after)))
            ok2, status2, body2 = post_once()
            if ok2:
                _emit(f"[DISCORD:{fallback_label}] sent after retry via {source_env} status={status2}")
                return True
            _emit(f"[DISCORD:{fallback_label}] failed after retry status={status2} body={body2[:300]!r}; message={content}")
            return False

        _emit(f"[DISCORD:{fallback_label}] non-2xx status={status} body={body[:300]!r}; message={content}")
        return False

    except urllib.error.HTTPError as exc:
        body = exc.read() if hasattr(exc, "read") else b""
        _emit(f"[DISCORD:{fallback_label}] HTTPError status={getattr(exc, 'code', '???')} body={body[:300]!r}; message={content}")
    except urllib.error.URLError as exc:
        _emit(f"[DISCORD:{fallback_label}] URLError {exc}; message={content}")
    except Exception as exc:
        _emit(f"[DISCORD:{fallback_label}] failed: {exc}; message={content}")
    return False
