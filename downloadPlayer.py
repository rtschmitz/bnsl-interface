#!/usr/bin/env python3
"""
Download a FanGraphs player headshot PNG given player name + FanGraphs ID.

Usage:
  python fg_headshot.py "Ryan McMahon" 15112 --out static/player_images
"""

from __future__ import annotations

import argparse
import os
import re
from pathlib import Path
import requests


def slugify_player_name(name: str) -> str:
    # FanGraphs player URLs use lowercase + hyphens; keep it simple.
    # (If you already store the full URL, you can skip slugifying entirely.)
    slug = name.strip().lower()
    slug = re.sub(r"[^a-z0-9\s-]", "", slug)
    slug = re.sub(r"\s+", "-", slug)
    slug = re.sub(r"-+", "-", slug)
    return slug


def guess_player_page_url(name: str, fg_id: int) -> str:
    slug = slugify_player_name(name)
    # The URL you gave works (redirects/variants exist, but this is fine):
    return f"https://www.fangraphs.com/players/{slug}/{fg_id}/stats"


def extract_headshot_url(html: str) -> str | None:
    """
    FanGraphs headshots commonly look like:
      https://images.fangraphs.com/nobg_small_XXXXXXXX.png
    We'll search for the "nobg" PNG first, then fall back to any images.fangraphs.com PNG.
    """
    m = re.search(r"https://images\.fangraphs\.com/[^\"']*nobg[^\"']*\.png", html)
    if m:
        return m.group(0)

    m = re.search(r"https://images\.fangraphs\.com/[^\"']+\.png", html)
    if m:
        return m.group(0)

    return None


def download_with_referer(session: requests.Session, url: str, referer: str, outpath: Path) -> None:
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/120 Safari/537.36"
        ),
        "Referer": referer,
        "Accept": "image/avif,image/webp,image/apng,image/*,*/*;q=0.8",
    }

    with session.get(url, headers=headers, stream=True, timeout=30) as r:
        r.raise_for_status()
        outpath.parent.mkdir(parents=True, exist_ok=True)
        with open(outpath, "wb") as f:
            for chunk in r.iter_content(chunk_size=1024 * 64):
                if chunk:
                    f.write(chunk)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("name", help='Player name, e.g. "Ryan McMahon"')
    ap.add_argument("fg_id", type=int, help="FanGraphs player ID, e.g. 15112")
    ap.add_argument("--out", default="player_images", help="Output directory")
    ap.add_argument("--filename", default=None, help="Optional output filename (default: <fg_id>.png)")
    ap.add_argument("--force", action="store_true", help="Redownload even if file exists")
    args = ap.parse_args()

    player_url = guess_player_page_url(args.name, args.fg_id)
    out_dir = Path(args.out)
    filename = args.filename or f"{args.fg_id}.png"
    out_path = out_dir / filename

    if out_path.exists() and not args.force:
        print(f"Already exists, skipping: {out_path}")
        return

    session = requests.Session()

    # First request the HTML (also helps set cookies, if any)
    html_headers = {
        "User-Agent": (
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/120 Safari/537.36"
        ),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    }
    resp = session.get(player_url, headers=html_headers, timeout=30)
    resp.raise_for_status()

    headshot_url = extract_headshot_url(resp.text)
    if not headshot_url:
        raise SystemExit(f"Could not find a headshot PNG on page: {player_url}")

    print(f"Player page: {player_url}")
    print(f"Headshot:   {headshot_url}")

    download_with_referer(session, headshot_url, referer=player_url, outpath=out_path)
    print(f"Saved to:   {out_path}")


if __name__ == "__main__":
    main()

