"""Entry point for IG Follow Diff.

Loads Instagram export JSON from data/input, computes the difference
between who you follow and who follows you, and generates HTML reports
under data/export/.
"""

import os
import sys
from pathlib import Path

def _update_sys_path():
    """Ensure project root is on sys.path and set CWD to root.

    This lets you run `python src/main.py` from repository root
    without installation.
    """

    root_dir = Path(__file__).resolve().parents[1]
    if str(root_dir) not in sys.path:
        sys.path.insert(0, str(root_dir))
    os.chdir(root_dir)

_update_sys_path()

import logging

from src.config import AppConstants as C
from src.utils.io import load_json
from src.utils.json import parse_followers, parse_followings
from src.utils.export import generate_html
from src.utils.logs import setup_logging

def main():
    """Run the pipeline: load -> parse -> diff -> export HTML."""
    setup_logging()
    
    followers_path = C.input_dir / f"{C.followers_basename}.json"
    followings_path = C.input_dir / f"{C.followings_basename}.json"

    followers = load_json(followers_path)
    followings = load_json(followings_path)

    if not (followers and followings):
        logging.warning("One or more input files are missing or empty.")
        return
    
    followers=parse_followers(followers)
    followings=parse_followings(followings)

    if not (followers and followings):
        logging.warning("Failed to parse followers or followings data.")
        return
    
    # Users you follow who don't follow you back
    fwings_fwers=set(followings.keys()) - set(followers.keys())
    # Users who follow you but you don't follow back
    fwers_fwings=set(followers.keys()) - set(followings.keys())

    if fwings_fwers:
        generate_html(
            data=followings,
            keys=fwings_fwers,
            title="Following but not followed back",
            out_name="following_not_followed_back.html",
        )
        logging.info(f"Generated following_not_followed_back.html")
    if fwers_fwings:
        generate_html(
            data=followers,
            keys=fwers_fwings,
            title="Followers not followed back",
            out_name="followers_not_followed_back.html",
        )
        logging.info(f"Generated followers_not_followed_back.html")

if __name__ == "__main__":
    raise SystemExit(main())