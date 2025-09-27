import os
import sys
from pathlib import Path

def _update_sys_path():

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
    setup_logging()
    
    followers_path = C.json_input_dir / f"{C.followers_basename}.json"
    followings_path = C.json_input_dir / f"{C.followings_basename}.json"

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
    
    fwings_fwers=set(followings.keys()) - set(followers.keys())
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