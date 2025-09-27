import logging

from src.config import AppConstants as C
from src.utils.io import load_json
from src.utils.json import parse_followers, parse_followings, parse_pending_follow_requests
from src.utils.export import generate_html

def main():
    followers_path = C.json_input_dir / f"{C.followers_basename}.json"
    followings_path = C.json_input_dir / f"{C.followings_basename}.json"
    pending_follow_requests_path = C.json_input_dir / f"{C.pending_follow_requests_basename}.json"

    followers = load_json(followers_path)
    followings = load_json(followings_path)
    pending_follow_requests = load_json(pending_follow_requests_path)

    if not (followers and followings and pending_follow_requests):
        logging.warning("One or more input files are missing or empty.")
        return
    
    followers=parse_followers(followers)
    followings=parse_followings(followings)
    pending_follow_requests=parse_pending_follow_requests(pending_follow_requests)

    if not (followers and followings):
        logging.warning("Failed to parse followers or followings data.")
        return
    
    fwings_fwers=set(followings.keys()) - set(followers.keys())
    fwers_fwings=set(followers.keys()) - set(followings.keys())
    pending_fwers=set(pending_follow_requests.keys()) - set(followers.keys())
    pending_fwings=set(pending_follow_requests.keys()) - set(followings.keys())

    if fwings_fwers:
        generate_html(
            data=followings,
            keys=fwings_fwers,
            title="Following but not followed back",
            out_name="following_not_followed_back.html",
        )
        logging.info(f"Generated following_not_followed_back.html")