import logging

from .config import AppConstants as C
from utils.io import load_json

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