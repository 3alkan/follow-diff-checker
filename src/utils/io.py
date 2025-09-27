import json
import logging
from pathlib import Path

def make_path_exists(path:Path | list[Path]) -> None:
    """Ensure that the directory for the given path exists."""
    if isinstance(path, list):
        for p in path:
            p.parent.mkdir(parents=True, exist_ok=True)
    else:
        path.parent.mkdir(parents=True, exist_ok=True)

def load_json(path:Path) -> dict:
    """Load JSON data from a file."""
    if not path.exists():
        logging.warning(f"File {path} does not exist.")
        return {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        logging.error(f"Error loading JSON from {path}: {e}")
        return {}