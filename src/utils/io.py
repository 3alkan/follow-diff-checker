import json
import logging
from pathlib import Path

def make_path_exists(path: Path | list[Path]) -> None:
    """Ensure that required directories exist.

    If a Path looks like a file path (has a suffix), its parent directory
    will be created. If it looks like a directory (no suffix), the directory
    itself will be created.
    """
    def _ensure(p: Path) -> None:
        # Heuristic: if it has a suffix (e.g. ".json"), treat as a file path
        target = p.parent if p.suffix else p
        target.mkdir(parents=True, exist_ok=True)

    if isinstance(path, list):
        for p in path:
            _ensure(p)
    else:
        _ensure(path)

def load_json(path:Path) -> object:
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