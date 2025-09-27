"""Central paths and constant values used across the app."""

from pathlib import Path

from src.utils.io import make_path_exists


class AppConstants:
    """Well-known directories and filenames.

    All paths are resolved relative to the repository root.
    """
    app_dir = Path(__file__).resolve().parents[1]
    logs_dir = app_dir / "logs"
    data_dir = app_dir / "data"
    input_dir = data_dir / "input"
    export_dir = data_dir / "export"
    make_path_exists([logs_dir, data_dir, input_dir, export_dir])

    # Instagram export basenames (without .json)
    followers_basename = "followers_1"
    followings_basename = "following"
