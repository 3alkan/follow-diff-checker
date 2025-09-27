from pathlib import Path

from src.utils.io import make_path_exists

class AppConstants:
    app_dir = Path(__file__).resolve().parents[1]
    logs_dir = app_dir / "logs"
    data_dir = app_dir / "data"
    input_dir = data_dir / "input"
    html_input_dir = input_dir / "html"
    json_input_dir = input_dir / "json"
    export_dir = data_dir / "export"
    make_path_exists([logs_dir, data_dir, input_dir, html_input_dir, json_input_dir, export_dir])
    followers_basename = "followers_1"
    followings_basename = "following"
