import os
import sys
from pathlib import Path

def _update_sys_path():

    root_dir = Path(__file__).resolve().parents[1]
    if str(root_dir) not in sys.path:
        sys.path.insert(0, str(root_dir))
    os.chdir(root_dir)

_update_sys_path()

from src.utils.logs import setup_logging
from src.json_main import main as json_main

def main():
    setup_logging()
    json_main()

if __name__ == "__main__":
    raise SystemExit(main())