

from utils.logs import setup_logging
from json_main import main as json_main

def main():
    setup_logging()
    json_main()

if __name__ == "__main__":
    raise SystemExit(main())