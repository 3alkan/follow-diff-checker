# IG Follow Diff

Compare your Instagram followers vs following lists locally and generate clean HTML reports:
- Following but not followed back
- Followers you don’t follow back

No login or API usage. Works entirely offline using Instagram’s official data export JSON.

## Features
- Simple: drop the JSON files from Instagram’s export into `data/input/` and run
- Fast: pure Python, no network calls
- Local-first: outputs static HTML under `data/export/`
- Istanbul (Türkiye) timestamps in logs and reports

## Requirements
- Python 3.10+
- Windows, macOS, or Linux (commands below are for Windows PowerShell)

Optional: create a virtual environment for isolation.

## Installation

1) Clone or download the repository

2) (Optional) Create a virtual environment and activate it

```pwsh
# from repository root
python -m venv .venv
. .venv/Scripts/Activate.ps1
```

3) Install dependencies

```pwsh
pip install -r requirements.txt
```

## Getting your Instagram data
Export your data from Instagram (Settings > Accounts Center > Your information and permissions > Download your information). Choose JSON format.

From the downloaded archive, locate the following files and copy them into `data/input/`:
- Followers: `followers_1.json`
- Following: `following.json`

See more details and examples in `doc/DATA_FORMATS.md`.

## How to run
From the repository root:

```pwsh
python src/main.py
```

Output files will be created under `data/export/`:
- `following_not_followed_back.html`
- `followers_not_followed_back.html`

Open them in your browser.

## Project layout
- `src/main.py` — entry point; loads JSON, computes diffs, writes HTML
- `src/config.py` — paths and constants
- `src/utils/json.py` — parsers for Instagram JSON structures
- `src/utils/export.py` — HTML report generator
- `src/utils/io.py` — filesystem helpers and JSON loader
- `src/utils/logs.py` — logging configuration
- `src/utils/timeutil.py` — Istanbul/Türkiye time helpers
- `data/input/` — place your Instagram JSON here
- `data/export/` — generated HTML reports

## Logging
Logs are written to `logs/app.log` and to the console. Timestamps are shown in Türkiye local time.

## Troubleshooting
- No output HTML files are generated
  - Ensure the input files exist and are not empty: `data/input/followers_1.json` and `data/input/following.json`
  - Check `logs/app.log` for warnings such as missing files or parsing errors
- HTML shows zero rows
  - Verify the JSON files match Instagram’s export structure (see `doc/DATA_FORMATS.md`)
  - Some usernames may be deactivated/changed and not appear in both lists

## Development
- Code style: standard Python with type hints; minimal dependencies
- Run from source; no packaging or CLI framework is required
- See `doc/DEVELOPMENT.md` for notes and ideas

## Roadmap ideas
- Include pending follow requests in reporting
- Optional CSV export
- CLI flags for custom filenames and output directory
- GitHub Actions to lint/test

## License
This project is provided as-is. Add a license if you plan to share or distribute widely.
