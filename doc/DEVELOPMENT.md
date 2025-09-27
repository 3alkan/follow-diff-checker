# Development notes

## Running
```pwsh
python src/main.py
```

## Structure
- src/main.py: orchestrates loading, parsing, diffing, exporting
- src/config.py: app paths and constants
- src/utils/json.py: parsing helpers for Instagram export shapes
- src/utils/export.py: HTML report generation (escaped, basic styling)
- src/utils/io.py: basic filesystem helpers (ensure dirs, load json)
- src/utils/logs.py: logging configured to console and logs/app.log using Türkiye time
- src/utils/timeutil.py: time utilities, Istanbul timezone preference

## Types and errors
- Parsers return dict[str, DomainObject] or empty dict on error; logging is minimal.
- HTML generation skips rows with missing fields.

## Future ideas
- Expand CLI (argparse) to accept custom input/output paths and basenames.
- Unit tests for parsers and HTML generator.
- Add optional CSV export.
- Hook up pre-commit and static analysis.
