from __future__ import annotations
import logging
import sys
import time

from src.config import AppConstants as C
from src.utils.timeutil import tr_time_tuple

def setup_logging(level: int = logging.INFO) -> None:
    """Configure root logging to write to logs/app.log and also print to console."""
    if getattr(logging, "_utils_logs_configured", False):
        return

    root = logging.getLogger()
    root.setLevel(level)

    # Use shared Türkiye timezone converter
    def _tr_time_converter(secs: float) -> time.struct_time:
        return tr_time_tuple(secs)

    fmt = logging.Formatter(
        fmt="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        # Türkiye-style datetime: DD.MM.YYYY HH:MM:SS
        datefmt="%d.%m.%Y %H:%M:%S",
    )
    # Ensure formatter uses TR timezone
    fmt.converter = _tr_time_converter  # type: ignore[attr-defined]

    file_path=C.logs_dir / "app.log"
    # File handler
    file_handler = logging.FileHandler(file_path, encoding="utf-8")
    file_handler.setLevel(level)
    file_handler.setFormatter(fmt)
    root.addHandler(file_handler)

    # Console handler
    console_handler = logging.StreamHandler(stream=sys.stdout)
    console_handler.setLevel(level)
    console_handler.setFormatter(fmt)
    root.addHandler(console_handler)

    logging._utils_logs_configured = True  # type: ignore[attr-defined]