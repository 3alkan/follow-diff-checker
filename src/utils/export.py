from __future__ import annotations

import logging
from html import escape
from pathlib import Path

from src.config import AppConstants as C
from src.utils.timeutil import format_ts_tr, now_tr


def _to_istanbul_dt(ts):
	return format_ts_tr(ts)


def generate_html(data: dict, keys: set, title: str, out_name: str) -> None:
    """Generate an HTML report under export dir using selected usernames.

    Args:
        data: Mapping of username -> object with attributes/keys: username, profile_url, timestamp.
        keys: Set of usernames to include in the report.
        title: Title to show in the HTML report.
        out_name: Output filename (e.g., 'following_not_followed_back.html').
    """
    try:
        export_path: Path = C.export_dir / out_name

        # Build rows
        rows: list[str] = []
        for username in sorted(keys, key=lambda s: s.lower()):
            item = data.get(username)
            if not item:
                continue
            # Support attribute-like access
            uname = getattr(item, "username", None)
            purl = getattr(item, "profile_url", None)
            ts = getattr(item, "timestamp", None)
			
            if not (uname and purl and ts):
                continue  
	
            ts_fmt = _to_istanbul_dt(ts)

            uname_e = escape(str(uname))
            purl_e = escape(str(purl))
            ts_e = escape(ts_fmt)

            link = f"<a href=\"{purl_e}\" target=\"_blank\">@{uname_e}</a>" if purl_e else f"@{uname_e}"
            rows.append(
                f"<tr><td>{link}</td><td>{purl_e}</td><td>{ts_e}</td></tr>"
            )

        generated_at = now_tr()

        html = f"""
<!DOCTYPE html>
<html lang=\"tr\">
<head>
  <meta charset=\"utf-8\" />
  <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\" />
  <title>{escape(title)}</title>
  <style>
	body {{ font-family: -apple-system, Segoe UI, Roboto, Helvetica, Arial, sans-serif; margin: 24px; color: #222; }}
	h1 {{ margin-bottom: 4px; }}
	.meta {{ color: #666; margin-bottom: 16px; font-size: 0.9rem; }}
	table {{ border-collapse: collapse; width: 100%; }}
	th, td {{ border: 1px solid #ddd; padding: 8px; }}
	th {{ background: #f7f7f7; text-align: left; }}
	tr:nth-child(even) {{ background: #fafafa; }}
	a {{ color: #0a66c2; text-decoration: none; }}
	a:hover {{ text-decoration: underline; }}
  </style>
  </head>
  <body>
	<h1>{escape(title)}</h1>
	<div class=\"meta\">Toplam: {len(rows)} • Oluşturulma: {escape(generated_at)}</div>
	<table>
	  <thead>
		<tr>
		  <th>Kullanıcı</th>
		  <th>Profil URL</th>
		  <th>Zaman</th>
		</tr>
	  </thead>
	  <tbody>
		{''.join(rows)}
	  </tbody>
	</table>
  </body>
</html>
"""
        export_path.write_text(html, encoding="utf-8")
    except Exception as e:
        logging.error(f"Failed to generate HTML '{out_name}': {e}")