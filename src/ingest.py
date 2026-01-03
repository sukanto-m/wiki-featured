from __future__ import annotations

import argparse
import datetime as dt
import json
import logging
import os
import sqlite3
import sys
from dataclasses import dataclass
from typing import Any, Dict, List

import requests

# ================== CONSTANTS ==================

MOST_READ_DAY_URL = (
    "https://api.wikimedia.org/feed/v1/wikipedia/{lang}/most-read/{yyyy}/{mm}/{dd}"
)
DEFAULT_UA = "wiki-attention-archive/1.1 (github project)"
LOOKBACK_DAYS = 7  # how far back to probe for latest available data

# ================== MODELS ==================

@dataclass(frozen=True)
class Row:
    date: str
    section: str
    title: str
    extra: str
    url: str

# ================== LOGGING ==================

def setup_logger() -> logging.Logger:
    logger = logging.getLogger("wiki-archive")
    logger.setLevel(logging.INFO)
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(
        logging.Formatter(
            "[%(asctime)s] [%(levelname)s] %(message)s",
            "%Y-%m-%d %H:%M:%S",
        )
    )
    if not logger.handlers:
        logger.addHandler(handler)
    return logger

# ================== HELPERS ==================

def fetch_json(url: str, ua: str) -> Dict[str, Any]:
    r = requests.get(url, headers={"User-Agent": ua}, timeout=30)
    r.raise_for_status()
    return r.json()

def find_latest_available_day(lang: str, ua: str) -> dt.date | None:
    """
    Probe backwards from today to find the most recent date
    for which the most-read endpoint exists.
    """
    today = dt.date.today()

    for delta in range(LOOKBACK_DAYS):
        day = today - dt.timedelta(days=delta)
        url = MOST_READ_DAY_URL.format(
            lang=lang,
            yyyy=day.year,
            mm=f"{day.month:02}",
            dd=f"{day.day:02}",
        )
        try:
            fetch_json(url, ua)
            return day
        except requests.HTTPError as e:
            if e.response is not None and e.response.status_code == 404:
                continue
            raise

    return None

def normalize_most_read(day: dt.date, payload: Dict[str, Any]) -> List[Row]:
    rows: List[Row] = []
    date_s = day.isoformat()

    for rank, a in enumerate(payload.get("articles", []), start=1):
        rows.append(
            Row(
                date=date_s,
                section="most_read",
                title=a.get("title", ""),
                extra=f"rank={rank};views={a.get('views', 0)}",
                url=a.get("content_urls", {})
                    .get("desktop", {})
                    .get("page", ""),
            )
        )
    return rows

# ================== DATABASE ==================

def init_db(con: sqlite3.Connection) -> None:
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS wiki (
            date TEXT,
            section TEXT,
            title TEXT,
            extra TEXT,
            url TEXT,
            PRIMARY KEY (date, section, title)
        )
        """
    )
    con.execute(
        "CREATE INDEX IF NOT EXISTS idx_wiki_date ON wiki(date)"
    )
    con.commit()

def upsert(con: sqlite3.Connection, rows: List[Row]) -> None:
    init_db(con)
    con.executemany(
        "INSERT OR IGNORE INTO wiki VALUES (?,?,?,?,?)",
        [(r.date, r.section, r.title, r.extra, r.url) for r in rows],
    )
    con.commit()

# ================== AGGREGATION ==================

def export_month(con: sqlite3.Connection, year: int, month: int, out_dir: str) -> None:
    os.makedirs(out_dir, exist_ok=True)

    start = f"{year}-{month:02}-01"
    end_date = (
        dt.date(year, month, 28) + dt.timedelta(days=4)
    ).replace(day=1) - dt.timedelta(days=1)
    end = end_date.isoformat()

    q = """
    SELECT
        title,
        SUM(CAST(substr(extra, instr(extra,'views=')+6) AS INTEGER)) AS views,
        COUNT(*) AS days_present,
        MAX(url) AS url
    FROM wiki
    WHERE section='most_read'
      AND date BETWEEN ? AND ?
    GROUP BY title
    ORDER BY views DESC
    """

    rows = con.execute(q, (start, end)).fetchall()

    items = []
    for title, views, days, url in rows:
        items.append(
            {
                "title": title,
                "views": views,
                "days_present": days,
                "avg_daily_views": round(views / days, 1) if days else 0,
                "url": url,
            }
        )

    data = {
        "schema_version": 1,
        "year": year,
        "month": month,
        "generated_at": dt.datetime.utcnow().isoformat() + "Z",
        "items": items,
    }

    with open(
        os.path.join(out_dir, "most_read.json"),
        "w",
        encoding="utf-8",
    ) as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    summary = {
        "year": year,
        "month": month,
        "total_articles": len(items),
        "top_article": items[0]["title"] if items else None,
        "top_views": items[0]["views"] if items else None,
    }

    with open(os.path.join(out_dir, "summary.json"), "w") as f:
        json.dump(summary, f, indent=2)

# ================== MAIN ==================

def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--lang", default="en")
    ap.add_argument("--db", default="data/wiki.sqlite")
    ap.add_argument("--export_monthlies", action="store_true")
    ap.add_argument("--site_dir", default="site/public/data")
    args = ap.parse_args()

    logger = setup_logger()
    lang = args.lang.split("_")[0]

    os.makedirs(os.path.dirname(args.db) or ".", exist_ok=True)
    con = sqlite3.connect(args.db)

    # ---- Find & ingest latest available day ----
    latest_day = find_latest_available_day(lang, DEFAULT_UA)

    if latest_day is None:
        logger.warning(
        f"No most-read data available in the last {LOOKBACK_DAYS} days. Exiting."
    )
    return
    logger.info(f"Latest available most-read date: {latest_day}")


    payload = fetch_json(
        MOST_READ_DAY_URL.format(
            lang=lang,
            yyyy=latest_day.year,
            mm=f"{latest_day.month:02}",
            dd=f"{latest_day.day:02}",
        ),
        DEFAULT_UA,
    )

    rows = normalize_most_read(latest_day, payload)
    upsert(con, rows)
    logger.info(f"Ingested {len(rows)} articles for {latest_day}")

    # ---- Export monthlies if requested ----
    if args.export_monthlies:
        logger.info("Exporting monthly datasets")
        cur = con.execute("SELECT DISTINCT substr(date,1,7) FROM wiki")
        for (ym,) in cur.fetchall():
            y, m = map(int, ym.split("-"))
            out = os.path.join(args.site_dir, str(y), f"{m:02}")
            export_month(con, y, m, out)
            logger.info(f"Exported {y}-{m:02}")

    con.close()
    logger.info("Done.")

if __name__ == "__main__":
    main()
