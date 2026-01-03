from __future__ import annotations

import argparse
import datetime as dt
import logging
import os
import sqlite3
import sys
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Tuple

import requests

FEED_URL = "https://api.wikimedia.org/feed/v1/wikipedia/{lang}/featured/{yyyy}/{mm}/{dd}"
DEFAULT_UA = "wiki-featured-crawler/0.1 (personal project; contact: you@example.com)"


@dataclass(frozen=True)
class Row:
    date: str        # YYYY-MM-DD
    section: str     # tfa | news_story | news_link | dyk | onthisday
    title: str
    text: str
    url: str


def setup_logger() -> logging.Logger:
    logger = logging.getLogger("wiki-featured")
    logger.setLevel(logging.INFO)

    handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter(
        "[%(asctime)s] [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    handler.setFormatter(formatter)

    if not logger.handlers:
        logger.addHandler(handler)

    return logger


def _try_import_pandas():
    try:
        import pandas as pd  # type: ignore
        return pd
    except Exception:
        return None


def daterange(start: dt.date, end: dt.date) -> Iterable[dt.date]:
    d = start
    while d <= end:
        yield d
        d += dt.timedelta(days=1)


def parse_date(s: str) -> dt.date:
    return dt.datetime.strptime(s, "%Y-%m-%d").date()


def safe_get(d: Dict[str, Any], *path: str) -> Optional[Any]:
    cur: Any = d
    for p in path:
        if not isinstance(cur, dict) or p not in cur:
            return None
        cur = cur[p]
    return cur


def fetch_featured(lang: str, day: dt.date, user_agent: str) -> Dict[str, Any]:
    url = FEED_URL.format(
        lang=lang,
        yyyy=day.strftime("%Y"),
        mm=day.strftime("%m"),
        dd=day.strftime("%d"),
    )
    headers = {
        "User-Agent": user_agent,
        "Accept": "application/json",
    }
    r = requests.get(url, headers=headers, timeout=30)
    r.raise_for_status()
    return r.json()


def normalize(day: dt.date, payload: Dict[str, Any]) -> List[Row]:
    out: List[Row] = []
    date_s = day.isoformat()

    # 1) Today's featured article (tfa)
    tfa = payload.get("tfa")
    if isinstance(tfa, dict):
        title = (tfa.get("title") or "").strip()
        text = (tfa.get("extract") or tfa.get("description") or "").strip()
        url = (safe_get(tfa, "content_urls", "desktop", "page") or "").strip()
        if title:
            out.append(Row(date_s, "tfa", title, text, url))

    # 2) In the news (news)
    news = payload.get("news")
    if isinstance(news, list):
        for item in news:
            if not isinstance(item, dict):
                continue

            story_text = (item.get("story_text") or "").strip()
            if story_text:
                out.append(Row(date_s, "news_story", "(news story)", story_text, ""))

            links = item.get("links") or []
            if isinstance(links, list):
                for link in links:
                    if not isinstance(link, dict):
                        continue
                    title = (link.get("title") or "").strip()
                    url = (safe_get(link, "content_urls", "desktop", "page") or "").strip()
                    if title:
                        out.append(Row(date_s, "news_link", title, "", url))

    # 3) Did you know (dyk)
    dyk = payload.get("dyk")
    if isinstance(dyk, dict):
        facts = dyk.get("facts")
        if isinstance(facts, list):
            for f in facts:
                if not isinstance(f, dict):
                    continue
                text = (f.get("text") or f.get("html") or "").strip()
                title = (f.get("title") or "(dyk fact)").strip()
                url = (safe_get(f, "content_urls", "desktop", "page") or "").strip()
                if text or title:
                    out.append(Row(date_s, "dyk", title, text, url))
        else:
            content = (dyk.get("content") or dyk.get("text") or "")
            content_s = str(content).strip()
            if content_s:
                out.append(Row(date_s, "dyk", "(dyk)", content_s, ""))

    # 4) On this day (onthisday)
    otd = payload.get("onthisday")
    if isinstance(otd, list):
        for ev in otd:
            if not isinstance(ev, dict):
                continue
            year = ev.get("year")
            text = (ev.get("text") or "").strip()
            title = (f"{year}: {text}" if year else text[:120]).strip() or "(event)"
            out.append(Row(date_s, "onthisday", title, text, ""))

    return out


def init_db(con: sqlite3.Connection) -> None:
    con.execute("""
        CREATE TABLE IF NOT EXISTS featured (
            date TEXT NOT NULL,
            section TEXT NOT NULL,
            title TEXT NOT NULL,
            text TEXT NOT NULL,
            url TEXT NOT NULL,
            PRIMARY KEY (date, section, title, url)
        );
    """)
    con.execute("CREATE INDEX IF NOT EXISTS idx_featured_date ON featured(date);")
    con.commit()


def upsert_rows(con: sqlite3.Connection, rows: List[Row]) -> int:
    init_db(con)
    cur = con.cursor()
    cur.executemany(
        "INSERT OR IGNORE INTO featured(date, section, title, text, url) VALUES (?,?,?,?,?)",
        [(r.date, r.section, r.title, r.text, r.url) for r in rows],
    )
    con.commit()
    return cur.rowcount


def get_max_date_in_db(db_path: str) -> Optional[dt.date]:
    if not os.path.exists(db_path):
        return None
    con = sqlite3.connect(db_path)
    try:
        init_db(con)
        row = con.execute("SELECT MAX(date) FROM featured;").fetchone()
        if not row or row[0] is None:
            return None
        return parse_date(row[0])
    finally:
        con.close()


def compute_catchup_range(db_path: str, floor_start: dt.date, today: dt.date) -> Tuple[dt.date, dt.date]:
    max_d = get_max_date_in_db(db_path)
    if max_d is None:
        return floor_start, today

    next_d = max_d + dt.timedelta(days=1)
    if next_d > today:
        return today, today

    if next_d < floor_start:
        next_d = floor_start
    return next_d, today


def export_outputs(db_path: str, out_dir: str, logger: Optional[logging.Logger] = None) -> None:
    os.makedirs(out_dir, exist_ok=True)
    con = sqlite3.connect(db_path)

    # 1) Daily counts CSV (always)
    q_counts = """
      SELECT date, section, COUNT(*) as count
      FROM featured
      GROUP BY date, section
      ORDER BY date ASC, section ASC
    """
    rows = con.execute(q_counts).fetchall()
    csv_path = os.path.join(out_dir, "featured_daily_counts.csv")
    with open(csv_path, "w", encoding="utf-8") as f:
        f.write("date,section,count\n")
        for d, s, c in rows:
            f.write(f"{d},{s},{c}\n")

    # 2) Latest rows parquet (optional); fallback to CSV if parquet deps missing
    pd = _try_import_pandas()
    if pd is not None:
        df = pd.read_sql_query("SELECT * FROM featured ORDER BY date DESC", con)
        try:
            parquet_path = os.path.join(out_dir, "featured_latest.parquet")
            df.to_parquet(parquet_path, index=False)
        except Exception:
            latest_csv = os.path.join(out_dir, "featured_latest.csv")
            df.to_csv(latest_csv, index=False)

    con.close()

    if logger:
        logger.info(f"Exports written to {out_dir}")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--lang", default="en")

    ap.add_argument("--db", default="data/featured.sqlite")
    ap.add_argument("--out_dir", default="data")
    ap.add_argument("--user_agent", default=os.getenv("WIKI_UA", DEFAULT_UA))

    ap.add_argument("--floor_start", default="2026-01-01", help="Earliest allowed start YYYY-MM-DD")
    ap.add_argument("--end", default=dt.date.today().isoformat(), help="YYYY-MM-DD (usually today)")

    ap.add_argument(
        "--mode",
        choices=["catchup", "range"],
        default="catchup",
        help="catchup: fetch missing days since last DB date; range: use --start/--end",
    )
    ap.add_argument("--start", default=None, help="YYYY-MM-DD (only used when --mode=range)")

    args = ap.parse_args()

    logger = setup_logger()
    logger.info("Starting Wikipedia featured ingestion")

    # Normalize lang: accept en_IN.UTF-8, hi_IN.UTF-8, etc.
    orig_lang = args.lang
    args.lang = (args.lang.split(".")[0]).split("_")[0]
    if args.lang != orig_lang:
        logger.info(f"Normalized language '{orig_lang}' → '{args.lang}'")

    # Define paths early & create dirs once
    db_path = args.db
    out_dir = args.out_dir

    os.makedirs(os.path.dirname(db_path) or ".", exist_ok=True)
    os.makedirs(out_dir, exist_ok=True)

    logger.info(f"DB path: {db_path}")
    logger.info(f"Output directory: {out_dir}")

    floor_start = parse_date(args.floor_start)
    end = parse_date(args.end)
    if end < floor_start:
        raise SystemExit("end date must be >= floor_start")

    if args.mode == "range":
        if not args.start:
            raise SystemExit("--start is required when --mode=range")
        start = parse_date(args.start)
        if start < floor_start:
            start = floor_start
    else:
        start, end = compute_catchup_range(db_path, floor_start, end)

    logger.info(f"Mode: {args.mode}")
    logger.info(f"Fetch range: {start.isoformat()} → {end.isoformat()}")

    all_rows: List[Row] = []
    for day in daterange(start, end):
        logger.info(f"Fetching {args.lang} / {day.isoformat()}")
        try:
            payload = fetch_featured(args.lang, day, args.user_agent)
            rows = normalize(day, payload)
            all_rows.extend(rows)
            logger.info(f"  Parsed {len(rows)} rows")
        except requests.HTTPError as e:
            logger.error(f"  HTTP error for {day}: {e}")
            raise
        except Exception:
            logger.exception(f"  Unexpected error on {day}")
            raise

    con = sqlite3.connect(db_path)
    inserted = upsert_rows(con, all_rows)
    con.close()

    logger.info(f"Rows parsed total: {len(all_rows)}")
    logger.info(f"Rows inserted (new): {inserted}")
    logger.info("Database updated")

    export_outputs(db_path, out_dir, logger)


if __name__ == "__main__":
    main()
