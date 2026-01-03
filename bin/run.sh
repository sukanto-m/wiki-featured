#!/usr/bin/env bash
set -euo pipefail

# --- Config (override via env vars if you want) ---
WIKI_LANG="${WIKI_LANG:-en}"
DB_PATH="${DB_PATH:-data/featured.sqlite}"
OUT_DIR="${OUT_DIR:-data}"
FLOOR_START="${FLOOR_START:-2026-01-01}"
END_DATE="${END_DATE:-$(date +%F)}"

# Use an honest UA. Put your email or repo link.
export WIKI_UA="${WIKI_UA:-wiki-featured-crawler/0.1 (repo: your-github-handle/wiki-featured)}"

# --- Dependencies ---
python3 -m pip -q install --upgrade pip >/dev/null 2>&1 || true
python3 -m pip -q install requests >/dev/null

# Optional, for nicer exports (won't fail the run if unavailable)
python3 -m pip -q install pandas pyarrow >/dev/null 2>&1 || true

# --- Ingest (catch-up mode ensures missed days are backfilled) ---
python3 src/ingest.py \
  --lang "$WIKI_LANG" \
  --db "$DB_PATH" \
  --out_dir "$OUT_DIR" \
  --floor_start "$FLOOR_START" \
  --end "$END_DATE" \
  --mode catchup

# --- Git commit + push (only if changes) ---
git add -A

if git diff --cached --quiet; then
  echo "No changes to commit. Exiting."
  exit 0
fi

STAMP="$(date -u +"%Y-%m-%d %H:%M:%SZ")"
git commit -m "Update Wikipedia featured feed (${LANG}) - ${STAMP}"

git push origin HEAD