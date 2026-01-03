#!/usr/bin/env bash
set -euo pipefail

echo "📚 Wiki Featured ingestion started at $(date)"

python3 src/ingest.py --export_monthlies

echo "✅ Wiki Featured ingestion finished at $(date)"