# Wikipedia Attention Archive (2026)

This project is a lightweight, automated archive of **what the world
actually read on Wikipedia**, day by day, throughout 2026.

Instead of ranking importance or relevance, it tracks **collective
attention** --- the articles millions of people chose to click,
repeatedly, over time.

The result is a growing dataset and a set of static visualisations that
answer simple but revealing questions:

-   What dominated public attention in a given month?
-   Which topics persisted beyond one-day spikes?
-   How did global interest drift across the year?

------------------------------------------------------------------------

## What this project collects

This archive uses **only officially supported Wikimedia Feed APIs**.

Currently collected data:

-   **Daily "Most Read" Wikipedia articles**
    -   Title
    -   Rank
    -   View count
    -   Article URL

From this, the project derives:

-   Monthly aggregates
-   Persistence metrics (days appeared)
-   Average daily views
-   Year-end summaries

No scraping. No undocumented endpoints. No guesswork.

------------------------------------------------------------------------

## What this project is *not*

-   ❌ Not a real-time dashboard\
-   ❌ Not a news classifier\
-   ❌ Not a measure of importance, quality, or truth

Attention ≠ significance.\
This archive deliberately preserves that ambiguity.

------------------------------------------------------------------------

## How it works

1.  A daily job probes Wikimedia for the **latest available "most-read"
    dataset**
2.  If data exists:
    -   It is ingested into a local SQLite database
    -   Monthly aggregates are updated
3.  If data does not exist yet:
    -   The script exits quietly
    -   No errors, no fake data

The archive grows **naturally as the year unfolds**.

------------------------------------------------------------------------

## Repository structure

    .
    ├── src/
    │   └── ingest.py
    ├── bin/
    │   └── run.sh
    ├── data/
    │   └── wiki.sqlite
    ├── site/
    │   └── public/
    │       └── data/
    │           └── 2026/
    │               └── 01/
    │                   ├── most_read.json
    │                   └── summary.json
    ├── .github/
    │   └── workflows/
    │       └── daily-ingest.yml
    └── README.md

------------------------------------------------------------------------

## Automation

A GitHub Action runs **once per day** and: - Executes the ingestion
script - Commits new data only if something changed - Pushes updates
back to the repository

------------------------------------------------------------------------

## Visualisations

Each month gets a static "hero" visualisation answering:

> *What were Wikipedia's most-read articles this month?*

At the end of 2026, a year-in-review visualisation will summarise how
attention shifted across the entire year.

------------------------------------------------------------------------

## Why this exists

Most analytics projects optimise for prediction and ranking.\
This one optimises for **memory**.

------------------------------------------------------------------------

## Data source & attribution

All data is sourced from the Wikimedia Foundation's public APIs.\
Wikipedia content is licensed under **CC BY-SA**.

------------------------------------------------------------------------

## License

This repository is released under the **MIT License**.
