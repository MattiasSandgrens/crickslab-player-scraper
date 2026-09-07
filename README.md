# Crickslab Match Central — API-First Sports Data Pipeline

Data pipeline that extracts, cleans, and stores large-scale sports data: **701,973 player profiles configured across 8 countries**, without HTML parsing. *(Full career batting/bowling/fielding stats per player — not just listing data.)*

## The problem

Traditional scraping against sites built with modern frontend frameworks (Next.js, in this case) is brittle: layout changes break selectors, and the site's own listing page caps what you can page through in the browser — `/match-central/players` always returns the same first 20 players in HTML no matter what page you request.

## The approach

Instead of parsing rendered HTML, I analyzed the site's network traffic and found the internal JSON endpoints the frontend itself calls — one to list players by country, one per player for full career stats. The pipeline talks to those endpoints directly, which gives:

- **Correct pagination:** bypasses the frontend's broken browser-side paging entirely, since the API itself pages correctly even though the rendered HTML doesn't.
- **Resilience:** unaffected by frontend layout or design changes, since the contract is the JSON schema, not the HTML structure.

## Architecture

```
[Network analysis] → [Sequential API client] → [Raw JSON] → [Typed casting] → [SQLite]
```

- **Collection:** a pooled `requests.Session` with an automatic retry adapter (5 attempts, backoff) on 500/502/503/504. Requests are deliberately sequential rather than concurrent, one country at a time (smallest first), with small fixed delays between calls (0.5s between listing pages, 0.3s between per-player stats requests) — a conscious choice to stay well under the API's rate limit rather than maximize raw throughput.
- **Rate limiting:** on a 429, the client stops and waits out a fixed 10-minute cooldown before retrying the same request, instead of failing the run.
- **Resume / fault tolerance:** a `scrape_progress` table (one row per country) tracks which listing page to resume from; the `players` table's primary key doubles as free deduplication, since "who's already done" is just a query against it. An interrupted run — Ctrl+C, a network error, or hitting the per-run budget below — picks up exactly where it left off.
- **Run budget:** each invocation stops after a capped number of new players (200 by default) rather than running to completion. This is what makes scraping Pakistan's ~635,000 players practical: instead of one ~2-day process, it's many short, resumable runs.
- **Data integrity:** fields are extracted defensively (`.get()` with defaults, typed casting that falls back to `None` on a bad value) rather than validated against a strict schema — a missing or renamed field becomes a `NULL` in the database instead of crashing a multi-hour run. That trades early detection of upstream API changes for an uninterrupted long-running job; the natural next step would be logging when an expected field is entirely absent, to surface schema drift without stopping the pipeline.
- **Storage:** SQLite — a `players` table (one row per player, ~35 typed stat columns spanning batting/bowling/fielding) and the `scrape_progress` table above.

## Tech stack

Python 3 · `requests` (with a `urllib3` retry adapter) · `sqlite3` (standard library)

## Running it

```
pip install requests
python scraper.py
```
Each run stops after 200 new players by default (`MAX_NEW_PLAYERS_PER_RUN`) and picks up where it left off next time — run it again, or schedule it (cron, a scheduled task), to keep going until every configured country is fully scraped.

## Results

- 701,973 player profiles configured across 8 countries (Nepal, Afghanistan, Qatar, Sri Lanka, Kuwait, Bangladesh, India, Pakistan)
- The 7 smaller countries (~67,000 players combined) complete in roughly 6–7 hours
- Pakistan alone accounts for ~635,000 of those profiles — at ~0.3s per stats request that's close to two full days of requests, so it's deliberately spread across many capped runs instead of one continuous job

## Contact

Open to freelance work in Python backend development, data pipelines, and API integrations.

- LinkedIn: [linkedin.com/in/mattias-sandgren-5560a0430](https://www.linkedin.com/in/mattias-sandgren-5560a0430/)
- Fiverr: [fiverr.com/users/mattias_sa](https://www.fiverr.com/users/mattias_sa/)
