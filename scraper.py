"""Fetch players and their full career stats from Crickslab Match Central.

The site is a Next.js app that paginates in the browser: the HTML response for
/match-central/players always contains the first 20 players, regardless of
?page=. So we call the API the page itself uses instead. For each player we
then hit their individual player-stats endpoint (the same one their public
profile page uses) to get full batting/bowling/fielding career stats.

Everything is stored in a single SQLite database (DEFAULT_DB): a `players`
table holds the scraped data, and a `scrape_progress` table tracks, per
country, which listing page to resume from. That makes the scrape resumable
for free - an interrupted run (Ctrl+C, a network error, or simply the
per-run player budget below) can be continued by running the script again,
since "which players are already done" is just a query against `players`
instead of a separate file to keep in sync.

Country sizes vary hugely (checked via the API on 2026-09-06):
  Nepal 1,255 | Afghanistan 1,717 | Qatar 4,139 | Sri Lanka 5,561 |
  Kuwait 7,944 | Bangladesh 16,513 | India 29,854 | Pakistan 634,990
Pakistan alone is ~635k players; at ~0.3s per stats request that is roughly
two full days of requests. MAX_NEW_PLAYERS_PER_RUN below keeps any single
invocation short - just run the script again (or schedule it) to keep going.
"""

import sqlite3
import sys
import time

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

LIST_URL = "https://apis.crickslab.com/v1/players/filter"
STATS_URL = "https://apis.crickslab.com/v1/players/{uuid}/player-stats/{slug}"
PROFILE_URL = "https://crickslab.com/match-central/player-details/{id}/{slug}/{uuid}/overview"
PER_PAGE = 20  # the server caps this at 20 no matter what we ask for
LIST_DELAY = 0.5   # seconds between listing pages
STATS_DELAY = 0.3  # seconds between per-player stats requests
RATE_LIMIT_COOLDOWN = 600  # seconds to wait after a 429 before retrying

DEFAULT_DB = "cricket_players.db"

# Smallest countries first, so a run of modest length completes a few
# countries instead of getting stuck partway through Pakistan.
COUNTRIES = [
    {"id": 154, "name": "Nepal"},
    {"id": 1, "name": "Afghanistan"},
    {"id": 179, "name": "Qatar"},
    {"id": 208, "name": "Sri Lanka"},
    {"id": 117, "name": "Kuwait"},
    {"id": 19, "name": "Bangladesh"},
    {"id": 101, "name": "India"},
    {"id": 167, "name": "Pakistan"},
]

# Stop after writing this many new players in a single run (None = no limit,
# run until every configured country is fully scraped).
MAX_NEW_PLAYERS_PER_RUN = 200

# (dict key from parse_player/parse_stats, database column, caster)
COLUMNS = [
    ("Id", "id", int),
    ("Name", "name", str),
    ("Nationality", "nationality", str),
    ("CountryCode", "country_code", str),
    ("City", "city", str),
    ("Role", "role", str),
    ("BattingStyle", "batting_style", str),
    ("BowlingStyle", "bowling_style", str),
    ("WicketKeeper", "wicket_keeper", lambda v: int(bool(v))),
    ("Teams", "teams", str),
    ("URL", "url", str),
    ("Matches", "matches", int),
    ("Wins", "wins", int),
    ("Losses", "losses", int),
    ("BatInnings", "bat_innings", int),
    ("BatRuns", "bat_runs", int),
    ("BatAverage", "bat_average", float),
    ("BatStrikeRate", "bat_strike_rate", float),
    ("BatHighestRuns", "bat_highest_runs", int),
    ("BatHundreds", "bat_hundreds", int),
    ("BatFifties", "bat_fifties", int),
    ("BatFours", "bat_fours", int),
    ("BatSixes", "bat_sixes", int),
    ("BatNotOuts", "bat_not_outs", int),
    ("BatDucks", "bat_ducks", int),
    ("BowlInnings", "bowl_innings", int),
    ("BowlWickets", "bowl_wickets", int),
    ("BowlEconomy", "bowl_economy", float),
    ("BowlAverage", "bowl_average", float),
    ("BowlStrikeRate", "bowl_strike_rate", float),
    ("BowlBestFigure", "bowl_best_figure", str),
    ("BowlOvers", "bowl_overs", float),
    ("BowlFourWickets", "bowl_four_wickets", int),
    ("BowlFiveWickets", "bowl_five_wickets", int),
    ("FieldCatches", "field_catches", int),
    ("FieldRunOuts", "field_run_outs", int),
    ("FieldStumpings", "field_stumpings", int),
]

_SQL_TYPES = {int: "INTEGER", float: "REAL", str: "TEXT"}


class RunBudgetReached(Exception):
    """Raised internally to unwind out of the country/page loops cleanly."""


def _cast(value, caster):
    if value is None:
        return None
    try:
        return caster(value)
    except (TypeError, ValueError):
        return None


def get_connection(db_path=DEFAULT_DB):
    conn = sqlite3.connect(db_path)

    column_defs = ", ".join(
        f"{col} {_SQL_TYPES.get(caster, 'TEXT')}" if key != "Id" else f"{col} INTEGER PRIMARY KEY"
        for key, col, caster in COLUMNS
    )
    conn.execute(f"CREATE TABLE IF NOT EXISTS players ({column_defs})")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS scrape_progress (
            country_id INTEGER PRIMARY KEY,
            country_name TEXT NOT NULL,
            list_page INTEGER NOT NULL DEFAULT 1,
            completed INTEGER NOT NULL DEFAULT 0
        )
    """)
    conn.commit()
    return conn


def load_done_ids(conn):
    return {row[0] for row in conn.execute("SELECT id FROM players")}


def get_progress(conn, country_id):
    """Return (list_page, completed) for a country, defaulting to (1, False)."""
    row = conn.execute(
        "SELECT list_page, completed FROM scrape_progress WHERE country_id = ?", (country_id,)
    ).fetchone()
    return (row[0], bool(row[1])) if row else (1, False)


def set_progress(conn, country_id, country_name, list_page, completed):
    conn.execute("""
        INSERT INTO scrape_progress (country_id, country_name, list_page, completed)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(country_id) DO UPDATE SET list_page = excluded.list_page, completed = excluded.completed
    """, (country_id, country_name, list_page, int(completed)))
    conn.commit()


def insert_player(conn, row):
    columns = [col for _, col, _ in COLUMNS]
    values = [_cast(row.get(key), caster) for key, _, caster in COLUMNS]
    placeholders = ", ".join("?" * len(columns))
    conn.execute(
        f"INSERT OR REPLACE INTO players ({', '.join(columns)}) VALUES ({placeholders})", values
    )
    conn.commit()


def make_session():
    session = requests.Session()
    session.headers.update({
        "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                       "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"),
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Origin": "https://crickslab.com",
        "Referer": "https://crickslab.com/",
    })
    # 429 is handled separately with a long cooldown (see request_with_cooldown),
    # so it's deliberately left out of this short automatic retry.
    retry = Retry(total=5, backoff_factor=1, status_forcelist=[500, 502, 503, 504],
                  allowed_methods=["GET", "POST"])
    session.mount("https://", HTTPAdapter(max_retries=retry))
    return session


def request_with_cooldown(request_fn):
    """Call request_fn() repeatedly, waiting out RATE_LIMIT_COOLDOWN whenever the
    server returns 429, instead of giving up like a normal error would."""
    while True:
        response = request_fn()
        if response.status_code == 429:
            minutes = RATE_LIMIT_COOLDOWN // 60
            print(f"Rate limited (429), cooling down for {minutes} minutes...", file=sys.stderr)
            time.sleep(RATE_LIMIT_COOLDOWN)
            continue
        return response


def fetch_page(session, page, filters):
    """Return (list of raw player objects, total number of pages)."""
    payload = {"search_type": "FROM_PLATFORM", **filters}
    response = request_with_cooldown(
        lambda: session.post(f"{LIST_URL}?page={page}&per_page={PER_PAGE}", json=payload, timeout=30)
    )
    response.raise_for_status()
    body = response.json()
    return body.get("data", []), body.get("meta", {}).get("last_page", 1)


def fetch_player_stats(session, uuid, slug):
    """Return the player's raw performances dict, or None if unavailable."""
    url = STATS_URL.format(uuid=uuid, slug=slug)
    response = request_with_cooldown(lambda: session.get(url, timeout=30))
    response.raise_for_status()
    return response.json().get("performances")


def parse_player(raw):
    """Pick the listing fields we care about out of the API object."""
    country = raw.get("country") or {}
    city = raw.get("city") or {}
    player_type = raw.get("playerType") or {}
    batting = raw.get("battingType") or {}
    bowling = raw.get("bowlingType") or {}
    # teams also holds a {"type": "more", "title": "+9"} entry when the list is truncated
    teams = [t.get("title") for t in raw.get("teams") or [] if t.get("type") == "team"]

    return {
        "Id": raw.get("id"),
        "Name": raw.get("displayName") or raw.get("name"),
        "Nationality": country.get("name"),
        "CountryCode": country.get("code"),
        "City": city.get("name"),
        "Role": player_type.get("title"),
        "BattingStyle": batting.get("name"),
        "BowlingStyle": bowling.get("name"),
        "WicketKeeper": bool(raw.get("isWicketKeeper")),
        "Teams": "; ".join(teams),
        "URL": PROFILE_URL.format(id=raw.get("id"), slug=raw.get("slug"), uuid=raw.get("uuid")),
    }


def parse_stats(performances):
    """Flatten the performances dict from player-stats into row fields."""
    performances = performances or {}
    bat = performances.get("batting") or {}
    bowl = performances.get("bowling") or {}
    field = performances.get("fielding") or {}

    return {
        "Matches": performances.get("matches"),
        "Wins": performances.get("win"),
        "Losses": performances.get("loss"),
        "BatInnings": bat.get("innings"),
        "BatRuns": bat.get("runs"),
        "BatAverage": bat.get("avg"),
        "BatStrikeRate": bat.get("strikeRate"),
        "BatHighestRuns": bat.get("highestRuns"),
        "BatHundreds": bat.get("hundreds"),
        "BatFifties": bat.get("fifties"),
        "BatFours": bat.get("fours"),
        "BatSixes": bat.get("sixes"),
        "BatNotOuts": bat.get("notOutCount"),
        "BatDucks": bat.get("duckCount"),
        "BowlInnings": bowl.get("innings"),
        "BowlWickets": bowl.get("wickets"),
        "BowlEconomy": bowl.get("economy"),
        "BowlAverage": bowl.get("average"),
        "BowlStrikeRate": bowl.get("strikeRate"),
        "BowlBestFigure": bowl.get("bestBowlingFigure"),
        "BowlOvers": bowl.get("totalOvers"),
        "BowlFourWickets": bowl.get("fourWickets"),
        "BowlFiveWickets": bowl.get("fiveWickets"),
        "FieldCatches": field.get("catches"),
        "FieldRunOuts": field.get("runOut"),
        "FieldStumpings": field.get("stumped"),
    }


class _Budget:
    def __init__(self, limit):
        self.limit = limit
        self.count = 0


def scrape_country(session, conn, country, page, done_ids, budget):
    """Walk every listing page for one country, storing new players as we go."""
    print(f"=== {country['name']} (id={country['id']}) ===")
    while True:
        raw_players, last_page = fetch_page(session, page, {"country_id": country["id"]})
        if not raw_players:
            break

        new_count = 0
        for raw in raw_players:
            if raw["id"] in done_ids:
                continue
            row = parse_player(raw)
            performances = fetch_player_stats(session, raw["uuid"], raw["slug"])
            row.update(parse_stats(performances))

            insert_player(conn, row)
            done_ids.add(raw["id"])
            new_count += 1

            if budget.limit is not None and budget.count + new_count >= budget.limit:
                budget.count += new_count
                set_progress(conn, country["id"], country["name"], page, completed=False)
                print(f"{country['name']} page {page}/{last_page}: {new_count} new players "
                      f"({len(done_ids)} total so far)")
                raise RunBudgetReached

            time.sleep(STATS_DELAY)

        budget.count += new_count
        print(f"{country['name']} page {page}/{last_page}: {new_count} new players "
              f"({len(done_ids)} total so far)")

        if page >= last_page:
            break
        page += 1
        set_progress(conn, country["id"], country["name"], page, completed=False)
        time.sleep(LIST_DELAY)


def run(db_path=DEFAULT_DB, countries=None, max_new_players_per_run=MAX_NEW_PLAYERS_PER_RUN):
    """Scrape players country by country, resuming from the database if it exists.

    Stops after max_new_players_per_run new players (None = run until every
    country in `countries` is fully scraped). Run the script again to continue.
    """
    countries = countries if countries is not None else COUNTRIES
    session = make_session()
    conn = get_connection(db_path)
    done_ids = load_done_ids(conn)
    budget = _Budget(max_new_players_per_run)

    try:
        try:
            for country in countries:
                page, completed = get_progress(conn, country["id"])
                if completed:
                    continue

                try:
                    scrape_country(session, conn, country, page, done_ids, budget)
                except requests.RequestException as exc:
                    print(f"Request failed, stopping so the run can resume later: {exc}", file=sys.stderr)
                    return len(done_ids)

                set_progress(conn, country["id"], country["name"], 1, completed=True)
        except RunBudgetReached:
            print(f"\nStopped after {budget.count} new players this run "
                  f"({len(done_ids)} total). Run the script again to continue.")
            return len(done_ids)
    finally:
        conn.close()

    print(f"\nAll countries done: {len(done_ids)} players stored in {db_path}")
    return len(done_ids)


if __name__ == "__main__":
    run()
