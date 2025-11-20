# src/data/kalshi/build_nba_events_index.py

from pathlib import Path
import time

import pandas as pd

from .client import get_kalshi_client
from .utils import parse_nba_event_ticker

OUTPUT_PATH = Path("src/data/kalshi/kalshi_events_index.csv")


def fetch_all_nba_events(series_ticker: str = "KXNBAGAME",
                         status: str | None = None,
                         sleep_seconds: float = 0.1) -> pd.DataFrame:
    """
    Fetch ALL NBA game events for a series using cursor-based pagination.
    Returns a DataFrame with one row per event.
    """
    client = get_kalshi_client()

    all_rows = []
    cursor: str | None = None
    page = 0

    while True:
        page += 1
        resp = client.get_events(
            series_ticker=series_ticker,
            limit=200,
            cursor=cursor,
            status=status,  # can be None, "open", "closed", "settled"
        )

        events = resp.events
        print(
            f"Page {page}: fetched {len(events)} events (cursor={resp.cursor})")

        if not events:
            break

        for ev in events:
            event_ticker = getattr(ev, "event_ticker", None)
            title = getattr(ev, "title", None)
            sub_title = getattr(ev, "sub_title", None)
            series = getattr(ev, "series_ticker", None)
            ev_status = getattr(ev, "status", None)

            event_date, away_team, home_team = (None, None, None)
            if event_ticker:
                event_date, away_team, home_team = parse_nba_event_ticker(
                    event_ticker)

            all_rows.append(
                {
                    "event_ticker": event_ticker,
                    "series_ticker": series,
                    "title": title,
                    "sub_title": sub_title,
                    "status": ev_status,
                    "event_date": event_date,
                    "away_team": away_team,
                    "home_team": home_team,
                }
            )

        cursor = resp.cursor
        if not cursor:
            break

        time.sleep(sleep_seconds)

    df = pd.DataFrame(all_rows)

    # Deduplicate by event_ticker just in case
    if "event_ticker" in df.columns:
        df = df.drop_duplicates(subset=["event_ticker"])

    return df


def main():
    df = fetch_all_nba_events(
        series_ticker="KXNBAGAME", status=None, sleep_seconds=0.1)

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(OUTPUT_PATH, index=False)

    print(f"\nTotal events fetched: {len(df)}")
    print(f"Wrote full NBA events index to: {OUTPUT_PATH.resolve()}")
    print("\nHEAD:")
    print(df.head(10))


if __name__ == "__main__":
    main()
