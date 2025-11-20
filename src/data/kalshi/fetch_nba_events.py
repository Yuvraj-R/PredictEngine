from pathlib import Path
from datetime import datetime
import pandas as pd

from .client import get_kalshi_client

OUTPUT_PATH = Path("src/data/kalshi/kalshi_events_sample.csv")


def parse_nba_event_ticker(event_ticker: str):
    """
    Parse event_ticker like:
      KXNBAGAME-25NOV22NYKORL

    into:
      event_date = '2025-11-22'
      away_team = 'NYK'
      home_team = 'ORL'

    Format is: YYMMMDD + AWAY + HOME
      YY   = season year suffix (25 -> 2025)
      MMM  = month (NOV)
      DD   = day of month (22)
    """
    try:
        # Strip prefix, keep suffix like "25NOV22NYKORL"
        suffix = event_ticker.split("-", 1)[1]
        date_str = suffix[:7]       # "25NOV22"
        teams_str = suffix[7:]      # "NYKORL"

        # Parse YY MMM DD
        year_suffix = int(date_str[:2])   # 25
        month_abbr = date_str[2:5]        # "NOV"
        day = int(date_str[5:])           # 22

        year = 2000 + year_suffix         # 25 -> 2025

        # Build something datetime can parse, e.g. "22NOV2025"
        date_token = f"{day:02d}{month_abbr}{year}"
        date_dt = datetime.strptime(date_token.upper(), "%d%b%Y")
        event_date = date_dt.date().isoformat()  # "2025-11-22"

        away_team = teams_str[:3]
        home_team = teams_str[3:]

        return event_date, away_team, home_team

    except Exception:
        return None, None, None


def fetch_nba_events(series_ticker: str = "KXNBAGAME", limit: int = 10) -> pd.DataFrame:
    client = get_kalshi_client()

    # Get a small sample for inspection
    response = client.get_events(series_ticker=series_ticker, limit=limit)
    events = response.events

    # Extra visibility (you already added this, keep it if you like)
    print(events[0])

    rows = []
    for ev in events:
        event_ticker = getattr(ev, "event_ticker", None)
        title = getattr(ev, "title", None)
        sub_title = getattr(ev, "sub_title", None)
        series = getattr(ev, "series_ticker", None)
        status = getattr(ev, "status", None)

        event_date, away_team, home_team = (None, None, None)
        if event_ticker:
            event_date, away_team, home_team = parse_nba_event_ticker(
                event_ticker)

        rows.append(
            {
                "event_ticker": event_ticker,
                "series_ticker": series,
                "title": title,
                "sub_title": sub_title,
                "status": status,
                "event_date": event_date,
                "away_team": away_team,
                "home_team": home_team,
            }
        )

    df = pd.DataFrame(rows)
    return df


def main():
    df = fetch_nba_events(series_ticker="KXNBAGAME", limit=100)

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(OUTPUT_PATH, index=False)

    print(f"Wrote {len(df)} events to {OUTPUT_PATH.resolve()}")
    print("\nHEAD:")
    print(df.head(10))


if __name__ == "__main__":
    main()
