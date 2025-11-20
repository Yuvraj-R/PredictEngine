from __future__ import annotations

import argparse
import time
from pathlib import Path

import pandas as pd
import requests

BASE_URL = "https://api.elections.kalshi.com/trade-api/v2"
SERIES_TICKER = "KXNBAGAME"


def get_markets_for_event(event_ticker: str) -> list[dict]:
    """
    Call Kalshi's /markets endpoint for a single NBA event_ticker and
    return the raw list of market dicts.

    We use raw HTTP (requests) instead of the SDK to avoid Pydantic enum issues.
    """
    markets: list[dict] = []
    cursor: str | None = None

    while True:
        params = {
            "series_ticker": SERIES_TICKER,
            "event_ticker": event_ticker,
            "limit": 1000,
        }
        if cursor:
            params["cursor"] = cursor

        resp = requests.get(
            f"{BASE_URL}/markets",
            params=params,
            timeout=10,
        )
        resp.raise_for_status()
        data = resp.json()

        batch = data.get("markets", []) or []
        markets.extend(batch)

        cursor = data.get("cursor")
        if not cursor:
            break

    return markets


def build_markets_index_for_season(
    season_label: str,
    map_path: Path,
    out_path: Path,
    limit_events: int | None = None,
    sleep_seconds: float = 0.2,
) -> None:
    """
    Read the merged NBA–Kalshi mapping CSV (one row per game/event),
    call /markets for each unique event_ticker, and write a single
    season-level markets CSV.
    """
    print(f"Loading mapping from: {map_path}")
    mapping_df = pd.read_csv(map_path, dtype={"GAME_ID": str})
    mapping_df = mapping_df.dropna(subset=["event_ticker"])

    # Unique games / events
    events_df = (
        mapping_df[
            [
                "GAME_ID",
                "GAME_DATE",
                "HOME_TEAM_ABBREV",
                "AWAY_TEAM_ABBREV",
                "SEASON_TYPE",
                "event_ticker",
                "title",
                "sub_title",
                "status",
            ]
        ]
        .drop_duplicates(subset=["event_ticker"])
        .reset_index(drop=True)
    )

    if limit_events is not None:
        events_df = events_df.head(limit_events)

    total_events = len(events_df)
    print(f"Found {total_events} unique events to fetch markets for.")

    rows: list[dict] = []

    for idx, row in events_df.iterrows():
        game_id = row["GAME_ID"]
        game_date = row["GAME_DATE"]
        home_team = row["HOME_TEAM_ABBREV"]
        away_team = row["AWAY_TEAM_ABBREV"]
        season_type = row["SEASON_TYPE"]
        event_ticker = row["event_ticker"]

        print(
            f"\n[{idx+1}/{total_events}] GAME_ID={game_id} "
            f"{away_team} @ {home_team} ({game_date}) "
            f"event_ticker={event_ticker}"
        )

        try:
            markets = get_markets_for_event(event_ticker)
        except Exception as e:  # noqa: BLE001
            print(f"  !! Error fetching markets for {event_ticker}: {e}")
            continue

        print(f"  -> {len(markets)} markets returned")

        if not markets:
            continue

        for m in markets:
            rows.append(
                {
                    # Game / season context
                    "season_label": season_label,
                    "season_type": season_type,
                    "game_id": game_id,
                    "game_date": game_date,
                    "home_team": home_team,
                    "away_team": away_team,
                    "event_ticker": event_ticker,
                    # Market fields from API
                    "ticker": m.get("ticker"),
                    "market_type": m.get("market_type"),
                    "category": m.get("category"),
                    "sub_category": m.get("sub_category"),
                    "title": m.get("title"),
                    "subtitle": m.get("subtitle"),
                    "yes_sub_title": m.get("yes_sub_title"),
                    "no_sub_title": m.get("no_sub_title"),
                    "status": m.get("status"),
                }
            )

        time.sleep(sleep_seconds)

    if not rows:
        print("\nNo market rows collected; nothing to write.")
        return

    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_df = pd.DataFrame(rows)
    out_df.to_csv(out_path, index=False)

    print(f"\nWrote {len(out_df)} rows to {out_path}")
    print("Columns:", list(out_df.columns))
    print(out_df.head())


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Build NBA markets index for a season from merged mapping."
    )
    p.add_argument(
        "--season-label",
        required=True,
        help='Season label for metadata, e.g. "2024-25" or "2025-26".',
    )
    p.add_argument(
        "--map-path",
        required=True,
        help="Path to merged nba_kalshi_game_map_*.csv.",
    )
    p.add_argument(
        "--out-path",
        required=False,
        help="Output CSV path (default: src/data/kalshi/nba_markets_<season>.csv).",
    )
    p.add_argument(
        "--limit-events",
        type=int,
        default=None,
        help="Limit number of events/games for testing.",
    )
    p.add_argument(
        "--sleep",
        type=float,
        default=0.2,
        help="Sleep between event API calls.",
    )
    return p.parse_args()


def main() -> None:
    args = parse_args()

    season_tag = args.season_label.replace("-", "_")
    default_out = Path("src/data/kalshi") / f"nba_markets_{season_tag}.csv"

    out_path = Path(args.out_path) if args.out_path else default_out

    build_markets_index_for_season(
        season_label=args.season_label,
        map_path=Path(args.map_path),
        out_path=out_path,
        limit_events=args.limit_events,
        sleep_seconds=args.sleep,
    )


if __name__ == "__main__":
    main()
