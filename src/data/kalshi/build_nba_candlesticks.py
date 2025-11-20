from __future__ import annotations

import argparse
import datetime as dt
import time
from pathlib import Path
from typing import Tuple

import pandas as pd
import requests

BASE_URL = "https://api.elections.kalshi.com/trade-api/v2"
SERIES_TICKER = "KXNBAGAME"


def compute_window_timestamps(
    game_date_str: str,
    pad_before_days: int = 1,
    pad_after_days: int = 2,
) -> Tuple[int, int]:
    """
    Given a game date (YYYY-MM-DD), build a [start_ts, end_ts] window (UTC)
    as Unix timestamps in seconds.

    We pad by:
      - pad_before_days: days before game date (midnight)
      - pad_after_days:  days after game date (midnight)

    This safely covers pre-game and post-game trading.
    """
    game_date = dt.datetime.strptime(game_date_str, "%Y-%m-%d").date()

    start_dt = dt.datetime.combine(
        game_date - dt.timedelta(days=pad_before_days),
        dt.time(0, 0, tzinfo=dt.timezone.utc),
    )
    end_dt = dt.datetime.combine(
        game_date + dt.timedelta(days=pad_after_days),
        dt.time(0, 0, tzinfo=dt.timezone.utc),
    )

    return int(start_dt.timestamp()), int(end_dt.timestamp())


def fetch_candlesticks_for_market(
    market_ticker: str,
    start_ts: int,
    end_ts: int,
    period_interval: int,
) -> list[dict]:
    """
    Call Kalshi's Get Market Candlesticks endpoint for a single market.
    Ref: GET /series/{series_ticker}/markets/{ticker}/candlesticks
    """
    url = f"{BASE_URL}/series/{SERIES_TICKER}/markets/{market_ticker}/candlesticks"
    params = {
        "start_ts": start_ts,
        "end_ts": end_ts,
        "period_interval": period_interval,  # minutes: 1, 60, or 1440
    }

    resp = requests.get(url, params=params, timeout=15)
    resp.raise_for_status()
    data = resp.json()

    return data.get("candlesticks", []) or []


def build_candlesticks_for_season(
    season_label: str,
    markets_path: Path,
    candles_dir: Path,
    period_interval: int = 1,
    limit_markets: int | None = None,
    sleep_seconds: float = 0.1,
) -> None:
    """
    For a given season, read nba_markets_<season>.csv (two winner markets per game),
    and fetch candlesticks for each unique market ticker.

    Output: one CSV per market:
        candles/<ticker>.csv
    """
    print(f"Loading markets from: {markets_path}")
    markets_df = pd.read_csv(markets_path, dtype={"game_id": str})

    # We expect two rows per game_id (home/away), but dedup by ticker just in case.
    markets_df = markets_df.dropna(subset=["ticker"])
    markets_df = markets_df.drop_duplicates(
        subset=["ticker"]).reset_index(drop=True)

    if limit_markets is not None:
        markets_df = markets_df.head(limit_markets)

    total_markets = len(markets_df)
    print(
        f"Found {total_markets} unique market tickers to fetch candlesticks for.")

    candles_dir.mkdir(parents=True, exist_ok=True)

    for idx, row in markets_df.iterrows():
        market_ticker = row["ticker"]
        game_id = row["game_id"]
        game_date = row["game_date"]
        home_team = row["home_team"]
        away_team = row["away_team"]

        candles_out = candles_dir / f"{market_ticker}.csv"

        print(
            f"\n[{idx+1}/{total_markets}] "
            f"GAME_ID={game_id} {away_team} @ {home_team} ({game_date}) "
            f"market_ticker={market_ticker}"
        )

        if candles_out.exists():
            print(f"  [skip] {candles_out} already exists")
            continue

        # Compute time window around this game
        start_ts, end_ts = compute_window_timestamps(game_date)

        print(f"  Fetching candlesticks: start_ts={start_ts}, end_ts={end_ts}, "
              f"period_interval={period_interval} minutes")

        try:
            candlesticks = fetch_candlesticks_for_market(
                market_ticker=market_ticker,
                start_ts=start_ts,
                end_ts=end_ts,
                period_interval=period_interval,
            )
        except Exception as e:  # noqa: BLE001
            print(f"  !! Error fetching candlesticks for {market_ticker}: {e}")
            time.sleep(sleep_seconds)
            continue

        if not candlesticks:
            print("  -> No candlesticks returned.")
            time.sleep(sleep_seconds)
            continue

        # Flatten candlesticks into rows
        rows: list[dict] = []
        for c in candlesticks:
            yes_bid = c.get("yes_bid") or {}
            yes_ask = c.get("yes_ask") or {}
            price = c.get("price") or {}

            end_ts_c = c.get("end_period_ts")
            end_iso = (
                dt.datetime.fromtimestamp(
                    end_ts_c, tz=dt.timezone.utc).isoformat()
                if isinstance(end_ts_c, (int, float))
                else None
            )

            rows.append(
                {
                    "series_ticker": SERIES_TICKER,
                    "season_label": season_label,
                    "game_id": game_id,
                    "game_date": game_date,
                    "home_team": home_team,
                    "away_team": away_team,
                    "event_ticker": row["event_ticker"],
                    "market_ticker": market_ticker,
                    "end_period_ts": end_ts_c,
                    "end_period_iso": end_iso,
                    # YES bid OHLC
                    "yes_bid_open": yes_bid.get("open"),
                    "yes_bid_low": yes_bid.get("low"),
                    "yes_bid_high": yes_bid.get("high"),
                    "yes_bid_close": yes_bid.get("close"),
                    # YES ask OHLC
                    "yes_ask_open": yes_ask.get("open"),
                    "yes_ask_low": yes_ask.get("low"),
                    "yes_ask_high": yes_ask.get("high"),
                    "yes_ask_close": yes_ask.get("close"),
                    # Price OHLC & mean
                    "price_open": price.get("open"),
                    "price_low": price.get("low"),
                    "price_high": price.get("high"),
                    "price_close": price.get("close"),
                    "price_mean": price.get("mean"),
                    # Volume / OI
                    "volume": c.get("volume"),
                    "open_interest": c.get("open_interest"),
                }
            )

        candles_df = pd.DataFrame(rows)
        candles_df.to_csv(candles_out, index=False)
        print(f"  -> Saved {len(candles_df)} rows to {candles_out}")

        time.sleep(sleep_seconds)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Fetch Kalshi candlesticks for NBA winner markets in a season."
    )
    parser.add_argument(
        "--season-label",
        required=True,
        help='Season label, e.g. "2025-26".',
    )
    parser.add_argument(
        "--markets-path",
        required=False,
        help="Path to nba_markets_<season>.csv "
             "(default: src/data/kalshi/nba_markets_<season>.csv).",
    )
    parser.add_argument(
        "--candles-dir",
        required=False,
        help="Directory to write candlestick CSVs "
             "(default: src/data/kalshi/candles).",
    )
    parser.add_argument(
        "--period-interval",
        type=int,
        default=1,
        choices=[1, 60, 1440],
        help="Candlestick length in minutes (1, 60, 1440). Default: 1.",
    )
    parser.add_argument(
        "--limit-markets",
        type=int,
        default=None,
        help="Limit number of markets (for testing).",
    )
    parser.add_argument(
        "--sleep",
        type=float,
        default=0.1,
        help="Sleep between API calls.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    season_tag = args.season_label.replace("-", "_")

    default_markets_path = Path("src/data/kalshi") / \
        f"nba_markets_{season_tag}.csv"
    markets_path = Path(
        args.markets_path) if args.markets_path else default_markets_path

    default_candles_dir = Path("src/data/kalshi") / "candles"
    candles_dir = Path(
        args.candles_dir) if args.candles_dir else default_candles_dir

    build_candlesticks_for_season(
        season_label=args.season_label,
        markets_path=markets_path,
        candles_dir=candles_dir,
        period_interval=args.period_interval,
        limit_markets=args.limit_markets,
        sleep_seconds=args.sleep,
    )


if __name__ == "__main__":
    main()
