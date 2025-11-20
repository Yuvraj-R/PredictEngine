from __future__ import annotations

import argparse
from pathlib import Path

import requests
import pandas as pd

BASE_URL = "https://api.elections.kalshi.com/trade-api/v2"
SERIES_TICKER = "KXNBAGAME"


def inspect_event(event_ticker: str) -> None:
    print(f"Inspecting markets for event_ticker={event_ticker}\n")

    # 1) Call the public markets endpoint directly (no SDK, no auth needed for market data)
    url = f"{BASE_URL}/markets"
    params = {
        "series_ticker": SERIES_TICKER,
        "event_ticker": event_ticker,
        "limit": 1000,
    }

    resp = requests.get(url, params=params, timeout=10)
    resp.raise_for_status()
    data = resp.json()

    markets = data.get("markets", [])
    print(markets)
    print(f"Total markets returned: {len(markets)}\n")

    if not markets:
        print("No markets found for this event.")
        return

    # 2) Build a small dataframe with fields we care about for filtering later
    rows = []
    for m in markets:
        rows.append(
            {
                "ticker": m.get("ticker"),
                "event_ticker": m.get("event_ticker"),
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

    df = pd.DataFrame(rows)

    # 3) Print a summary of market types/categories
    print("=== Distinct (market_type, category, sub_category) combos ===")
    print(df[["market_type", "category", "sub_category"]].drop_duplicates(), "\n")

    # 4) Show a sample of markets so we can visually identify the “who will win” ones
    print("=== First few markets (ticker, title, yes/no subtitles) ===")
    with pd.option_context("display.max_colwidth", 120):
        print(
            df[
                [
                    "ticker",
                    "market_type",
                    "title",
                    "yes_sub_title",
                    "no_sub_title",
                ]
            ].head(25)
        )

    # 5) Save full snapshot for deeper manual inspection if needed
    out_dir = Path("src/data/kalshi")
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"inspect_{event_ticker}.csv"
    df.to_csv(out_path, index=False)
    print(f"\nSaved raw markets snapshot to {out_path}")


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Inspect Kalshi markets for a single NBA event_ticker."
    )
    p.add_argument(
        "--event-ticker",
        required=True,
        help="e.g. KXNBAGAME-25OCT21HOUOKC",
    )
    return p.parse_args()


def main() -> None:
    args = parse_args()
    inspect_event(args.event_ticker)


if __name__ == "__main__":
    main()
