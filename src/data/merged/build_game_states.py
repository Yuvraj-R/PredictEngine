from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Dict, List, Any

import pandas as pd
import numpy as np
import datetime as dt


# =====================================================================
# Helpers
# =====================================================================

def to_native(x):
    """
    Convert numpy/pandas scalars to JSON-safe Python types.
    Recursively handles lists and dicts.
    """

    # Preserve None
    if x is None:
        return None

    # Recurse for lists
    if isinstance(x, list):
        return [to_native(v) for v in x]

    # Recurse for dicts
    if isinstance(x, dict):
        return {k: to_native(v) for k, v in x.items()}

    # Simple pandas/numpy NaN → None (only for scalars)
    try:
        if isinstance(x, (float, int)) and pd.isna(x):
            return None
    except Exception:
        pass

    # numpy scalar → python scalar
    if isinstance(x, (np.integer,)):
        return int(x)

    if isinstance(x, (np.floating,)):
        return float(x)

    # numpy bool_ → python bool
    if isinstance(x, (np.bool_)):
        return bool(x)

    # timestamps → ISO string
    if isinstance(x, (pd.Timestamp, dt.datetime)):
        return x.isoformat()

    # Python native types stay as-is
    if isinstance(x, (int, float, str, bool)):
        return x

    # Fallback: string, but this should rarely hit now
    return str(x)


def normalize_price(x):
    """Convert Kalshi 0–100 price to probability 0–1."""
    try:
        return float(x) / 100.0
    except Exception:
        return None


# =====================================================================
# Loading Data
# =====================================================================

def load_nba_timeline(season: str, game_id: str) -> pd.DataFrame:
    season_dir = season.replace("-", "_")
    path = Path("src/data/nba/game_states") / \
        season_dir / f"game_{game_id}.csv"

    if not path.exists():
        raise FileNotFoundError(f"NBA timeline not found: {path}")

    df = pd.read_csv(path)
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
    df = df.sort_values("timestamp").reset_index(drop=True)
    return df


def load_market_tick_data(market_ticker: str) -> pd.DataFrame:
    path = Path("src/data/kalshi/candles") / f"{market_ticker}.csv"
    if not path.exists():
        raise FileNotFoundError(f"Candlestick file not found: {path}")

    df = pd.read_csv(path)
    df["end_period_iso"] = pd.to_datetime(df["end_period_iso"], utc=True)
    df = df.sort_values("end_period_iso").reset_index(drop=True)
    return df


# =====================================================================
# Alignment
# =====================================================================

def align_candles_to_timeline(
    timeline: pd.DataFrame,
    cand_df: pd.DataFrame
) -> List[Any]:
    """
    Align candlesticks to timeline timestamps using forward fill logic.
    For each timeline row, choose the last candle whose end_period_iso <= timestamp.
    """

    aligned = []
    candle_idx = 0
    n = len(cand_df)

    for _, row in timeline.iterrows():
        t = row["timestamp"]

        # Advance candle pointer while next candle <= t
        while candle_idx + 1 < n and cand_df.loc[candle_idx + 1, "end_period_iso"] <= t:
            candle_idx += 1

        # If even the current candle is after t → no candle yet
        if cand_df.loc[candle_idx, "end_period_iso"] > t:
            aligned.append(None)
        else:
            aligned.append(cand_df.loc[candle_idx])

    return aligned


# =====================================================================
# Market Entry Builder
# =====================================================================

def build_market_entry(
    candle_row: pd.Series,
    market_ticker: str,
    home_team: str,
    away_team: str
) -> Dict[str, Any]:

    suffix = market_ticker.split("-")[-1]  # e.g. "LAL", "GSW"
    team = suffix
    side = "home" if team == home_team else "away"

    price = normalize_price(candle_row.get("price_close"))
    yes_bid = normalize_price(candle_row.get("yes_bid_close"))
    yes_ask = normalize_price(candle_row.get("yes_ask_close"))
    spread = None

    if yes_bid is not None and yes_ask is not None:
        spread = abs(yes_ask - yes_bid)

    entry = {
        "market_id": market_ticker,
        "type": "moneyline",
        "team": team,
        "side": side,
        "line": None,
        "price": price,
        "yes_bid_prob": yes_bid,
        "yes_ask_prob": yes_ask,
        "bid_ask_spread": spread,
        "volume": candle_row.get("volume"),
        "open_interest": candle_row.get("open_interest"),
        "open_time": None,
        "close_time": None,
        "expiration_time": None,
        "status": None,
        "result": None,
        "market_title": None,
        "market_subtitle": None,
        "rules_primary": None,
    }

    return to_native(entry)


# =====================================================================
# Main Builder
# =====================================================================

def build_states_for_game(
    season: str,
    game_id: str,
    home_team: str,
    away_team: str,
    market_tickers: List[str],
    output_dir: Path,
) -> None:

    print(
        f"\nBuilding states for GAME_ID={game_id}  {away_team} @ {home_team}")

    # --- Load timeline ---
    timeline = load_nba_timeline(season, game_id)

    # --- Load & align candlesticks ---
    aligned_by_ticker = {}
    for ticker in market_tickers:
        df = load_market_tick_data(ticker)
        aligned_by_ticker[ticker] = align_candles_to_timeline(timeline, df)

    # --- Build final states list ---
    states = []

    for idx, row in timeline.iterrows():
        timestamp = row["timestamp"]

        markets_list = []
        for ticker in market_tickers:
            cand = aligned_by_ticker[ticker][idx]
            if cand is None:
                continue

            markets_list.append(
                build_market_entry(cand, ticker, home_team, away_team)
            )

        state = {
            "timestamp": timestamp,
            "game_id": game_id,
            "home_team": home_team,
            "away_team": away_team,
            "score_home": row.get("score_home"),
            "score_away": row.get("score_away"),
            "score_diff": row.get("score_diff"),
            "quarter": row.get("quarter"),
            "time_remaining_minutes": row.get("time_remaining_minutes"),
            "time_remaining_quarter_seconds": row.get("time_remaining_quarter_seconds"),
            "markets": markets_list,
        }

        state = to_native(state)
        states.append(state)

    # --- Save ---
    output_dir.mkdir(parents=True, exist_ok=True)
    outpath = output_dir / f"{game_id}.json"
    with open(outpath, "w") as f:
        json.dump(states, f, indent=2)

    print(f"  -> WROTE {len(states)} STATES to {outpath}")


# =====================================================================
# CLI
# =====================================================================

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Build merged engine-facing game states.")
    p.add_argument("--season", required=True)
    p.add_argument("--game-id", required=True)
    p.add_argument("--home-team", required=True)
    p.add_argument("--away-team", required=True)
    p.add_argument("--market-tickers", nargs="+", required=True)
    p.add_argument("--out-dir", default="src/data/merged/states")
    return p.parse_args()


def main():
    args = parse_args()

    build_states_for_game(
        season=args.season,
        game_id=args.game_id,
        home_team=args.home_team,
        away_team=args.away_team,
        market_tickers=args.market_tickers,
        output_dir=Path(args.out_dir),
    )


if __name__ == "__main__":
    main()
