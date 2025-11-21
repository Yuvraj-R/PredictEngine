from __future__ import annotations
import math

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
    market_ticker: str,
    candles_df: pd.DataFrame,
    ts: pd.Timestamp,
    home_team: str,
    away_team: str,
) -> dict[str, Any] | None:
    """
    For a given timestamp ts and market_ticker, return a single market dict.

    - Uses the last candle with end_period_iso <= ts.
    - Normalizes prices to [0, 1].
    - Canonical 'price' is:
        1) price_close / 100, or
        2) price_mean / 100, or
        3) mid(yes_bid_close, yes_ask_close) / 100, or
        4) yes_bid_close/100 or yes_ask_close/100 as last resort.
    """

    if candles_df.empty:
        return None

    # Make sure the candles are sorted by time
    candles_sorted = candles_df.sort_values("end_period_iso")

    # Take last candle whose end_period_iso <= ts
    mask = candles_sorted["end_period_iso"] <= ts
    sub = candles_sorted.loc[mask]
    if sub.empty:
        return None

    row = sub.iloc[-1]

    def safe_float(x):
        if x is None:
            return None
        if isinstance(x, (int, float)):
            if isinstance(x, float) and math.isnan(x):
                return None
            return float(x)
        try:
            v = float(x)
            if math.isnan(v):
                return None
            return v
        except Exception:
            return None

    # Raw values from candles (in cents/probability points)
    yes_bid_close_raw = safe_float(row.get("yes_bid_close"))
    yes_ask_close_raw = safe_float(row.get("yes_ask_close"))
    price_close_raw = safe_float(row.get("price_close"))
    price_mean_raw = safe_float(row.get("price_mean"))

    # Normalize to [0, 1]
    yes_bid_prob = yes_bid_close_raw / 100.0 if yes_bid_close_raw is not None else None
    yes_ask_prob = yes_ask_close_raw / 100.0 if yes_ask_close_raw is not None else None
    price_close = price_close_raw / 100.0 if price_close_raw is not None else None
    price_mean = price_mean_raw / 100.0 if price_mean_raw is not None else None

    # Canonical price selection:
    # 1) trade close, 2) trade mean, 3) mid of bid/ask, 4) single side
    price = None
    if price_close is not None:
        price = price_close
    elif price_mean is not None:
        price = price_mean
    else:
        if yes_bid_prob is not None and yes_ask_prob is not None:
            price = 0.5 * (yes_bid_prob + yes_ask_prob)
        elif yes_bid_prob is not None:
            price = yes_bid_prob
        elif yes_ask_prob is not None:
            price = yes_ask_prob
        else:
            price = None  # truly no information for this timestamp

    # Bid/ask spread in probability space
    spread = None
    if yes_bid_prob is not None and yes_ask_prob is not None:
        spread = abs(yes_ask_prob - yes_bid_prob)

    volume = safe_float(row.get("volume"))
    open_interest = safe_float(row.get("open_interest"))

    # Team code is the suffix of the ticker after the last '-'
    #   e.g. KXNBAGAME-25OCT21HOUOKC-OKC -> OKC
    team_code = market_ticker.split("-")[-1]
    if team_code == home_team:
        side = "home"
    elif team_code == away_team:
        side = "away"
    else:
        side = None  # shouldn't really happen for our mapping

    entry = {
        "market_id": market_ticker,
        "type": "moneyline",
        "team": team_code,
        "side": side,
        "line": None,
        "price": price,
        "yes_bid_prob": yes_bid_prob,
        "yes_ask_prob": yes_ask_prob,
        "bid_ask_spread": spread,
        "volume": volume,
        "open_interest": open_interest,
        "open_time": None,
        "close_time": None,
        "expiration_time": None,
        "status": None,
        "result": None,
        "market_title": None,
        "market_subtitle": None,
        "rules_primary": None,
    }

    # Let to_native handle scalars/lists/dicts as before
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

    # --- Load candles per market ticker ---
    candles_by_ticker: Dict[str, pd.DataFrame] = {}
    for ticker in market_tickers:
        df = load_market_tick_data(ticker)
        candles_by_ticker[ticker] = df

    # --- Build final states list ---
    states: List[Dict[str, Any]] = []

    for _, row in timeline.iterrows():
        timestamp = row["timestamp"]

        markets_list: List[Dict[str, Any]] = []
        for ticker in market_tickers:
            df = candles_by_ticker[ticker]

            entry = build_market_entry(
                market_ticker=ticker,
                candles_df=df,
                ts=timestamp,
                home_team=home_team,
                away_team=away_team,
            )

            if entry is not None:
                markets_list.append(entry)

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
            "time_remaining_quarter_seconds": row.get(
                "time_remaining_quarter_seconds"
            ),
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
