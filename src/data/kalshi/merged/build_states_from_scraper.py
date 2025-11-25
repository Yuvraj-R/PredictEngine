from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd


# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parents[5]  # .../PredictEngine
LIVE_DATA_ROOT = PROJECT_ROOT / "src" / "data" / "kalshi" / "live" / "live_data"
LIVE_JOBS_DIR = PROJECT_ROOT / "src" / "data" / "kalshi" / "live" / "jobs"
NBA_GAME_STATES_ROOT = PROJECT_ROOT / "src" / "data" / "nba" / "game_states"
MERGED_STATES_DIR = PROJECT_ROOT / "src" / \
    "data" / "kalshi" / "merged" / "states"


TERMINAL_STATUSES = {"finalized", "inactive", "settled", "closed"}


@dataclass
class GameJob:
    game_date: str
    game_id: str
    home_team: str
    away_team: str
    tipoff_utc: Optional[str]
    event_ticker: str
    market_tickers: List[str]

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "GameJob":
        return cls(
            game_date=d["game_date"],
            game_id=d["game_id"],
            home_team=d["home_team"],
            away_team=d["away_team"],
            tipoff_utc=d.get("tipoff_utc"),
            event_ticker=d["event_ticker"],
            market_tickers=list(d["market_tickers"]),
        )


# ---------------------------------------------------------------------------
# Load raw data
# ---------------------------------------------------------------------------


def _find_nba_game_states_csv(game_id: str) -> Path:
    """
    Find game_states CSV for a given GAME_ID by scanning season subdirs:
      src/data/nba/game_states/*/game_<GAME_ID>.csv

    Assumes you've already run build_game_states_batch so these exist.
    """
    pattern = f"*/game_{game_id}.csv"
    candidates = list(NBA_GAME_STATES_ROOT.glob(pattern))
    if not candidates:
        raise FileNotFoundError(
            f"No NBA game_states CSV found for GAME_ID={game_id} under "
            f"{NBA_GAME_STATES_ROOT} (pattern={pattern})"
        )
    if len(candidates) > 1:
        # Should not normally happen; be explicit if it does.
        raise RuntimeError(
            f"Multiple NBA game_states CSVs found for GAME_ID={game_id}: {candidates}"
        )
    return candidates[0]


def _load_nba_timeline(game_id: str) -> pd.DataFrame:
    """
    Load the NBA timeline CSV (one row per PBP action) and add a parsed
    timestamp column for as-of merging.
    """
    path = _find_nba_game_states_csv(game_id)
    df = pd.read_csv(path)

    # Ensure timestamp is tz-aware UTC
    df["timestamp_dt"] = pd.to_datetime(df["timestamp"], utc=True)

    # Sort just to be safe
    df = df.sort_values("timestamp_dt").reset_index(drop=True)
    return df


def _load_kalshi_ticks(job: GameJob) -> pd.DataFrame:
    """
    Load all Kalshi ticks for a single event_ticker from scraper JSONL files.

    Expects files like:
      src/scraper/data/<YYYY-MM-DD>/<EVENT_TICKER>/<MARKET_TICKER>.jsonl
    """
    event_dir = LIVE_DATA_ROOT / job.game_date / job.event_ticker
    if not event_dir.exists():
        raise FileNotFoundError(f"Missing Kalshi data dir: {event_dir}")

    rows: List[Dict[str, Any]] = []
    for path in sorted(event_dir.glob("*.jsonl")):
        with path.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                rec = json.loads(line)
                # Ensure these fields exist even if early files were missing them
                rec.setdefault("event_ticker", job.event_ticker)
                rec.setdefault("market_ticker", path.stem)
                rows.append(rec)

    if not rows:
        raise RuntimeError(f"No Kalshi tick rows found in {event_dir}")

    df = pd.DataFrame(rows)

    # Parse Kalshi timestamp → tz-aware datetime
    if "ts_iso" in df.columns:
        df["ts_dt"] = pd.to_datetime(df["ts_iso"], utc=True)
    elif "kalshi_ts" in df.columns:
        df["ts_dt"] = pd.to_datetime(df["kalshi_ts"], unit="s", utc=True)
    else:
        raise RuntimeError("Kalshi ticks missing ts_iso/kalshi_ts columns")

    df = df.sort_values("ts_dt").reset_index(drop=True)
    return df


# ---------------------------------------------------------------------------
# Merge NBA + Kalshi into engine-ready states
# ---------------------------------------------------------------------------


def _compute_bid_ask_spread(bid: Optional[float], ask: Optional[float]) -> Optional[float]:
    if bid is None or ask is None:
        return None
    try:
        return float(ask) - float(bid)
    except Exception:
        return None


def _attach_nba_to_ticks(kalshi_df: pd.DataFrame, nba_df: pd.DataFrame) -> pd.DataFrame:
    """
    For each Kalshi tick, attach the last NBA scoreboard row with
    timestamp <= tick time (backward as-of join).

    We drop ticks that occur before the first NBA row to avoid
    "seeing into the future" when we don't yet know the scoreboard.
    """
    merged = pd.merge_asof(
        kalshi_df.sort_values("ts_dt"),
        nba_df.sort_values("timestamp_dt"),
        left_on="ts_dt",
        right_on="timestamp_dt",
        direction="backward",
    )

    # Drop ticks that have no NBA context (pre-first-PBP rows)
    merged = merged.dropna(subset=["score_home", "score_away"])
    return merged.reset_index(drop=True)


def _build_market_static_meta(job: GameJob) -> Dict[str, Dict[str, Any]]:
    """
    For each market_ticker, derive static metadata:
    team, side (home/away), type, etc.
    """
    meta: Dict[str, Dict[str, Any]] = {}

    for mt in job.market_tickers:
        # Expect ticker like KXNBAGAME-25NOV22NYKORL-ORL
        parts = mt.split("-")
        team = parts[-1] if parts else None

        if team == job.home_team:
            side = "home"
        elif team == job.away_team:
            side = "away"
        else:
            side = "unknown"

        meta[mt] = {
            "market_id": mt,
            "type": "moneyline",
            "team": team,
            "side": side,
            "line": None,
            "market_title": None,
            "market_subtitle": None,
            "rules_primary": None,
            "open_time": None,
            "close_time": None,
            "expiration_time": None,
            "result": None,
        }

    return meta


def build_states_for_game(job: GameJob) -> List[Dict[str, Any]]:
    """
    Core logic: given one GameJob, load Kalshi ticks + NBA timeline and
    return a list of merged state dicts suitable for the engine.
    """
    kalshi_df = _load_kalshi_ticks(job)
    nba_df = _load_nba_timeline(job.game_id)

    merged = _attach_nba_to_ticks(kalshi_df, nba_df)
    if merged.empty:
        raise RuntimeError(
            f"No merged ticks for GAME_ID={job.game_id} / {job.event_ticker}"
        )

    market_meta = _build_market_static_meta(job)

    # We'll maintain "last known" snapshot per market and emit a full
    # markets[] list at each unique tick timestamp.
    last_snapshots: Dict[str, Optional[Dict[str, Any]]] = {
        mt: None for mt in job.market_tickers
    }

    states: List[Dict[str, Any]] = []

    # Group by tick timestamp
    for ts_dt, group in merged.groupby("ts_dt", sort=True):
        # Update snapshots for whichever markets changed at this tick
        for _, row in group.iterrows():
            mt = row["market_ticker"]
            if mt not in market_meta:
                # Ignore stray markets if any (non-moneyline)
                continue

            base = dict(market_meta[mt])

            price = row.get("price_prob")
            yes_bid = row.get("yes_bid_prob")
            yes_ask = row.get("yes_ask_prob")

            snap = {
                **base,
                "price": float(price) if price is not None else None,
                "yes_bid_prob": float(yes_bid) if yes_bid is not None else None,
                "yes_ask_prob": float(yes_ask) if yes_ask is not None else None,
                "bid_ask_spread": _compute_bid_ask_spread(yes_bid, yes_ask),
                "volume": row.get("volume"),
                "open_interest": row.get("open_interest"),
                "status": row.get("status"),
            }
            last_snapshots[mt] = snap

        # Scoreboard info: take from last row in this group
        row0 = group.iloc[-1]

        markets_list = [snap for snap in last_snapshots.values()
                        if snap is not None]
        if not markets_list:
            # No markets have ticked yet; skip this timestamp.
            continue

        state = {
            "timestamp": ts_dt.isoformat(),
            "game_id": job.game_id,
            "home_team": job.home_team,
            "away_team": job.away_team,
            "score_home": float(row0["score_home"]),
            "score_away": float(row0["score_away"]),
            "score_diff": float(row0["score_diff"]),
            "quarter": int(row0["quarter"]),
            "time_remaining_minutes": float(row0["time_remaining_minutes"]),
            "time_remaining_quarter_seconds": float(
                row0["time_remaining_quarter_seconds"]
            ),
            "markets": markets_list,
        }

        states.append(state)

    return states


def write_states_for_game(job: GameJob) -> Path:
    """
    Build merged states for one game and write them to:
      src/data/merged/states/<GAME_ID>.json
    """
    MERGED_STATES_DIR.mkdir(parents=True, exist_ok=True)
    out_path = MERGED_STATES_DIR / f"{job.game_id}.json"

    states = build_states_for_game(job)

    with out_path.open("w", encoding="utf-8") as f:
        json.dump(states, f)

    print(
        f"[build_states] GAME_ID={job.game_id} "
        f"{job.away_team}@{job.home_team} "
        f"-> {out_path} ({len(states)} states)"
    )
    return out_path


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _load_jobs_for_date(game_date: str) -> List[GameJob]:
    jobs_path = LIVE_JOBS_DIR / f"jobs_{game_date}.json"
    if not jobs_path.exists():
        raise FileNotFoundError(f"Jobs file not found: {jobs_path}")

    with jobs_path.open("r", encoding="utf-8") as f:
        raw = json.load(f)

    return [GameJob.from_dict(j) for j in raw]


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description=(
            "Build merged 1s game states by joining Kalshi websocket ticks "
            "with NBA play-by-play for a given date."
        )
    )
    p.add_argument(
        "--date",
        required=True,
        help="Game date in YYYY-MM-DD (must match scraper jobs_<date>.json).",
    )
    p.add_argument(
        "--limit-games",
        type=int,
        default=None,
        help="Optional limit on number of games to process (for testing).",
    )
    return p.parse_args()


def main() -> None:
    args = _parse_args()
    jobs = _load_jobs_for_date(args.date)

    if args.limit_games is not None:
        jobs = jobs[: args.limit_games]

    print(
        f"[build_states] Building merged states for {len(jobs)} games on {args.date}")

    for job in jobs:
        try:
            write_states_for_game(job)
        except Exception as e:  # noqa: BLE001
            print(
                f"[build_states] ERROR building states for {job.game_id} "
                f"/ {job.event_ticker}: {e}"
            )


if __name__ == "__main__":
    main()
