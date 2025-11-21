from __future__ import annotations

import argparse
from pathlib import Path
from typing import List

import pandas as pd

from .build_game_states import build_states_for_game


def build_states_for_season(
    season: str,
    markets_path: Path,
    out_dir: Path,
    limit_games: int | None = None,
    overwrite: bool = False,
) -> None:
    """
    For a given season, read nba_markets_<season>.csv and build
    states JSON files for each game.

    markets_path: e.g. src/data/kalshi/nba_markets_2025_26.csv
    out_dir: where to write JSON, e.g. src/data/merged/states
    """

    print(f"Loading markets from: {markets_path}")
    df = pd.read_csv(markets_path, dtype={"game_id": str})

    # Just in case, keep only rows matching this season label if present
    if "season_label" in df.columns:
        df = df[df["season_label"] == season]

    # Group by game_id; expect 2 rows (home/away winner market) per game
    grouped = df.groupby("game_id", sort=True)

    game_ids: List[str] = list(grouped.groups.keys())
    game_ids.sort()

    if limit_games is not None:
        game_ids = game_ids[:limit_games]

    print(f"Found {len(game_ids)} games to build states for.")

    out_dir.mkdir(parents=True, exist_ok=True)

    for idx, game_id in enumerate(game_ids, start=1):
        game_rows = grouped.get_group(game_id)

        # Basic sanity: expect at least 1 row (ideally 2)
        tickers = list(game_rows["ticker"].unique())
        home_team = game_rows["home_team"].iloc[0]
        away_team = game_rows["away_team"].iloc[0]

        outpath = out_dir / f"{game_id}.json"
        if outpath.exists() and not overwrite:
            print(
                f"[{idx}/{len(game_ids)}] GAME_ID={game_id} -> skip (already exists)")
            continue

        print(
            f"[{idx}/{len(game_ids)}] GAME_ID={game_id} "
            f"{away_team} @ {home_team} | markets={tickers}"
        )

        try:
            build_states_for_game(
                season=season,
                game_id=game_id,
                home_team=home_team,
                away_team=away_team,
                market_tickers=tickers,
                output_dir=out_dir,
            )
        except Exception as e:  # noqa: BLE001
            print(f"  !! Error building states for GAME_ID={game_id}: {e}")


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Build engine-facing state JSONs for all games in a season."
    )
    p.add_argument(
        "--season",
        required=True,
        help='Season label, e.g. "2025-26" or "2024-25".',
    )
    p.add_argument(
        "--markets-path",
        required=False,
        help=(
            "Path to nba_markets_<season>.csv "
            "(default: src/data/kalshi/nba_markets_<season_tag>.csv)"
        ),
    )
    p.add_argument(
        "--out-dir",
        default="src/data/merged/states",
        help="Directory to write JSON state files.",
    )
    p.add_argument(
        "--limit-games",
        type=int,
        default=None,
        help="Limit number of games (for testing).",
    )
    p.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite existing JSON files (default is to skip).",
    )
    return p.parse_args()


def main() -> None:
    args = parse_args()

    season_tag = args.season.replace("-", "_")
    default_markets = Path("src/data/kalshi") / f"nba_markets_{season_tag}.csv"
    markets_path = Path(
        args.markets_path) if args.markets_path else default_markets

    build_states_for_season(
        season=args.season,
        markets_path=markets_path,
        out_dir=Path(args.out_dir),
        limit_games=args.limit_games,
        overwrite=args.overwrite,
    )


if __name__ == "__main__":
    main()
