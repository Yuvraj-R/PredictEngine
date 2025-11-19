import time
import argparse
from pathlib import Path

import pandas as pd

from .fetch_play_by_play import save_timeline_csv


def build_game_states_for_season(
    season: str,
    index_path: Path,
    limit: int | None,
    offset: int,
    sleep_seconds: float,
) -> None:
    print(f"Loading games index from: {index_path}")
    games_index = pd.read_csv(index_path, dtype={"GAME_ID": str})

    # Apply offset + limit
    if offset > 0:
        games_index = games_index.iloc[offset:]
    if limit is not None:
        games_index = games_index.head(limit)

    total = len(games_index)
    print(
        f"Building game_states for {total} games in season {season} (offset={offset})...\n")

    season_dir = season.replace("-", "_")
    out_base = Path("src/data/nba/game_states") / season_dir

    for i, row in games_index.iterrows():
        game_id = row["GAME_ID"]
        home_team = row["HOME_TEAM_ABBREV"]
        away_team = row["AWAY_TEAM_ABBREV"]

        out_path = out_base / f"game_{game_id}.csv"
        if out_path.exists():
            print(f"[skip] {game_id} already exists")
            continue

        idx = offset + i + 1
        print(f"[{idx}] {game_id}: {home_team} vs {away_team}")

        try:
            saved = save_timeline_csv(
                season=season,
                game_id=game_id,
                home_team=home_team,
                away_team=away_team,
            )
            print(f"  -> saved {saved}")
        except Exception as e:
            print(f"  !! error for game {game_id}: {e}")

        time.sleep(sleep_seconds)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--season", required=True,
                        help="e.g. 2024-25 or 2025-26")
    parser.add_argument(
        "--index-path",
        default="src/data/nba/games_index_2024_25.csv",
        help="Path to games_index CSV",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Max number of games to process (default: all)",
    )
    parser.add_argument(
        "--offset",
        type=int,
        default=0,
        help="Number of games to skip from the top",
    )
    parser.add_argument(
        "--sleep",
        type=float,
        default=0.5,
        help="Seconds to sleep between games",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    season = args.season
    index_path = Path(args.index_path)

    build_game_states_for_season(
        season=season,
        index_path=index_path,
        limit=args.limit,
        offset=args.offset,
        sleep_seconds=args.sleep,
    )


if __name__ == "__main__":
    main()
