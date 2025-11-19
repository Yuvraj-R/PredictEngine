import time
from pathlib import Path

import pandas as pd

from .fetch_play_by_play import save_timeline_csv


def build_game_states_for_season(
    season: str,
    index_path: Path,
    limit: int | None = 5,
    sleep_seconds: float = 0.5,
) -> None:
    print(f"Loading games index from: {index_path}")
    games_index = pd.read_csv(index_path, dtype={"GAME_ID": str})

    if limit is not None:
        games_index = games_index.head(limit)

    print(
        f"Building game_states for {len(games_index)} games in season {season}...\n")

    for i, row in games_index.iterrows():
        game_id = str(row["GAME_ID"])
        home_team = row["HOME_TEAM_ABBREV"]
        away_team = row["AWAY_TEAM_ABBREV"]

        print(f"[{i + 1}/{len(games_index)}] {game_id}: {home_team} vs {away_team}")

        try:
            out_path = save_timeline_csv(
                season=season,
                game_id=game_id,
                home_team=home_team,
                away_team=away_team,
            )
            print(f"  -> saved {out_path}")
        except Exception as e:
            print(f"  !! error for game {game_id}: {e}")

        time.sleep(sleep_seconds)


def main():
    season = "2024-25"
    index_path = Path("src/data/nba/games_index_2024_25.csv")

    build_game_states_for_season(
        season=season,
        index_path=index_path,
        limit=5,
        sleep_seconds=0.5,
    )


if __name__ == "__main__":
    main()
