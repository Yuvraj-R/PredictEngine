from nba_api.stats.endpoints import LeagueGameFinder
import pandas as pd
from pathlib import Path


def build_games_index_2024_25() -> pd.DataFrame:
    game_finder = LeagueGameFinder(
        league_id_nullable="00",
        season_nullable="2024-25",
        season_type_nullable="Regular Season",
    )

    games = game_finder.get_data_frames()[0]

    # Home games only: "XXX vs. YYY"
    home_games = games[games["MATCHUP"].str.contains("vs.")].copy()

    home_games["HOME_TEAM_ABBREV"] = home_games["TEAM_ABBREVIATION"]
    home_games["AWAY_TEAM_ABBREV"] = (
        home_games["MATCHUP"].str.split("vs.").str[1].str.strip()
    )

    games_index = (
        home_games[
            ["GAME_ID", "GAME_DATE", "HOME_TEAM_ABBREV", "AWAY_TEAM_ABBREV"]
        ]
        .drop_duplicates()
        .sort_values("GAME_DATE")
    )

    return games_index


def main():
    games_index = build_games_index_2024_25()

    # Print a small preview (sanity check)
    print("Preview:")
    print(games_index.head())
    print("\nTOTAL GAMES:", len(games_index))

    # Ensure directory exists
    out_dir = Path("src/data/nba")
    out_dir.mkdir(parents=True, exist_ok=True)

    out_path = out_dir / "games_index_2024_25.csv"
    games_index.to_csv(out_path, index=False)

    print(f"\nSaved to: {out_path.resolve()}")


if __name__ == "__main__":
    main()
