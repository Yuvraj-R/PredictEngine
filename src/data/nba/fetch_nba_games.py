from nba_api.stats.endpoints import LeagueGameFinder
import pandas as pd
from pathlib import Path


def build_games_index(season: str) -> pd.DataFrame:
    """
    Build a games index for a given season string, e.g. '2024-25', '2025-26'.
    One row per game, with home/away team abbreviations.
    """
    game_finder = LeagueGameFinder(
        league_id_nullable="00",
        season_nullable=season,
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


def save_games_index(season: str, filename: str) -> None:
    games_index = build_games_index(season)

    print(f"\n=== Season {season} preview ===")
    print(games_index.head())
    print("\nTOTAL GAMES:", len(games_index))

    out_dir = Path("src/data/nba")
    out_dir.mkdir(parents=True, exist_ok=True)

    out_path = out_dir / filename
    games_index.to_csv(out_path, index=False)

    print(f"Saved to: {out_path.resolve()}")


def main():
    # 2024–25 full season
    save_games_index("2024-25", "games_index_2024_25.csv")

    # 2025–26 season so far
    save_games_index("2025-26", "games_index_2025_26.csv")


if __name__ == "__main__":
    main()
