from typing import List
from nba_api.stats.endpoints import LeagueGameFinder
import pandas as pd
from pathlib import Path


def build_games_index(season: str, season_types: List[str] | None = None) -> pd.DataFrame:
    """
    Build a games index for a given season string, e.g. '2024-25', '2025-26'.

    season_types: list like ["Regular Season", "Playoffs"].
    Returns one row per game with home/away abbrevs + SEASON_TYPE.
    """
    if season_types is None:
        season_types = ["Regular Season"]

    frames: list[pd.DataFrame] = []

    for stype in season_types:
        game_finder = LeagueGameFinder(
            league_id_nullable="00",
            season_nullable=season,
            season_type_nullable=stype,
        )
        df = game_finder.get_data_frames()[0]
        df["SEASON_TYPE"] = stype
        frames.append(df)

    games_all = pd.concat(frames, ignore_index=True)

    # You already had logic to pivot home/away based on MATCHUP.
    # Keep that the same — just apply it to games_all.
    games_all["HOME_TEAM_ABBREV"] = games_all.apply(
        lambda row: row["TEAM_ABBREVIATION"]
        if "vs." in row["MATCHUP"]
        # quick safety, but your original logic is fine
        else row["MATCHUP"].split()[-1],
        axis=1,
    )

    games_all["AWAY_TEAM_ABBREV"] = games_all.apply(
        lambda row: row["TEAM_ABBREVIATION"]
        if "@" in row["MATCHUP"]
        else row["MATCHUP"].split()[-1],
        axis=1,
    )

    # Now reduce to one row per GAME_ID (home team row)
    mask_home = games_all["MATCHUP"].str.contains("vs.")
    games_index = games_all.loc[mask_home, [
        "GAME_ID", "GAME_DATE", "HOME_TEAM_ABBREV", "AWAY_TEAM_ABBREV", "SEASON_TYPE"]]

    # Sort for sanity
    games_index = games_index.sort_values(
        ["GAME_DATE", "GAME_ID"]).reset_index(drop=True)
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
    # out_2425 = Path("src/data/nba/games_index_2024_25.csv")
    out_2526 = Path("src/data/nba/games_index_2025_26.csv")

    # # 2024–25: Regular Season + Playoffs
    # games_2425 = build_games_index("2024-25", ["Regular Season", "Playoffs"])
    # out_2425.parent.mkdir(parents=True, exist_ok=True)
    # games_2425.to_csv(out_2425, index=False)
    # print(f"2024-25 games (reg + playoffs): {len(games_2425)} -> {out_2425}")

    # 2025–26: only Regular Season for now
    games_2526 = build_games_index("2025-26", ["Regular Season"])
    games_2526.to_csv(out_2526, index=False)
    print(f"2025-26 games (regular season): {len(games_2526)} -> {out_2526}")


if __name__ == "__main__":
    main()
