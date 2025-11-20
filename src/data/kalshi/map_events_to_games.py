# src/data/kalshi/map_events_to_games.py

from pathlib import Path
import pandas as pd


EVENTS_INDEX_PATH = Path("src/data/kalshi/kalshi_events_index.csv")


def load_events_index() -> pd.DataFrame:
    df = pd.read_csv(EVENTS_INDEX_PATH)
    # Ensure event_date is string ISO date
    if "event_date" in df.columns:
        df["event_date"] = df["event_date"].astype("string")
    return df


def map_season(
    season_label: str,
    nba_index_path: Path,
    events_df: pd.DataFrame,
    out_map_path: Path,
    out_unmatched_path: Path,
) -> None:
    print(f"\n=== Mapping season {season_label} ===")

    nba = pd.read_csv(nba_index_path, dtype={"GAME_ID": str})
    nba["GAME_DATE"] = nba["GAME_DATE"].astype("string")

    print(f"NBA games ({season_label}): {len(nba)}")
    print(f"Total Kalshi events: {len(events_df)}")

    # Inner join on (date, home, away)
    merged = nba.merge(
        events_df,
        left_on=["GAME_DATE", "HOME_TEAM_ABBREV", "AWAY_TEAM_ABBREV"],
        right_on=["event_date", "home_team", "away_team"],
        how="inner",
    )

    print(f"Matched games ({season_label}): {len(merged)}")

    cols_to_keep = [
        "GAME_ID",
        "GAME_DATE",
        "HOME_TEAM_ABBREV",
        "AWAY_TEAM_ABBREV",
        "event_ticker",
        "title",
        "sub_title",
        "status",
    ]

    out_map_path.parent.mkdir(parents=True, exist_ok=True)
    merged[cols_to_keep].to_csv(out_map_path, index=False)
    print(f"Wrote mapping to: {out_map_path.resolve()}")

    # Optional: log NBA games without a Kalshi event
    unmatched = nba[~nba["GAME_ID"].isin(merged["GAME_ID"])]
    unmatched.to_csv(out_unmatched_path, index=False)
    print(f"NBA games without Kalshi coverage: {len(unmatched)}")
    print(f"Wrote unmatched list to: {out_unmatched_path.resolve()}")


def main():
    events_df = load_events_index()

    # 2024–25 season
    # map_season(
    #     season_label="2024_25",
    #     nba_index_path=Path("src/data/nba/games_index_2024_25.csv"),
    #     events_df=events_df,
    #     out_map_path=Path("src/data/merged/nba_kalshi_game_map_2024_25.csv"),
    #     out_unmatched_path=Path(
    #         "src/data/merged/nba_without_kalshi_2024_25.csv"),
    # )

    # 2025–26 season
    map_season(
        season_label="2025_26",
        nba_index_path=Path("src/data/nba/games_index_2025_26.csv"),
        events_df=events_df,
        out_map_path=Path("src/data/merged/nba_kalshi_game_map_2025_26.csv"),
        out_unmatched_path=Path(
            "src/data/merged/nba_without_kalshi_2025_26.csv"),
    )


if __name__ == "__main__":
    main()
