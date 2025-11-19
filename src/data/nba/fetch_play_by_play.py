import requests
import pandas as pd
from pathlib import Path

HEADERS = {
    "Connection": "keep-alive",
    "Accept": "application/json, text/plain, */*",
    "x-nba-stats-token": "true",
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_6) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/79.0.3945.130 Safari/537.36"
    ),
    "x-nba-stats-origin": "stats",
    "Sec-Fetch-Site": "same-origin",
    "Sec-Fetch-Mode": "cors",
    "Referer": "https://www.nba.com/",
    "Accept-Encoding": "gzip, deflate, br",
    "Accept-Language": "en-US,en;q=0.9",
}


def fetch_pbp_live(game_id: str) -> pd.DataFrame:
    url = f"https://cdn.nba.com/static/json/liveData/playbyplay/playbyplay_{game_id}.json"
    print(f"Requesting: {url}")
    resp = requests.get(url, headers=HEADERS)
    resp.raise_for_status()
    data = resp.json()
    actions = data["game"]["actions"]
    return pd.DataFrame(actions)


def parse_clock_to_seconds(clock: str) -> float:
    if not isinstance(clock, str):
        return None
    try:
        no_prefix = clock.replace("PT", "")
        minutes_str, seconds_part = no_prefix.split("M")
        seconds_str = seconds_part.replace("S", "")
        minutes = float(minutes_str)
        seconds = float(seconds_str)
        return minutes * 60 + seconds
    except Exception:
        return None


def build_timeline_for_game(game_id: str, home_team: str, away_team: str) -> pd.DataFrame:
    df = fetch_pbp_live(game_id)

    df["time_remaining_quarter_seconds"] = df["clock"].apply(
        parse_clock_to_seconds)
    df["time_remaining_minutes"] = df["time_remaining_quarter_seconds"] / 60.0

    timeline = pd.DataFrame(
        {
            "timestamp": df["timeActual"],
            "game_id": game_id,
            "home_team": home_team,
            "away_team": away_team,
            "quarter": df["period"],
            "time_remaining_quarter_seconds": df["time_remaining_quarter_seconds"],
            "time_remaining_minutes": df["time_remaining_minutes"],
            "score_home": df["scoreHome"],
            "score_away": df["scoreAway"],
        }
    )

    timeline["score_home"] = timeline["score_home"].astype(float)
    timeline["score_away"] = timeline["score_away"].astype(float)
    timeline["score_diff"] = (
        timeline["score_home"] - timeline["score_away"]).abs()

    return timeline


def save_timeline_csv(
    season: str,
    game_id: str,
    home_team: str,
    away_team: str,
) -> Path:
    """
    Build and save the timeline CSV for a single game.
    Returns the path to the written file.
    """
    timeline = build_timeline_for_game(game_id, home_team, away_team)

    # Normalize season string to directory-friendly, e.g. '2024-25' -> '2024_25'
    season_dir = season.replace("-", "_")
    out_dir = Path("src/data/nba/game_states") / season_dir
    out_dir.mkdir(parents=True, exist_ok=True)

    out_path = out_dir / f"game_{game_id}.csv"
    timeline.to_csv(out_path, index=False)

    return out_path


def main():
    season = "2024-25"
    game_id = "0022400061"
    home_team = "BOS"
    away_team = "NYK"

    print(f"Saving timeline CSV for {season} game_id={game_id}...")
    out_path = save_timeline_csv(season, game_id, home_team, away_team)

    print(f"Saved to: {out_path.resolve()}")

    # Quick sanity check: read back and print first few rows
    df = pd.read_csv(out_path)
    print("\nHEAD (from CSV):")
    print(df.head(10))


if __name__ == "__main__":
    main()
