import requests
import pandas as pd


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
    """
    Fetch play-by-play from the liveData endpoint for a single game.
    """
    url = f"https://cdn.nba.com/static/json/liveData/playbyplay/playbyplay_{game_id}.json"
    print(f"Requesting: {url}")

    resp = requests.get(url, headers=HEADERS)
    resp.raise_for_status()

    data = resp.json()

    # Core events live under game.actions
    actions = data["game"]["actions"]
    df = pd.DataFrame(actions)
    return df


def main():
    game_id = "0022400061"  # one of your 2024-25 games

    print(f"Fetching liveData play-by-play for game_id={game_id}...")
    df = fetch_pbp_live(game_id)

    print("\nCOLUMNS:")
    print(list(df.columns))

    print("\nHEAD:")
    print(df.head(50))

    print("\nTOTAL ROWS:", len(df))


if __name__ == "__main__":
    main()
