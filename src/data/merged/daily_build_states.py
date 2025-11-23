from __future__ import annotations

import argparse
import json
import subprocess
import sys
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List

from zoneinfo import ZoneInfo

from src.data.nba.fetch_nba_games import main as fetch_nba_games_main
from src.data.nba.fetch_play_by_play import save_timeline_csv

# Paths relative to project root (WorkingDirectory=/home/<user>/PredictEngine)
JOBS_DIR = Path("src/scraper/jobs")
GAME_STATES_BASE = Path("src/data/nba/game_states")


def _compute_default_date_et() -> date:
    """
    Default target date = yesterday in US/Eastern.
    """
    now_et = datetime.now(ZoneInfo("America/New_York"))
    return (now_et.date() - timedelta(days=1))


def _infer_season_label_for_date(d: date) -> str:
    """
    Infer NBA season label like '2024-25' or '2025-26' from a calendar date.

    Rule of thumb:
      - If month >= 10 → season_start = year, season_end = year+1
      - Else          → season_start = year-1, season_end = year
    """
    if d.month >= 10:
        season_start = d.year
        season_end = (d.year + 1) % 100
    else:
        season_start = d.year - 1
        season_end = d.year % 100
    return f"{season_start}-{season_end:02d}"


def _load_jobs_for_date(date_str: str) -> List[Dict[str, Any]]:
    jobs_path = JOBS_DIR / f"jobs_{date_str}.json"
    if not jobs_path.exists():
        raise FileNotFoundError(f"Jobs file not found: {jobs_path}")
    with jobs_path.open() as f:
        jobs = json.load(f)
    if not isinstance(jobs, list):
        raise ValueError(f"Jobs file is not a list: {jobs_path}")
    return jobs


def _collect_unique_games(jobs: List[Dict[str, Any]]) -> Dict[str, Dict[str, str]]:
    """
    From jobs list, build {game_id: {home_team, away_team}}.
    """
    games: Dict[str, Dict[str, str]] = {}
    for j in jobs:
        gid = str(j.get("game_id") or "").strip()
        if not gid:
            continue
        if gid not in games:
            home = j.get("home_team")
            away = j.get("away_team")
            if not home or not away:
                continue
            games[gid] = {"home_team": home, "away_team": away}
    return games


def _ensure_nba_timeline_for_game(
    season_label: str,
    game_id: str,
    home_team: str,
    away_team: str,
) -> None:
    """
    Ensure src/data/nba/game_states/<season_dir>/game_<GAME_ID>.csv exists
    for the given game. If it already exists, do nothing. Otherwise,
    fetch play-by-play and save it.
    """
    season_dir = season_label.replace("-", "_")
    out_dir = GAME_STATES_BASE / season_dir
    out_dir.mkdir(parents=True, exist_ok=True)

    out_path = out_dir / f"game_{game_id}.csv"
    if out_path.exists():
        print(
            f"[daily_build_states] [skip] game_states already exist for GAME_ID={game_id}")
        return

    print(
        f"[daily_build_states] Fetching PBP for GAME_ID={game_id} "
        f"{away_team} @ {home_team} (season={season_label})"
    )
    try:
        saved = save_timeline_csv(
            season=season_label,
            game_id=game_id,
            home_team=home_team,
            away_team=away_team,
        )
        print(f"[daily_build_states]   -> saved {saved}")
    except Exception as e:  # noqa: BLE001
        print(
            f"[daily_build_states]   !! error fetching PBP for {game_id}: {e}")


def _run_build_states_from_scraper(date_str: str) -> None:
    """
    Call: python -m src.data.merged.build_states_from_scraper --date <date_str>
    """
    cmd = [
        sys.executable,
        "-m",
        "src.data.merged.build_states_from_scraper",
        "--date",
        date_str,
    ]
    print(
        f"[daily_build_states] Running build_states_from_scraper: {' '.join(cmd)}")
    subprocess.check_call(cmd)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Daily pipeline: NBA games index + PBP + merged states from Kalshi scraper."
    )
    parser.add_argument(
        "--date",
        help="Target game date in YYYY-MM-DD (default: yesterday in US/Eastern).",
    )
    parser.add_argument(
        "--season",
        help='Season label like "2025-26" (default: inferred from date).',
    )
    return parser.parse_args()


def main() -> None:
    args = _parse_args()

    if args.date:
        target_date = date.fromisoformat(args.date)
    else:
        target_date = _compute_default_date_et()

    date_str = target_date.isoformat()

    if args.season:
        season_label = args.season
    else:
        season_label = _infer_season_label_for_date(target_date)

    print(
        f"[daily_build_states] Starting daily build for date={date_str}, "
        f"season={season_label}"
    )

    # 1) Refresh NBA games index (idempotent)
    try:
        print("[daily_build_states] Refreshing NBA games index...")
        fetch_nba_games_main()
    except Exception as e:  # noqa: BLE001
        # Not fatal for this pipeline, but log it.
        print(f"[daily_build_states] WARNING: fetch_nba_games failed: {e}")

    # 2) Ensure NBA PBP game_states for all Kalshi-covered games on this date
    jobs = _load_jobs_for_date(date_str)
    games = _collect_unique_games(jobs)
    if not games:
        print(
            f"[daily_build_states] No games found in jobs for {date_str}; nothing to do.")
        return

    print(
        f"[daily_build_states] Ensuring NBA game_states for {len(games)} games "
        f"on {date_str}"
    )

    for game_id, info in games.items():
        _ensure_nba_timeline_for_game(
            season_label=season_label,
            game_id=game_id,
            home_team=info["home_team"],
            away_team=info["away_team"],
        )

    # 3) Build merged engine-ready states from Kalshi + NBA
    _run_build_states_from_scraper(date_str)

    print("[daily_build_states] Done.")


if __name__ == "__main__":
    main()
