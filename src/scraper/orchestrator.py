from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

SCRAPER_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRAPER_DIR.parent.parent
JOBS_DIR = SCRAPER_DIR / "jobs"


def _load_jobs(game_date: str) -> List[Dict[str, Any]]:
    path = JOBS_DIR / f"jobs_{game_date}.json"
    if not path.exists():
        raise FileNotFoundError(f"Jobs file not found: {path}")
    with path.open() as f:
        return json.load(f)


def _filter_jobs(
    jobs: List[Dict[str, Any]],
    team: Optional[str] = None,
) -> List[Dict[str, Any]]:
    if not team:
        return jobs
    t = team.upper()
    return [
        j for j in jobs
        if j.get("home_team") == t or j.get("away_team") == t
    ]


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Launch WebSocket game_workers for all jobs on a date."
    )
    p.add_argument(
        "--date",
        required=True,
        help="Game date in YYYY-MM-DD (must match jobs_<date>.json).",
    )
    p.add_argument(
        "--team",
        help="Optional 3-letter team abbrev to filter (e.g. BOS, LAC).",
    )
    p.add_argument(
        "--pregame-minutes",
        type=int,
        default=10,
        help="Minutes before tipoff each worker should start.",
    )
    p.add_argument(
        "--max-workers",
        type=int,
        default=64,  # soft cap; NBA slate is small, this is effectively "unlimited"
        help="Max concurrent workers (soft cap; currently not strictly enforced).",
    )
    return p.parse_args()


def main() -> None:
    args = _parse_args()

    jobs = _load_jobs(args.date)
    jobs = _filter_jobs(jobs, team=args.team)
    if not jobs:
        print("[orchestrator] No jobs after filtering; nothing to do.")
        return

    print(f"[orchestrator] Launching {len(jobs)} workers for {args.date}")

    procs: List[tuple[str, subprocess.Popen]] = []

    # Start one game_worker per job
    for j in jobs:
        event_ticker = j["event_ticker"]
        cmd = [
            sys.executable,
            "-m",
            "src.scraper.game_worker",
            "--date",
            args.date,
            "--event-ticker",
            event_ticker,
            "--pregame-minutes",
            str(args.pregame_minutes),
        ]

        print(f"[orchestrator] Starting worker for {event_ticker}")
        p = subprocess.Popen(
            cmd,
            cwd=str(PROJECT_ROOT),
        )
        procs.append((event_ticker, p))

    # Wait for all workers to finish
    for event_ticker, p in procs:
        ret = p.wait()
        print(f"[orchestrator] Worker {event_ticker} exited with code {ret}")

    print("[orchestrator] All workers done.")


if __name__ == "__main__":
    main()
