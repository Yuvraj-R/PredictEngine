from __future__ import annotations
from datetime import datetime
from zoneinfo import ZoneInfo
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[4]


def main():
    # Use America/New_York to match NBA / Kalshi calendar
    today = datetime.now(ZoneInfo("America/New_York")).date().isoformat()

    # 1) Discover jobs
    subprocess.check_call(
        [sys.executable, "-m", "src.data.kalshi.live.discover_games", "--date", today],
        cwd=str(ROOT),
    )

    # 2) Launch workers for all jobs
    subprocess.check_call(
        [sys.executable, "-m", "src.data.kalshi.live.orchestrator", "--date", today],
        cwd=str(ROOT),
    )


if __name__ == "__main__":
    main()
