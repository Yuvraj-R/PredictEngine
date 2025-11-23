from __future__ import annotations

import argparse
import asyncio
import base64
import json
import os
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import websockets  # pip install websockets
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding
from dotenv import load_dotenv

from .discover_games import Job  # reuse Job dataclass from discover_games.py

# ---------------------------------------------------------------------------
# Paths / basic setup
# ---------------------------------------------------------------------------

SCRAPER_DIR = Path(__file__).resolve(
).parent                  # .../src/scraper
PROJECT_ROOT = SCRAPER_DIR.parent.parent                       # .../PredictEngine
JOBS_DIR = SCRAPER_DIR / "jobs"
# where we write ticks
DATA_DIR = SCRAPER_DIR / "data"

# Load .env from project root
load_dotenv(PROJECT_ROOT / ".env")

WS_URL = "wss://api.elections.kalshi.com/trade-api/ws/v2"
API_KEY_ID = os.getenv("KALSHI_API_KEY_ID")
PRIVATE_KEY_PATH = PROJECT_ROOT / "kalshi_private_key.pem"

PREGAME_MINUTES_DEFAULT = 10  # how long before tipoff to start streaming
INACTIVITY_RECONNECT_SECS = 90.0


# ---------------------------------------------------------------------------
# Auth helpers (RSA-PSS, per Kalshi docs)
# ---------------------------------------------------------------------------

def _load_private_key() -> Any:
    if not PRIVATE_KEY_PATH.exists():
        raise RuntimeError(f"Private key not found at {PRIVATE_KEY_PATH}")
    with PRIVATE_KEY_PATH.open("rb") as f:
        return serialization.load_pem_private_key(f.read(), password=None)


def _sign_pss_text(private_key: Any, text: str) -> str:
    message = text.encode("utf-8")
    signature = private_key.sign(
        message,
        padding.PSS(
            mgf=padding.MGF1(hashes.SHA256()),
            salt_length=padding.PSS.DIGEST_LENGTH,
        ),
        hashes.SHA256(),
    )
    return base64.b64encode(signature).decode("utf-8")


def _create_ws_headers(private_key: Any) -> Dict[str, str]:
    if not API_KEY_ID:
        raise RuntimeError(
            "KALSHI_API_KEY_ID env var is required for WebSocket auth")

    timestamp = str(int(time.time() * 1000))
    # Per docs: timestamp + "GET" + "/trade-api/ws/v2"
    msg_string = timestamp + "GET" + "/trade-api/ws/v2"
    signature = _sign_pss_text(private_key, msg_string)

    return {
        "Content-Type": "application/json",
        "KALSHI-ACCESS-KEY": API_KEY_ID,
        "KALSHI-ACCESS-SIGNATURE": signature,
        "KALSHI-ACCESS-TIMESTAMP": timestamp,
    }


# ---------------------------------------------------------------------------
# Job helpers
# ---------------------------------------------------------------------------

def _load_job_for_event(game_date: str, event_ticker: str) -> Job:
    jobs_path = JOBS_DIR / f"jobs_{game_date}.json"
    if not jobs_path.exists():
        raise FileNotFoundError(f"Jobs file not found: {jobs_path}")

    with jobs_path.open() as f:
        raw_jobs: List[Dict[str, Any]] = json.load(f)

    for j in raw_jobs:
        if j.get("event_ticker") == event_ticker:
            return Job(**j)

    raise ValueError(
        f"No job found for event_ticker={event_ticker} on {game_date}")


def _compute_start_time(job: Job, pregame_minutes: int) -> Optional[datetime]:
    """
    When should we start streaming?

    If tipoff_utc is known: tipoff_utc - pregame_minutes.
    If not known: return None => start immediately.
    """
    if not job.tipoff_utc:
        return None

    tipoff = datetime.fromisoformat(job.tipoff_utc)
    if tipoff.tzinfo is None:
        tipoff = tipoff.replace(tzinfo=timezone.utc)
    else:
        tipoff = tipoff.astimezone(timezone.utc)

    return tipoff - timedelta(minutes=pregame_minutes)


# ---------------------------------------------------------------------------
# Core WS loop
# ---------------------------------------------------------------------------

TERMINAL_STATUSES = {"finalized", "inactive", "settled", "closed"}


async def _run_ws_for_job(job: Job, pregame_minutes: int):
    private_key = _load_private_key()

    start_at = _compute_start_time(job, pregame_minutes)
    now = datetime.now(timezone.utc)
    if start_at and now < start_at:
        wait_secs = (start_at - now).total_seconds()
        print(
            f"[game_worker] Sleeping {wait_secs:.0f}s until pregame window "
            f"for {job.event_ticker}"
        )
        await asyncio.sleep(wait_secs)

    # data dir: src/scraper/data/<YYYY-MM-DD>/<EVENT_TICKER>/<market>.jsonl
    out_base = DATA_DIR / job.game_date / job.event_ticker
    out_base.mkdir(parents=True, exist_ok=True)

    writers: Dict[str, Any] = {}
    try:
        for mt in job.market_tickers:
            fp = out_base / f"{mt}.jsonl"
            f = fp.open("a", encoding="utf-8")
            writers[mt] = f

        # track latest status for each market
        latest_status: Dict[str, str] = {}

        while True:
            headers = _create_ws_headers(private_key)
            try:
                async with websockets.connect(WS_URL, additional_headers=headers) as ws:
                    print(
                        f"[game_worker] Connected WS for {job.event_ticker}; "
                        f"subscribing to ticker for {len(job.market_tickers)} markets"
                    )

                    sub_msg = {
                        "id": 1,
                        "cmd": "subscribe",
                        "params": {
                            "channels": ["ticker"],
                            "market_tickers": job.market_tickers,
                        },
                    }
                    await ws.send(json.dumps(sub_msg))

                    # track last time we saw ANY ticker message
                    last_ticker_ts = datetime.now(timezone.utc)

                    while True:
                        # bail out if we hit end-of-game window (if you keep end_at logic)
                        # if end_at and datetime.now(timezone.utc) >= end_at:
                        #     print(f"[game_worker] End window reached while streaming {job.event_ticker}; closing WS.")
                        #     return

                        try:
                            raw = await asyncio.wait_for(ws.recv(), timeout=30.0)
                        except asyncio.TimeoutError:
                            # no message in 30s; if we haven't seen *any* ticker in a while, reconnect
                            idle = (datetime.now(timezone.utc) -
                                    last_ticker_ts).total_seconds()
                            if idle > INACTIVITY_RECONNECT_SECS:
                                print(
                                    f"[game_worker] No ticker msgs for {job.event_ticker} in "
                                    f"{idle:.0f}s; forcing WS reconnect."
                                )
                                # break out of inner loop → outer loop will reconnect
                                break
                            else:
                                continue

                        try:
                            msg = json.loads(raw)
                        except json.JSONDecodeError:
                            continue

                        msg_type = msg.get("type")
                        if msg_type == "subscribed":
                            continue
                        if msg_type != "ticker":
                            continue

                        last_ticker_ts = datetime.now(timezone.utc)

                        payload = msg.get("msg") or {}
                        mkt = payload.get("market_ticker")
                        if not mkt or mkt not in writers:
                            continue

                        kalshi_ts = payload.get("ts")  # seconds since epoch
                        if isinstance(kalshi_ts, (int, float)):
                            ts_iso = datetime.fromtimestamp(
                                kalshi_ts, tz=timezone.utc).isoformat()
                        else:
                            ts_iso = datetime.now(timezone.utc).isoformat()

                        def norm_cents(field: str) -> Optional[float]:
                            val = payload.get(field)
                            if val is None:
                                return None
                            try:
                                return float(val) / 100.0
                            except Exception:
                                return None

                        status = payload.get("status")
                        if isinstance(status, str):
                            status_lower = status.lower()
                            latest_status[mkt] = status_lower
                        else:
                            status_lower = None

                        record = {
                            "ts_iso": ts_iso,
                            "kalshi_ts": kalshi_ts,
                            "event_ticker": job.event_ticker,
                            "market_ticker": mkt,
                            "price_prob": norm_cents("price"),
                            "yes_bid_prob": norm_cents("yes_bid"),
                            "yes_ask_prob": norm_cents("yes_ask"),
                            "volume": payload.get("volume"),
                            "open_interest": payload.get("open_interest"),
                            "status": status,
                        }

                        f = writers[mkt]
                        f.write(json.dumps(record) + "\n")
                        f.flush()

                        # If every market we care about is terminal => stop.
                        if latest_status and all(
                            latest_status.get(
                                t, "").lower() in TERMINAL_STATUSES
                            for t in job.market_tickers
                        ):
                            print(
                                f"[game_worker] All markets terminal for "
                                f"{job.event_ticker}; shutting down."
                            )
                            return

            except Exception as e:
                print(f"[game_worker] WS error for {job.event_ticker}: {e!r}")
                # reconnect after brief backoff
                await asyncio.sleep(5.0)

    finally:
        for f in writers.values():
            try:
                f.close()
            except Exception:
                pass


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Stream Kalshi ticker data for one NBA game via WebSockets."
    )
    p.add_argument(
        "--date",
        required=True,
        help="Game date in YYYY-MM-DD (must match jobs_<date>.json).",
    )
    p.add_argument(
        "--event-ticker",
        required=True,
        help="Kalshi event ticker, e.g. KXNBAGAME-25NOV22LACCHA.",
    )
    p.add_argument(
        "--pregame-minutes",
        type=int,
        default=PREGAME_MINUTES_DEFAULT,
        help=f"Minutes before tipoff to start streaming (default {PREGAME_MINUTES_DEFAULT}).",
    )
    return p.parse_args()


def main() -> None:
    args = _parse_args()
    job = _load_job_for_event(args.date, args.event_ticker)
    print(f"[game_worker] Loaded job: {job}")

    asyncio.run(
        _run_ws_for_job(
            job,
            pregame_minutes=args.pregame_minutes,
        )
    )


if __name__ == "__main__":
    main()
