from __future__ import annotations

import argparse
import json
from dataclasses import dataclass, asdict
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests
from nba_api.stats.endpoints import scoreboardv2
from zoneinfo import ZoneInfo

KALSHI_BASE_URL = "https://api.elections.kalshi.com/trade-api/v2"
SERIES_TICKER = "KXNBAGAME"
JOBS_DIR = Path(__file__).resolve().parent / "jobs"

# Static mapping from NBA TEAM_ID -> standard abbreviation
TEAM_ID_TO_ABBREV: Dict[int, str] = {
    # East
    1610612737: "ATL",
    1610612738: "BOS",
    1610612751: "BKN",
    1610612766: "CHA",
    1610612741: "CHI",
    1610612739: "CLE",
    1610612765: "DET",
    1610612754: "IND",
    1610612748: "MIA",
    1610612749: "MIL",
    1610612752: "NYK",
    1610612753: "ORL",
    1610612755: "PHI",
    1610612761: "TOR",
    1610612764: "WAS",
    # West
    1610612742: "DAL",
    1610612743: "DEN",
    1610612744: "GSW",
    1610612745: "HOU",
    1610612747: "LAL",
    1610612746: "LAC",
    1610612763: "MEM",
    1610612750: "MIN",
    1610612740: "NOP",
    1610612760: "OKC",
    1610612756: "PHX",
    1610612757: "POR",
    1610612758: "SAC",
    1610612759: "SAS",
    1610612762: "UTA",
}


@dataclass
class Job:
    game_date: str        # YYYY-MM-DD
    game_id: str
    home_team: str
    away_team: str
    tipoff_utc: Optional[str]   # ISO string or None
    event_ticker: str
    market_tickers: List[str]


# --------------------------------------------------------------------
# NBA side
# --------------------------------------------------------------------

def _parse_tipoff_utc(game_date: date, status_text: str) -> Optional[str]:
    """
    GAME_STATUS_TEXT is usually:
      - "7:30 pm ET" (pre-game / scheduled)
      - "Final" / "In Progress" / etc.

    We only try to parse "HH:MM am/pm ET" formats.
    """
    status = status_text.strip()
    lower = status.lower()
    if "et" not in lower or ":" not in status:
        return None

    # Strip trailing "ET" and normalize
    txt = status.replace("ET", "").replace("et", "").strip()
    parts = txt.split()
    if len(parts) < 2:
        return None

    time_part = parts[0]           # "7:30"
    ampm = parts[1].upper()        # "PM"

    try:
        dt_str = f"{game_date.isoformat()} {time_part} {ampm}"
        dt_local = datetime.strptime(dt_str, "%Y-%m-%d %I:%M %p")
        dt_et = dt_local.replace(tzinfo=ZoneInfo("America/New_York"))
        dt_utc = dt_et.astimezone(ZoneInfo("UTC"))
        return dt_utc.isoformat()
    except Exception:
        return None


def _fetch_nba_games_for_date(target_date: date) -> List[Dict[str, Any]]:
    """
    Use nba_api ScoreboardV2 to get all NBA games for a given date.
    Returns a list of dicts:
      {game_id, home_team, away_team, tipoff_utc, status_text}
    """
    # ScoreboardV2 wants MM/DD/YYYY for game_date
    ds = target_date.strftime("%m/%d/%Y")
    print(f"[discover_games] fetching NBA scoreboard for {ds}")

    sb = scoreboardv2.ScoreboardV2(game_date=ds, league_id="00", day_offset=0)
    data = sb.get_normalized_dict()
    headers = data.get("GameHeader", []) or []

    games: List[Dict[str, Any]] = []

    for row in headers:
        game_id = row.get("GAME_ID")
        home_id = row.get("HOME_TEAM_ID")
        away_id = row.get("VISITOR_TEAM_ID")
        status_text = (row.get("GAME_STATUS_TEXT") or "").strip()

        if not game_id or home_id is None or away_id is None:
            continue

        home_abbrev = TEAM_ID_TO_ABBREV.get(int(home_id))
        away_abbrev = TEAM_ID_TO_ABBREV.get(int(away_id))

        if not home_abbrev or not away_abbrev:
            print(
                f"[discover_games] missing abbrev for "
                f"home_id={home_id}, away_id={away_id}; skipping"
            )
            continue

        tipoff_utc = _parse_tipoff_utc(target_date, status_text)

        games.append(
            {
                "game_id": game_id,
                "home_team": home_abbrev,
                "away_team": away_abbrev,
                "tipoff_utc": tipoff_utc,
                "status_text": status_text,
            }
        )

    print(
        f"[discover_games] NBA games found for {target_date.isoformat()}: {len(games)}")
    return games


# --------------------------------------------------------------------
# Kalshi side
# --------------------------------------------------------------------

def _fetch_kalshi_events() -> List[Dict[str, Any]]:
    """
    Fetch all NBA (KXNBAGAME) events with nested markets.
    We don't filter by date here; we'll do that ourselves.
    """
    url = f"{KALSHI_BASE_URL}/events"
    params = {
        "series_ticker": SERIES_TICKER,
        "with_nested_markets": "true",
        "status": "open",
        "limit": 200,
    }

    print(f"[discover_games] fetching Kalshi events from {url}")
    resp = requests.get(url, params=params, timeout=10)
    resp.raise_for_status()
    data = resp.json()

    # API sometimes returns {"events": [...]} and sometimes a bare list.
    if isinstance(data, dict):
        events = data.get("events", []) or []
    elif isinstance(data, list):
        events = data
    else:
        events = []

    print(f"[discover_games] Kalshi events fetched: {len(events)}")
    return events


def _parse_nba_event_ticker(event_ticker: str) -> Optional[Tuple[date, str, str]]:
    """
    Parse an NBA event ticker like:
      KXNBAGAME-25NOV22LACCHA

    Kalshi encoding is: yyMMMddTEAMAWAYTEAMHOME
      "25NOV22" -> Nov 22, 2025

    Returns:
      (date(2025, 11, 22), away_abbrev, home_abbrev)
    """
    try:
        _, suffix = event_ticker.split("-", 1)
    except ValueError:
        return None

    if len(suffix) < 13:
        return None

    date_code = suffix[:7]  # "25NOV22"
    yy_str = date_code[:2]
    mon_str = date_code[2:5]
    dd_str = date_code[5:7]

    try:
        yy = int(yy_str)
        day = int(dd_str)
        # Map "NOV" etc to month int via datetime
        month = datetime.strptime(mon_str, "%b").month
        year = 2000 + yy           # assume 20xx
        event_dt = date(year, month, day)
    except Exception:
        return None

    away = suffix[7:10]
    home = suffix[10:13]
    return event_dt, away, home


def _extract_winner_market_tickers(event: Dict[str, Any]) -> List[str]:
    """
    From an event with nested markets, return the two moneyline 'Winner?' markets.
    """
    markets = event.get("markets") or []
    tickers: List[str] = []

    for m in markets:
        if m.get("market_type") != "binary":
            continue
        title = (m.get("title") or "").lower()
        if "winner" not in title:
            continue
        ticker = m.get("ticker")
        if ticker:
            tickers.append(ticker)

    return tickers


def _index_kalshi_events_for_date(
    events: List[Dict[str, Any]],
    target_date: date,
) -> Dict[Tuple[str, str], Dict[str, Any]]:
    """
    Build an index:
      (home_team, away_team) -> event
    for events that match the given calendar date.
    """
    idx: Dict[Tuple[str, str], Dict[str, Any]] = {}
    for e in events:
        et = e.get("event_ticker") or e.get("ticker")
        if not et:
            continue

        parsed = _parse_nba_event_ticker(et)
        if not parsed:
            continue

        event_dt, away, home = parsed
        if event_dt != target_date:
            continue

        idx[(home, away)] = e

    print(
        f"[discover_games] Kalshi events matching {target_date.isoformat()}: "
        f"{len(idx)} (by home/away)"
    )
    return idx


# --------------------------------------------------------------------
# Job discovery
# --------------------------------------------------------------------

def discover_jobs_for_date(target_date: date) -> List[Job]:
    print(f"[discover_games] discovering jobs for {target_date.isoformat()}")

    nba_games = _fetch_nba_games_for_date(target_date)
    events = _fetch_kalshi_events()
    events_index = _index_kalshi_events_for_date(events, target_date)
    print(events_index)  # keep for now; remove later if too noisy

    jobs: List[Job] = []

    for g in nba_games:
        key = (g["home_team"], g["away_team"])
        event = events_index.get(key)
        status_text = g.get("status_text", "")

        if not event:
            print(
                f"[discover_games] no Kalshi event found for "
                f"{g['away_team']} @ {g['home_team']} (status={status_text})"
            )
            continue

        event_ticker = event.get("event_ticker") or event.get("ticker")
        market_tickers = _extract_winner_market_tickers(event)

        if len(market_tickers) != 2:
            print(
                f"[discover_games] skipping {event_ticker}: "
                f"expected 2 winner markets, got {len(market_tickers)}"
            )
            continue

        job = Job(
            game_date=target_date.isoformat(),
            game_id=g["game_id"],
            home_team=g["home_team"],
            away_team=g["away_team"],
            tipoff_utc=g["tipoff_utc"],
            event_ticker=event_ticker,
            market_tickers=market_tickers,
        )
        jobs.append(job)

    print(f"[discover_games] total jobs discovered: {len(jobs)}")
    return jobs


def _write_jobs_file(jobs: List[Job], target_date: date) -> Path:
    JOBS_DIR.mkdir(parents=True, exist_ok=True)
    out_path = JOBS_DIR / f"jobs_{target_date.isoformat()}.json"

    payload = [asdict(j) for j in jobs]

    with open(out_path, "w") as f:
        json.dump(payload, f, indent=2)

    print(f"[discover_games] wrote {len(jobs)} jobs to {out_path}")
    return out_path


# --------------------------------------------------------------------
# CLI
# --------------------------------------------------------------------

def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Discover NBA/Kalshi jobs for a given date."
    )
    p.add_argument(
        "--date",
        required=True,
        help="Target date in YYYY-MM-DD (NBA calendar date, Eastern).",
    )
    return p.parse_args()


def main() -> None:
    args = _parse_args()
    target = date.fromisoformat(args.date)

    try:
        jobs = discover_jobs_for_date(target)
    except Exception as e:
        print(f"[discover_games] ERROR during discovery: {e!r}")
        raise

    _write_jobs_file(jobs, target)


if __name__ == "__main__":
    main()
