from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List

STATES_DIR = Path("src/data/kalshi/merged/states")


def _sanitize_state(obj: Any) -> Dict[str, Any] | None:
    """
    Ensure we only pass clean dict states to the engine:
      - obj must be a dict
      - markets must be a list of dicts (or will be set to [])
    """
    if not isinstance(obj, dict):
        return None

    markets = obj.get("markets")

    if isinstance(markets, list):
        # keep only dict markets
        clean_markets = [m for m in markets if isinstance(m, dict)]
    else:
        clean_markets = []

    obj["markets"] = clean_markets
    return obj


def load_states_for_config(config: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Load and concatenate states based on config.

    Config:
      - config["game_ids"] = list of GAME_ID strings (optional).

    If game_ids is omitted or empty:
      → load ALL *.json in STATES_DIR.
    """
    game_ids = config.get("game_ids")

    if not game_ids:
        game_ids = sorted(p.stem for p in STATES_DIR.glob("*.json"))

    states: List[Dict[str, Any]] = []

    for gid in game_ids:
        path = STATES_DIR / f"{gid}.json"
        if not path.exists():
            print(
                f"[load_states] WARNING: missing states file for GAME_ID={gid}: {path}")
            continue

        with open(path, "r") as f:
            raw = json.load(f)

        if not isinstance(raw, list):
            print(
                f"[load_states] WARNING: states file not a list for GAME_ID={gid}")
            continue

        for obj in raw:
            clean = _sanitize_state(obj)
            if clean is not None:
                states.append(clean)

    print(f"[load_states] Loaded {len(states)} sanitized states "
          f"from {len(game_ids)} games.")
    return states
