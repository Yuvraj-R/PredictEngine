from typing import Any, Dict, List
from .strategy import Strategy, TradeIntent


class UnderdogResilienceStrategy(Strategy):
    """
    If a strong pre-game favorite is in a close game in Q2–Q3, but the
    market is still pessimistic on the pre-game underdog, buy the underdog ML.
    """

    def __init__(self, params: Dict[str, Any] | None = None):
        super().__init__(params)

        self.stake: float = self.params.get("stake", 25.0)

        # Underdog had to be a real dog pre-game (<= this implied prob)
        self.pregame_underdog_max: float = self.params.get(
            "pregame_underdog_max", 0.40
        )

        # In-game: market still pessimistic on the dog
        self.current_underdog_max: float = self.params.get(
            "current_underdog_max", 0.45
        )

        # Score must be reasonably close
        self.max_score_diff: float = self.params.get("max_score_diff", 6.0)

        # Always avoid degenerate prices
        self.price_min: float = self.params.get("price_min", 0.01)
        self.price_max: float = self.params.get("price_max", 0.99)

        # internal per-game memory
        # games[game_id] = {
        #   "favorite_market_id": str,
        #   "underdog_market_id": str,
        #   "underdog_pre_price": float,
        #   "skip": bool,
        # }
        self.state.setdefault("games", {})

    # -------------------------
    # Helpers
    # -------------------------

    def _effective_price(self, m: Dict[str, Any]) -> float | None:
        yes_bid = m.get("yes_bid_prob")
        yes_ask = m.get("yes_ask_prob")
        mid = m.get("price")

        if mid is not None:
            return mid
        if yes_ask is not None:
            return yes_ask
        return yes_bid

    def _ensure_game_info(
        self,
        game_id: str,
        markets: List[Dict[str, Any]],
    ) -> Dict[str, Any] | None:
        games: Dict[str, Dict[str, Any]] = self.state["games"]

        if game_id in games:
            return games[game_id]

        # First time we see this game: classify favorite vs underdog
        ml_markets: List[Dict[str, Any]] = []
        for m in markets:
            if not (isinstance(m, dict) and m.get("type") == "moneyline"):
                continue
            p = self._effective_price(m)
            if p is None:
                continue
            m["_p0"] = p
            ml_markets.append(m)

        if len(ml_markets) < 2:
            # Not enough info, skip this game
            info = {"skip": True}
            games[game_id] = info
            return info

        # Favorite = higher implied prob; underdog = lower implied prob
        favorite = max(ml_markets, key=lambda x: x["_p0"])
        underdog = min(ml_markets, key=lambda x: x["_p0"])

        underdog_pre_price = float(underdog["_p0"])

        info = {
            "favorite_market_id": favorite["market_id"],
            "underdog_market_id": underdog["market_id"],
            "underdog_pre_price": underdog_pre_price,
            "skip": False,
        }

        # Only trade games where the dog was clearly a dog pre-game
        if underdog_pre_price > self.pregame_underdog_max:
            info["skip"] = True

        games[game_id] = info
        return info

    # -------------------------
    # Strategy interface
    # -------------------------

    def on_state(
        self,
        state: Dict[str, Any],
        portfolio: Dict[str, Any],
    ) -> List[TradeIntent]:
        intents: List[TradeIntent] = []

        game_id = state["game_id"]
        markets = state.get("markets") or []

        game_info = self._ensure_game_info(game_id, markets)
        if not game_info or game_info.get("skip"):
            return intents

        underdog_mid = game_info["underdog_market_id"]
        underdog_pre_price = game_info["underdog_pre_price"]

        quarter: int = state["quarter"]
        time_remaining: float = state["time_remaining_minutes"]
        score_diff: float = abs(state["score_diff"])

        # Mid-game only: Q2–Q3, avoid super-early and super-late spots
        if quarter < 2 or quarter > 3:
            return intents

        # Optional: avoid final minute craziness in Q3
        if quarter == 3 and time_remaining < 1.0:
            return intents

        # Game must be reasonably close
        if score_diff > self.max_score_diff:
            return intents

        # Locate the underdog market in this state
        m = next(
            (m for m in markets if m.get("market_id") == underdog_mid),
            None,
        )
        if not m:
            return intents

        p_curr = self._effective_price(m)
        if p_curr is None or not (self.price_min < p_curr < self.price_max):
            return intents

        # Market still pessimistic on the dog
        if p_curr > self.current_underdog_max:
            return intents

        # Also require that the price hasn't moved too far *up* vs pre-game
        # (i.e., the crowd hasn't fully updated yet).
        if p_curr > underdog_pre_price + 0.10:
            return intents

        positions = portfolio.get("positions", {})
        pos_info = positions.get(underdog_mid)
        current_risk = pos_info["dollars_at_risk"] if pos_info else 0.0

        if current_risk > 0.0:
            # already in this market
            return intents

        intents.append(
            TradeIntent(
                market_id=underdog_mid,
                action="open",
                position_size=self.stake,
            )
        )

        return intents
