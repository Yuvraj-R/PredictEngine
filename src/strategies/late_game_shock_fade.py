# src/strategies/late_game_shock_fade.py

from typing import Any, Dict, List
from .strategy import Strategy, TradeIntent


class LateGameShockFadeStrategy(Strategy):
    """
    Fade big late-game overreaction: if one side's price spikes hard
    on a small score change, buy the opposite side.
    """

    def __init__(self, params: Dict[str, Any] | None = None):
        super().__init__(params)
        self.stake: float = float(self.params.get("stake", 25.0))

        self.q_late: int = int(self.params.get("q_late", 4))
        self.t_late: float = float(self.params.get("t_late", 4.0))  # minutes
        self.max_score_diff: float = float(
            self.params.get("max_score_diff", 6.0))

        self.min_shock_move: float = float(
            self.params.get("min_shock_move", 0.15))
        self.max_delta_score_for_shock: float = float(
            self.params.get("max_delta_score_for_shock", 3.0)
        )
        self.min_spread_after_shock: float = float(
            self.params.get("min_spread_after_shock", 0.30)
        )

        self.price_min: float = float(self.params.get("price_min", 0.01))
        self.price_max: float = float(self.params.get("price_max", 0.99))

    # -------------------------
    # Helpers
    # -------------------------

    def _effective_open_price(self, m: Dict[str, Any]) -> float | None:
        """
        Mirror execution: pay ask if available, else mid, else bid.
        """
        yes_bid = m.get("yes_bid_prob")
        yes_ask = m.get("yes_ask_prob")
        mid = m.get("price")

        if yes_ask is not None:
            return yes_ask
        if mid is not None:
            return mid
        return yes_bid

    def _update_memory(
        self,
        game_mem: Dict[str, Any],
        markets: List[Dict[str, Any]],
        score_diff: float,
    ) -> None:
        last_price: Dict[str, float] = game_mem.setdefault("last_price", {})
        for m in markets:
            if not (isinstance(m, dict) and m.get("type") == "moneyline"):
                continue
            p = self._effective_open_price(m)
            if p is None:
                continue
            last_price[m["market_id"]] = p
        game_mem["last_score_diff"] = score_diff

    # -------------------------
    # Strategy interface
    # -------------------------

    def on_state(
        self,
        state: Dict[str, Any],
        portfolio: Dict[str, Any],
    ) -> List[TradeIntent]:
        intents: List[TradeIntent] = []

        quarter: int = state["quarter"]
        time_remaining: float = state["time_remaining_minutes"]
        score_diff: float = state["score_diff"]
        markets = state.get("markets") or []
        game_id = state["game_id"]

        games = self.state.setdefault("games", {})
        game_mem: Dict[str, Any] = games.setdefault(
            game_id, {"last_score_diff": None, "last_price": {}}
        )

        # Only care about late, close games
        in_window = (
            quarter >= self.q_late
            and time_remaining <= self.t_late
            and abs(score_diff) <= self.max_score_diff
        )

        last_score_diff = game_mem.get("last_score_diff")
        last_price: Dict[str, float] = game_mem.get("last_price", {})

        if in_window and last_score_diff is not None:
            moneylines = [
                m
                for m in markets
                if isinstance(m, dict) and m.get("type") == "moneyline"
            ]
            positions = portfolio.get("positions", {})

            for m in moneylines:
                mid = self._effective_open_price(m)
                if mid is None:
                    continue

                prev = last_price.get(m["market_id"])
                if prev is None:
                    continue

                delta_price = mid - prev
                if delta_price < self.min_shock_move:
                    continue

                if abs(score_diff - last_score_diff) > self.max_delta_score_for_shock:
                    continue

                # Find the opposite side market (assume exactly 2 MLs)
                others = [
                    mm
                    for mm in moneylines
                    if mm.get("market_id") != m.get("market_id")
                ]
                if len(others) != 1:
                    continue

                opp = others[0]
                opp_price = self._effective_open_price(opp)
                if opp_price is None:
                    continue

                spread = mid - opp_price
                if spread < self.min_spread_after_shock:
                    continue

                if not (self.price_min < opp_price < self.price_max):
                    continue

                opp_pos = positions.get(opp["market_id"])
                current_risk = opp_pos["dollars_at_risk"] if opp_pos else 0.0
                if current_risk != 0.0:
                    continue

                intents.append(
                    TradeIntent(
                        market_id=opp["market_id"],
                        action="open",
                        position_size=self.stake,
                    )
                )
                # One trade per tick is enough
                break

        # Always update memory at the end
        self._update_memory(game_mem, markets, score_diff)
        return intents
