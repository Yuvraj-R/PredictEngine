from typing import Any, Dict, List
from .strategy import Strategy, TradeIntent


class LateGameUnderdogStrategy(Strategy):
    """
    Buy the underdog moneyline late in the game when the score is close
    and the market is pessimistic. Let the position settle at game end.
    """

    def on_state(
        self,
        state: Dict[str, Any],
        portfolio: Dict[str, Any],
    ) -> List[TradeIntent]:
        intents: List[TradeIntent] = []

        time_remaining: float = state["time_remaining_minutes"]
        score_diff: int = state["score_diff"]
        markets = state["markets"]

        moneyline_markets = [
            m for m in markets if m.get("type") == "moneyline"]
        if not moneyline_markets:
            return intents

        underdog = min(moneyline_markets, key=lambda m: m["price"])
        underdog_market_id = underdog["market_id"]
        implied_win_prob: float = underdog["price"]

        positions = portfolio.get("positions", {})
        pos_info = positions.get(underdog_market_id)
        current_risk = pos_info["dollars_at_risk"] if pos_info else 0.0

        # Simple v1 sizing: fixed $100 per trade
        stake = 100.0

        if (
            time_remaining < 10.0
            and score_diff <= 6
            and implied_win_prob < 0.25
            and current_risk == 0.0
        ):
            intents.append(
                TradeIntent(
                    market_id=underdog_market_id,
                    action="open",
                    position_size=stake,
                )
            )

        return intents
