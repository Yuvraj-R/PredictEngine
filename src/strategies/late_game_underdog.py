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
        portfolio: Dict[str, float],
    ) -> List[TradeIntent]:
        intents: List[TradeIntent] = []

        time_remaining: float = state["time_remaining_minutes"]
        score_diff: int = state["score_diff"]
        markets = state["markets"]

        # Find moneyline markets for this game
        moneyline_markets = [
            m for m in markets if m.get("type") == "moneyline"]
        if not moneyline_markets:
            return intents  # nothing to do

        # Define "underdog" as the side with the lowest price
        underdog = min(moneyline_markets, key=lambda m: m["price"])
        underdog_market_id = underdog["market_id"]
        implied_win_prob: float = underdog["price"]

        current_position: float = portfolio.get(underdog_market_id, 0.0)

        # Entry: under 10 minutes, close score, market < 25%, no existing position
        if (
            time_remaining < 10.0
            and score_diff <= 6
            and implied_win_prob < 0.25
            and current_position == 0.0
        ):
            intents.append(
                TradeIntent(
                    market_id=underdog_market_id,
                    action="open",
                    position_size=100.00,
                )
            )

        # No explicit exit rule; positions are held to settlement.
        return intents
