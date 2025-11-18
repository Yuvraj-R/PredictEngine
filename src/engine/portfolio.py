from typing import Dict, List
from .models import Position, Trade


class PortfolioState:
    def __init__(self):
        self.cash: float = 0.0
        self.positions: Dict[str, Position] = {}
        self.trade_log: List[Trade] = []

    def get_portfolio_view(self) -> Dict[str, float]:
        """
        Returns strategy-facing dict: {market_id: dollars_at_risk}
        """
        view = {}
        for mid, pos in self.positions.items():
            view[mid] = pos.contracts * pos.entry_price
        return view
