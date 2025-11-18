from dataclasses import dataclass
from typing import List, Dict, Optional


@dataclass
class Position:
    market_id: str
    contracts: float
    entry_price: float


@dataclass
class Trade:
    timestamp: str
    market_id: str
    action: str          # "open" | "close" | "auto_close"
    price: float
    contracts: float
    pnl: float


@dataclass
class BacktestResult:
    summary: Dict[str, float]
    trades: List[Trade]
    equity_curve: List[Dict[str, float]]  # [{"timestamp":..., "equity":...}]
