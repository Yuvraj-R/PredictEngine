import math
from .models import Position, Trade
from .portfolio import PortfolioState


def apply_intent(intent, state, portfolio: PortfolioState):
    market = _get_market(state, intent.market_id)
    if market is None:
        return  # placeholder: skip missing market data

    price = market["price"]

    if intent.action == "open":
        _open_position(intent, price, state, portfolio)

    elif intent.action == "close":
        _close_position(intent.market_id, price, state, portfolio)


def auto_settle(state, portfolio: PortfolioState):
    for mid, pos in list(portfolio.positions.items()):
        market = _get_market(state, mid)
        if not market:
            continue

        result = market.get("result")
        if result not in ("yes", "no"):
            continue

        settlement_price = 1.0 if result == "yes" else 0.0
        _close_position(mid, settlement_price, state, portfolio, auto=True)


# -------------------------
# Internal helpers
# -------------------------

def _calc_fee(contracts: float, price: float, fee_rate: float = 0.07) -> float:
    """
    Kalshi taker fee:
      fee = round_up( fee_rate * C * P * (1 - P) )
    where P is 0–1 in our engine.
    """
    if contracts <= 0 or price is None or price <= 0 or price >= 1:
        return 0.0

    raw = fee_rate * contracts * price * (1.0 - price)
    # round up to next cent
    return math.ceil(raw * 100.0) / 100.0


def _open_position(intent, price, state, portfolio: PortfolioState):
    size = intent.position_size
    if price is None or price <= 0:
        return

    contracts = size / price
    open_fee = _calc_fee(contracts, price)

    pos = Position(intent.market_id, contracts, price, open_fee)
    portfolio.positions[intent.market_id] = pos

    # pay cost + opening fee
    portfolio.cash -= (size + open_fee)

    portfolio.trade_log.append(
        Trade(
            timestamp=state["timestamp"],
            market_id=intent.market_id,
            action="open",
            price=price,
            contracts=contracts,
            pnl=0.0,  # PnL realized at close; fee handled via cash + close PnL
        )
    )


def _close_position(market_id, price, state, portfolio: PortfolioState, auto=False):
    pos = portfolio.positions.get(market_id)
    if not pos or price is None:
        return

    close_fee = _calc_fee(pos.contracts, price)

    proceeds = pos.contracts * price
    portfolio.cash += (proceeds - close_fee)
    del portfolio.positions[market_id]

    # round-trip PnL including both open + close fees
    pnl = pos.contracts * (price - pos.entry_price) - pos.open_fee - close_fee

    portfolio.trade_log.append(
        Trade(
            timestamp=state["timestamp"],
            market_id=market_id,
            action="auto_close" if auto else "close",
            price=price,
            contracts=pos.contracts,
            pnl=pnl,
        )
    )


def _get_market(state, market_id):
    for m in state["markets"]:
        if m["market_id"] == market_id:
            return m
    return None
