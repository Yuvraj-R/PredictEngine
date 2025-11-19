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

def _open_position(intent, price, state, portfolio: PortfolioState):
    size = intent.position_size
    contracts = size / price

    pos = Position(intent.market_id, contracts, price)
    portfolio.positions[intent.market_id] = pos
    portfolio.cash -= size

    portfolio.trade_log.append(
        Trade(
            timestamp=state["timestamp"],
            market_id=intent.market_id,
            action="open",
            price=price,
            contracts=contracts,
            pnl=0.0,
        )
    )


def _close_position(market_id, price, state, portfolio: PortfolioState, auto=False):
    pos = portfolio.positions.get(market_id)
    if not pos:
        return

    proceeds = pos.contracts * price
    pnl = pos.contracts * (price - pos.entry_price)

    portfolio.cash += proceeds
    del portfolio.positions[market_id]

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
