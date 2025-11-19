from typing import Dict, List
from .portfolio import PortfolioState
from .execution import apply_intent, auto_settle
from .metrics import compute_metrics
from .models import BacktestResult


def run_backtest(strategy, config: Dict) -> BacktestResult:
    portfolio = PortfolioState()
    equity_curve = []

    # TODO: load real data from DB
    states: List[Dict] = []
    # states = load_data_for_config(config)

    for state in states:
        # Equity before making new decisions
        equity_before = _compute_equity(state, portfolio)

        # Strategy-facing portfolio snapshot
        portfolio_view = portfolio.get_portfolio_view(equity_before)

        # Strategy returns TradeIntents
        intents = strategy.on_state(state, portfolio_view)

        for intent in intents:
            apply_intent(intent, state, portfolio)

        # Auto settle resolved markets
        auto_settle(state, portfolio)

        # Equity after applying intents + settlements
        equity_after = _compute_equity(state, portfolio)
        equity_curve.append(
            {"timestamp": state["timestamp"], "equity": equity_after})

    summary = compute_metrics(
        portfolio.cash, equity_curve, portfolio.trade_log)

    # TODO: persist results to DB
    # save_backtest_to_db(config, summary, portfolio.trade_log)

    return BacktestResult(
        summary=summary,
        trades=portfolio.trade_log,
        equity_curve=equity_curve,
    )


def _compute_equity(state, portfolio):
    equity = portfolio.cash

    for pos in portfolio.positions.values():
        market = _get_market(state, pos.market_id)
        if not market:
            continue
        mtm_price = market["price"]
        equity += pos.contracts * mtm_price

    return equity


def _get_market(state, market_id):
    for m in state["markets"]:
        if m["market_id"] == market_id:
            return m
    return None
