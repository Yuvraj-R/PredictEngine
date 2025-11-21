from typing import Dict, List
from .portfolio import PortfolioState
from .execution import apply_intent, auto_settle
from .metrics import compute_metrics
from .models import BacktestResult
from data.merged.load_states import load_states_for_config


def run_backtest(strategy, config: Dict) -> BacktestResult:
    portfolio = PortfolioState()
    equity_curve = []

    # Load real states
    states: List[Dict] = load_states_for_config(config)

    for idx, state in enumerate(states):
        equity_before = _compute_equity(state, portfolio)
        portfolio_view = portfolio.get_portfolio_view(equity_before)

        # Strategy decisions
        intents = strategy.on_state(state, portfolio_view)
        for intent in intents:
            apply_intent(intent, state, portfolio)

        # ------------------------------------------------------
        # PATCH: Detect end-of-game & assign market["result"]
        # ------------------------------------------------------
        is_final_state = False

        # Heuristic: final snapshot of a game has quarter >= 4 AND decreasing timestamps
        if idx + 1 < len(states):
            curr_gid = state["game_id"]
            next_gid = states[idx+1]["game_id"]

            # Game changes → last state of this game
            if curr_gid != next_gid:
                is_final_state = True
        else:
            # Last overall state in the dataset
            is_final_state = True

        if is_final_state:
            score_home = state.get("score_home")
            score_away = state.get("score_away")

            # Determine winner
            if score_home is not None and score_away is not None:
                if score_home > score_away:
                    winning_team = state["home_team"]
                elif score_away > score_home:
                    winning_team = state["away_team"]
                else:
                    winning_team = None  # rare: tie / unresolved

                # Assign yes/no to each market
                for m in state.get("markets", []):
                    team = m.get("team")
                    if winning_team is None:
                        m["result"] = None
                    else:
                        m["result"] = "yes" if team == winning_team else "no"

        # ------------------------------------------------------
        # Auto-settle uses the result values we just added.
        # ------------------------------------------------------
        auto_settle(state, portfolio)

        equity_after = _compute_equity(state, portfolio)
        equity_curve.append(
            {"timestamp": state["timestamp"], "equity": equity_after})

    summary = compute_metrics(
        portfolio.cash, equity_curve, portfolio.trade_log)

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

        mtm_price = market.get("price")

        # If we don't have a usable price here, skip MTM for this tick
        if mtm_price is None:
            continue

        equity += pos.contracts * mtm_price

    return equity


def _get_market(state, market_id):
    for m in state["markets"]:
        if m["market_id"] == market_id:
            return m
    return None
