from flask import Blueprint, request, jsonify
from engine.backtest import run_backtest
from strategies.late_game_underdog import LateGameUnderdogStrategy

bp = Blueprint("backtests", __name__)


STRATEGY_REGISTRY = {
    "late_game_underdog": LateGameUnderdogStrategy,
}


@bp.route("/backtests", methods=["POST"])
def create_backtest():
    data = request.json or {}

    strategy_name = data.get("strategy")
    params = data.get("params", {})
    config = data.get("config", {})

    StrategyClass = STRATEGY_REGISTRY.get(strategy_name)
    if not StrategyClass:
        return jsonify({"error": "unknown strategy"}), 400

    strategy = StrategyClass(params)

    result = run_backtest(strategy, config)

    # Return serialized
    return jsonify({
        "summary": result.summary,
        "trades": [t.__dict__ for t in result.trades],
        "equity_curve": result.equity_curve,
    })
