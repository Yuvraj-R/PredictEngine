# src/app/backtests.py

from pathlib import Path
import datetime as dt
import json
import csv

from flask import Blueprint, request, jsonify
from engine.backtest import run_backtest
from strategies.late_game_underdog import LateGameUnderdogStrategy

bp = Blueprint("backtests", __name__)

STRATEGY_REGISTRY = {
    "late_game_underdog": LateGameUnderdogStrategy,
}


def _save_backtest_run(
    strategy_name: str,
    config: dict,
    result,
) -> str:
    """
    Persist a single backtest run under:

      src/data/backtest_runs/<strategy_name>/<run_id>/

    Files:
      - summary.json        (metrics)
      - config.json         (what was run)
      - trades.csv          (full trade log)
      - equity_curve.csv    (timestamp, equity)
    """

    base_dir = Path("src/data/backtest_runs") / strategy_name
    base_dir.mkdir(parents=True, exist_ok=True)

    # Run ID: UTC timestamp, filesystem-safe (no colons)
    run_id = dt.datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    run_dir = base_dir / run_id
    run_dir.mkdir(parents=True, exist_ok=True)

    # 1) summary.json
    summary_path = run_dir / "summary.json"
    with open(summary_path, "w") as f:
        json.dump(result.summary, f, indent=2)

    # 2) config.json
    config_path = run_dir / "config.json"
    with open(config_path, "w") as f:
        json.dump(config or {}, f, indent=2)

    # 3) trades.csv
    trades_path = run_dir / "trades.csv"
    with open(trades_path, "w", newline="") as f:
        fieldnames = ["timestamp", "market_id",
                      "action", "price", "contracts", "pnl"]
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for t in result.trades:
            writer.writerow({
                "timestamp": t.timestamp,
                "market_id": t.market_id,
                "action": t.action,
                "price": t.price,
                "contracts": t.contracts,
                "pnl": t.pnl,
            })

    # 4) equity_curve.csv
    equity_path = run_dir / "equity_curve.csv"
    with open(equity_path, "w", newline="") as f:
        if result.equity_curve:
            # ["timestamp", "equity"]
            fieldnames = list(result.equity_curve[0].keys())
        else:
            fieldnames = ["timestamp", "equity"]
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in result.equity_curve:
            writer.writerow(row)

    return run_id


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

    # Persist full run to disk
    run_id = _save_backtest_run(strategy_name, config, result)

    # Return only light summary + metadata
    response = {
        "strategy": strategy_name,
        "run_id": run_id,
        "summary": result.summary,
        "num_trades": len(result.trades),
    }

    return jsonify(response)
