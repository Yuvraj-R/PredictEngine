def compute_metrics(cash, equity_curve, trades):
    if not equity_curve:
        return {"total_pnl": 0.0, "max_drawdown": 0.0, "num_trades": 0}

    initial = equity_curve[0]["equity"]
    final = equity_curve[-1]["equity"]
    total_pnl = final - initial

    max_drawdown = _max_drawdown([p["equity"] for p in equity_curve])

    return {
        "total_pnl": total_pnl,
        "max_drawdown": max_drawdown,
        "num_trades": len(trades),
    }


def _max_drawdown(values):
    peak = values[0]
    max_dd = 0
    for v in values:
        peak = max(peak, v)
        max_dd = max(max_dd, peak - v)
    return max_dd
