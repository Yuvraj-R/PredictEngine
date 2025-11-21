def compute_metrics(cash, equity_curve, trades):
    if not equity_curve:
        return {
            "total_pnl": 0.0,
            "max_drawdown": 0.0,
            "num_trades": 0,
            "num_round_trips": 0,
            "num_closing_trades": 0,
            "hit_rate": 0.0,
            "avg_win": 0.0,
            "avg_loss": 0.0,
            "buckets": {},
        }

    # --- Basic PnL / drawdown ---
    initial = equity_curve[0]["equity"]
    final = equity_curve[-1]["equity"]
    total_pnl = final - initial
    max_drawdown = _max_drawdown([p["equity"] for p in equity_curve])

    # --- Build round trips (open -> close/auto_close) ---
    # We assume at most one open per market_id at a time.
    open_by_market = {}
    round_trips = []

    for t in trades:
        action = getattr(t, "action", None)
        market_id = getattr(t, "market_id", None)

        if action == "open":
            open_by_market[market_id] = t
        elif action in ("close", "auto_close") and market_id in open_by_market:
            open_t = open_by_market.pop(market_id)
            entry_price = getattr(open_t, "price", None)
            exit_price = getattr(t, "price", None)
            pnl = getattr(t, "pnl", 0.0)

            round_trips.append(
                {
                    "market_id": market_id,
                    "entry_price": entry_price,
                    "exit_price": exit_price,
                    "pnl": pnl,
                }
            )

    # --- Trade-level stats (on round trips) ---
    wins = []
    losses = []

    import numpy as np

    step = 0.05
    bucket_edges = np.arange(0, 1.0, step)
    bucket_names = []
    for i, edge in enumerate(bucket_edges):
        lower = edge
        upper = lower + step
        name = f"{lower:.2f}-{upper:.2f}"
        bucket_names.append(name)

    buckets = {name: [] for name in bucket_names}

    for rt in round_trips:
        pnl = rt["pnl"]
        entry_price = rt["entry_price"]

        if pnl > 0:
            wins.append(pnl)
        elif pnl < 0:
            losses.append(pnl)

        if entry_price is not None:
            placed = False
            for i, edge in enumerate(bucket_edges):
                lower = edge
                upper = lower + step
                if lower <= entry_price < upper:
                    buckets[bucket_names[i]].append(pnl)
                    placed = True
                    break

    num_round_trips = len(round_trips)
    num_closes = num_round_trips  # each round trip ends with a close
    hit_rate = (len(wins) / num_closes) if num_closes else 0.0
    avg_win = (sum(wins) / len(wins)) if wins else 0.0
    avg_loss = (sum(losses) / len(losses)) if losses else 0.0

    # --- Bucket stats ---
    bucket_stats = {}
    for name, arr in buckets.items():
        if arr:
            wins_b = [x for x in arr if x > 0]
            losses_b = [x for x in arr if x < 0]
            total = len(arr)
            bucket_stats[name] = {
                "count": total,
                "hit_rate": len(wins_b) / total if total else 0.0,
                "avg_pnl": sum(arr) / total if total else 0.0,
                "avg_win": (sum(wins_b) / len(wins_b)) if wins_b else 0.0,
                "avg_loss": (sum(losses_b) / len(losses_b)) if losses_b else 0.0,
            }
        else:
            bucket_stats[name] = {
                "count": 0,
                "hit_rate": 0.0,
                "avg_pnl": 0.0,
                "avg_win": 0.0,
                "avg_loss": 0.0,
            }

    return {
        "total_pnl": total_pnl,
        "max_drawdown": max_drawdown,
        "num_trades": len(trades),        # events (opens + closes)
        "num_round_trips": num_round_trips,
        "num_closing_trades": num_closes,
        "hit_rate": hit_rate,
        "avg_win": avg_win,
        "avg_loss": avg_loss,
        "buckets": bucket_stats,
    }


def _max_drawdown(values):
    peak = values[0]
    max_dd = 0
    for v in values:
        peak = max(peak, v)
        max_dd = max(max_dd, peak - v)
    return max_dd
