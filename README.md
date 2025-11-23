# 🏀 Kalshi NBA Prediction Market Backtester

**Kalshi NBA Prediction Market Backtester** is a Python-based engine that:

* Streams **live Kalshi NBA moneyline ticks** via WebSockets on a GCP VM
* Aligns them with **NBA play-by-play game state** (score, time, quarter)
* Builds per-tick **game snapshots** and runs **rule-based strategies** over real historical data

---

## 🚀 Key Features

* **Live Kalshi Collector**

  * One WebSocket worker per NBA game
  * Writes tick data to JSONL under `src/scraper/data/<YYYY-MM-DD>/<EVENT_TICKER>/`
  * Driven by a daily `systemd` timer on GCP (no manual babysitting)

* **NBA Game State Timelines**

  * Uses NBA APIs to build per-game timelines (score, quarter, time remaining)
  * Stored under `src/data/nba/game_states/<season_tag>/`

* **Merged Engine States**

  * For each Kalshi tick, attaches the **latest known NBA game state at or before that timestamp** (no look-ahead)
  * Outputs engine-ready states as JSON lists under `src/data/merged/states/<GAME_ID>.json`

* **Backtesting Engine + Strategies**

  * Core engine in `src/engine/` (portfolio, execution, metrics)
  * Pluggable strategies in `src/strategies/` (e.g. late-game underdog heuristics)
  * Backtest runs (config, summary, equity curve, trades) saved in `src/data/backtest_runs/`

---

## 💻 Tech Stack

* **Python**
* **Data & APIs:** `requests`, `nba_api`, `pandas`
* **Streaming:** `websockets`, `cryptography` (Kalshi auth)
* **Infra:** GCP Compute Engine + `systemd` services/timers

---

## 🔁 Daily Flow (High Level)

1. **Early AM:** `daily_collect.py` runs on GCP → starts WebSocket workers for that day’s NBA slate.
2. **Next Morning (~6am):**

   * Refresh NBA games index (`fetch_nba_games.py`)
   * Build game state timelines (`build_game_states_batch.py`)
   * Merge NBA game states + Kalshi ticks (`build_states_from_scraper.py`) → engine states
3. **Backtest:** Run strategies over the merged states and inspect the run artifacts in `src/data/backtest_runs/`.

---

Project is **actively in development** — APIs and file layout may still change as more strategies and markets are added.
