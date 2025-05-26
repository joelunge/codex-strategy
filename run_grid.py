import argparse
import itertools
from multiprocessing import Pool, cpu_count

import pandas as pd

from backtests.run_backtest import load_data
from strategies.vol_breakout import VolBreakout
from backtests.core import cagr, max_drawdown, sharpe_ratio
from utils.db import db_conn


def run_combo(args):
    symbol, lookback, range_pct, breakout_pct, start, end = args
    conn = db_conn()
    try:
        df = load_data(conn, symbol, start, end)
    finally:
        conn.close()
    strat = VolBreakout(
        lookback=lookback,
        range_threshold=range_pct / 100,
        breakout_threshold=breakout_pct / 100,
    )
    trades, equity = strat.simulate(df)
    return {
        "symbol": symbol,
        "lookback": lookback,
        "range_thr": range_pct,
        "breakout_thr": breakout_pct,
        "sharpe": sharpe_ratio(equity),
        "maxdd": max_drawdown(equity),
        "cagr": cagr(equity),
        "trades": len(trades),
    }


def main():
    parser = argparse.ArgumentParser(description="Grid search vol_breakout")
    parser.add_argument("--start", required=True)
    parser.add_argument("--end", required=True)
    parser.add_argument("--symbols", nargs="+", required=True)
    args = parser.parse_args()

    lookbacks = [15, 30, 45, 60]
    range_thrs = [0.10, 0.15, 0.20, 0.25]
    breakout_thrs = [0.05, 0.10, 0.15]

    combos = list(
        itertools.product(args.symbols, lookbacks, range_thrs, breakout_thrs)
    )
    combos = [(s, l, r, b, args.start, args.end) for s, l, r, b in combos]

    workers = max(1, cpu_count() - 1)
    with Pool(workers) as pool:
        results = list(pool.imap_unordered(run_combo, combos))

    df = pd.DataFrame(results)
    df.to_csv("grid_results.csv", index=False)
    print(f"Saved grid_results.csv with {len(df)} rows")


if __name__ == "__main__":
    main()
