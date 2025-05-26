import argparse
from importlib import import_module

import pandas as pd

from backtests.run_backtest import load_data
from backtests.core import run_portfolio, cagr, max_drawdown, sharpe_ratio
from utils.db import db_conn


def main():
    parser = argparse.ArgumentParser(description="Run portfolio backtest")
    parser.add_argument("--symbols", nargs="+", required=True)
    parser.add_argument("--strategies", nargs="+", required=True)
    parser.add_argument("--start", required=True)
    parser.add_argument("--end", required=True)
    args = parser.parse_args()

    conn = db_conn()
    data = {sym: load_data(conn, sym, args.start, args.end, with_index=True) for sym in args.symbols}
    conn.close()

    strategy_items = []
    for strat_name in args.strategies:
        module = import_module(f"strategies.{strat_name}")
        cls = getattr(module, "".join([p.capitalize() for p in strat_name.split("_")]))
        for sym in args.symbols:
            strat = cls()
            strategy_items.append((strat_name, sym, strat, data[sym]))

    summary, portfolio_eq, _ = run_portfolio(strategy_items)

    for strat in summary["strategy"].unique():
        sub = summary[summary["strategy"] == strat]
        print(f"Strategy {strat}:")
        print(sub.to_string(index=False))
        print()

    print("Portfolio Metrics:")
    print(f"CAGR: {cagr(portfolio_eq):.2%}")
    print(f"MaxDD: {max_drawdown(portfolio_eq):.2%}")
    print(f"Sharpe: {sharpe_ratio(portfolio_eq):.2f}")


if __name__ == "__main__":
    main()

