import argparse
import pandas as pd
from importlib import import_module
from backtests.core import cagr, max_drawdown, sharpe_ratio, win_rate, payoff_ratio


def load_data(symbol: str, start: str, end: str) -> pd.DataFrame:
    path = f"data/{symbol}.csv"
    df = pd.read_csv(path, parse_dates=['timestamp'])
    df.set_index('timestamp', inplace=True)
    return df.loc[start:end]


def main():
    parser = argparse.ArgumentParser(description="Run backtest")
    parser.add_argument('--symbol', required=True)
    parser.add_argument('--strategy', required=True)
    parser.add_argument('--start', required=True)
    parser.add_argument('--end', required=True)
    args = parser.parse_args()

    df = load_data(args.symbol, args.start, args.end)

    module = import_module(f"strategies.{args.strategy}")
    cls = getattr(module, ''.join([p.capitalize() for p in args.strategy.split('_')]))
    strat = cls()

    trades, equity = strat.simulate(df)

    print(f"Trades: {len(trades)}")
    print(f"CAGR: {cagr(equity):.2%}")
    print(f"MaxDD: {max_drawdown(equity):.2%}")
    print(f"Sharpe: {sharpe_ratio(equity):.2f}")
    print(f"Win rate: {win_rate(trades):.2%}")
    print(f"Payoff ratio: {payoff_ratio(trades):.2f}")


if __name__ == '__main__':
    main()
