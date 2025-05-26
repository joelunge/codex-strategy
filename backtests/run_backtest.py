import argparse
import os
import pandas as pd
from importlib import import_module
from backtests.core import cagr, max_drawdown, sharpe_ratio, win_rate, payoff_ratio
from run_ingest import db_conn


def load_data(conn, symbol: str, start: str, end: str) -> pd.DataFrame:
    """Load OHLC data from MySQL mark1 table."""
    query = (
        "SELECT startTime AS ts, open, high, low, close "
        "FROM mark1 "
        "WHERE symbol=%s AND startTime BETWEEN %s AND %s "
        "ORDER BY startTime"
    )
    start_ts = int(pd.Timestamp(start).timestamp() * 1000)
    end_ts = int(pd.Timestamp(end).timestamp() * 1000)
    df = pd.read_sql(query, conn, params=(symbol, start_ts, end_ts))
    df["ts"] = pd.to_datetime(df.ts, unit="ms")
    df.set_index("ts", inplace=True)
    return df[["open", "high", "low", "close"]].astype(float)


def main():
    parser = argparse.ArgumentParser(description="Run backtest")
    parser.add_argument('--symbol', required=True)
    parser.add_argument('--strategy', required=True)
    parser.add_argument('--start', required=True)
    parser.add_argument('--end', required=True)
    parser.add_argument('--db-user')
    parser.add_argument('--db-pass')
    parser.add_argument('--db-host')
    parser.add_argument('--db-name')
    args = parser.parse_args()

    if args.db_user:
        os.environ['DB_USER'] = args.db_user
    if args.db_pass:
        os.environ['DB_PASSWORD'] = args.db_pass
    if args.db_host:
        os.environ['DB_HOST'] = args.db_host
    if args.db_name:
        os.environ['DB_NAME'] = args.db_name

    conn = db_conn()
    df = load_data(conn, args.symbol, args.start, args.end)
    conn.close()

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
