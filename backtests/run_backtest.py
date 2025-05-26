import argparse
from importlib import import_module
import warnings
import pandas as pd
from backtests.core import cagr, max_drawdown, sharpe_ratio, win_rate, payoff_ratio
from utils.db import db_conn

warnings.filterwarnings(
    "ignore",
    message=".*read_sql.*",
    category=FutureWarning,
)


def load_data(conn, symbol: str, start: str, end: str, with_index: bool = False) -> pd.DataFrame:
    """Load OHLC data from MySQL mark1 table.

    When ``with_index`` is True the index close is joined as ``index_close``.
    """
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
    df = df[["open", "high", "low", "close"]].astype(float)
    if with_index:
        query2 = (
            "SELECT startTime AS ts, close FROM index1 "
            "WHERE symbol=%s AND startTime BETWEEN %s AND %s ORDER BY startTime"
        )
        idx = pd.read_sql(query2, conn, params=(symbol, start_ts, end_ts))
        idx["ts"] = pd.to_datetime(idx.ts, unit="ms")
        idx.set_index("ts", inplace=True)
        df = df.join(idx[["close"]].rename(columns={"close": "index_close"}), how="left")
    return df


def main():
    parser = argparse.ArgumentParser(description="Run backtest")
    parser.add_argument('--symbol', required=True)
    parser.add_argument('--strategy', required=True)
    parser.add_argument('--start', required=True)
    parser.add_argument('--end', required=True)
    parser.add_argument('--risk-mult', type=float, default=1.0,
                        help='Risk multiplier for position size')
    parser.add_argument('--range-thr', type=float, default=0.001,
                        help='Range threshold as decimal percentage')
    parser.add_argument('--breakout-thr', type=float, default=0.0005,
                        help='Breakout threshold as decimal percentage')
    args = parser.parse_args()

    conn = db_conn()
    df = load_data(conn, args.symbol, args.start, args.end, with_index=args.strategy == 'funding_carry')
    conn.close()

    module = import_module(f"strategies.{args.strategy}")
    cls = getattr(module, ''.join([p.capitalize() for p in args.strategy.split('_')]))
    if args.strategy == 'vol_breakout':
        strat = cls(
            range_threshold=args.range_thr,
            breakout_threshold=args.breakout_thr,
            risk_mult=args.risk_mult,
        )
    elif 'risk_mult' in cls.__init__.__code__.co_varnames:
        strat = cls(risk_mult=args.risk_mult)
    else:
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
