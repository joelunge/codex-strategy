import pandas as pd
import numpy as np
from dataclasses import dataclass

@dataclass
class Trade:
    entry_time: pd.Timestamp
    exit_time: pd.Timestamp
    position: int
    entry_price: float
    exit_price: float
    pnl: float

class Strategy:
    """Base strategy class."""
    maker_spread_threshold = 0.0002  # 0.02%
    taker_fee_bp = 0.05  # bp per side
    slippage_bp = 0.5  # bp per side

    def generate_signals(self, df: pd.DataFrame) -> pd.Series:
        raise NotImplementedError

    def simulate(self, df: pd.DataFrame) -> tuple:
        signals = self.generate_signals(df)
        df = df.copy()
        df['signal'] = signals

        position = 0
        entry_price = 0.0
        entry_idx = None
        stop_price = None
        take_price = None
        trades = []
        equity = []
        cash = 1.0

        for i in range(len(df)):
            row = df.iloc[i]
            signal = row['signal']
            price = row['open']
            spread = row.get('spread', 0)
            fee = 0.0
            if spread < self.maker_spread_threshold:
                fee = self.taker_fee_bp / 10000
            slip = self.slippage_bp / 10000

            if position == 0:
                if signal != 0:
                    position = signal * getattr(self, "risk_mult", 1.0)
                    trade_price = price * (1 + slip * position)
                    entry_price = trade_price
                    entry_idx = i
                    rng = row.get('range', 0)
                    stop_price = entry_price - position * 0.5 * rng
                    take_price = entry_price + position * 1.0 * rng
            else:
                exit_flag = False
                exit_at = price
                # stop or take profit
                if position == 1:
                    if row['low'] <= stop_price:
                        exit_at = stop_price
                        exit_flag = True
                    elif row['high'] >= take_price:
                        exit_at = take_price
                        exit_flag = True
                else:
                    if row['high'] >= stop_price:
                        exit_at = stop_price
                        exit_flag = True
                    elif row['low'] <= take_price:
                        exit_at = take_price
                        exit_flag = True
                # max hold 120 bars
                if entry_idx is not None and i - entry_idx >= 120:
                    exit_flag = True
                if signal == -position:
                    exit_flag = True

                if exit_flag:
                    exit_price = exit_at * (1 - slip * position)
                    pnl = position * (exit_price - entry_price) - fee * entry_price - fee * exit_price
                    cash *= (1 + pnl / entry_price)
                    trades.append(Trade(
                        entry_time=df.index[entry_idx],
                        exit_time=df.index[i],
                        position=position,
                        entry_price=entry_price,
                        exit_price=exit_price,
                        pnl=pnl,
                    ))
                    position = 0
                    entry_idx = None
            # mark to market
            if position != 0:
                mtm = position * (row['close'] - entry_price)
                eq = cash * (1 + mtm / entry_price)
            else:
                eq = cash
            equity.append(eq)

        equity_series = pd.Series(equity, index=df.index)
        trades_df = pd.DataFrame([t.__dict__ for t in trades])
        return trades_df, equity_series

def cagr(equity: pd.Series) -> float:
    if equity.empty:
        return 0.0
    days = (equity.index[-1] - equity.index[0]).days / 365.25
    if days <= 0:
        return 0.0
    return (equity.iloc[-1] / equity.iloc[0]) ** (1/days) - 1

def max_drawdown(equity: pd.Series) -> float:
    cummax = equity.cummax()
    dd = equity / cummax - 1
    return dd.min()

def sharpe_ratio(equity: pd.Series) -> float:
    rets = equity.pct_change().dropna()
    if rets.std() == 0:
        return 0.0
    return np.sqrt(525600) * rets.mean() / rets.std()

def win_rate(trades: pd.DataFrame) -> float:
    if trades.empty:
        return 0.0
    wins = (trades['pnl'] > 0).sum()
    return wins / len(trades)

def payoff_ratio(trades: pd.DataFrame) -> float:
    if trades.empty:
        return 0.0
    gains = trades.loc[trades['pnl'] > 0, 'pnl']
    losses = trades.loc[trades['pnl'] < 0, 'pnl'].abs()
    if losses.sum() == 0:
        return float('inf')
    return gains.mean() / losses.mean()


def kelly_fraction(trades: pd.DataFrame) -> float:
    """Return the Kelly fraction based on trade history."""
    w = win_rate(trades)
    pr = payoff_ratio(trades)
    if pr <= 0:
        return 0.0
    return max(w - (1 - w) / pr, 0.0)


MIN_WEIGHT = 0.05


class PortfolioSimulator:
    """Combine multiple strategies into a portfolio."""

    def __init__(self, strategies, risk_scale: float = 0.5):
        self.strategies = strategies  # list of (name, symbol, instance, df)
        self.risk_scale = risk_scale

    def run(self):
        results = []
        equities = []
        trades_all = []
        for name, symbol, strat, df in self.strategies:
            trades, equity = strat.simulate(df)
            kelly = kelly_fraction(trades)
            weight = kelly * self.risk_scale
            if weight == 0:
                weight = MIN_WEIGHT
            equities.append((equity, weight))
            trades['strategy'] = name
            trades['symbol'] = symbol
            trades_all.append(trades)
            results.append({
                'strategy': name,
                'symbol': symbol,
                'sharpe': sharpe_ratio(equity),
                'maxdd': max_drawdown(equity),
                'cagr': cagr(equity),
                'trades': len(trades),
                'kelly': kelly,
                'weight': weight,
            })

        all_index = sorted(set().union(*(eq.index for eq, _ in equities)))
        portfolio = pd.Series(1.0, index=pd.Index(all_index))
        for eq, w in equities:
            aligned = eq.reindex(all_index).ffill().fillna(1.0)
            portfolio += (aligned - 1.0) * w

        trades_df = pd.concat(trades_all, ignore_index=True) if trades_all else pd.DataFrame()
        return pd.DataFrame(results), portfolio, trades_df


def run_portfolio(strategies, risk_scale: float = 0.5):
    sim = PortfolioSimulator(strategies, risk_scale=risk_scale)
    return sim.run()
