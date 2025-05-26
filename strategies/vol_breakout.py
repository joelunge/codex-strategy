import pandas as pd
from backtests.core import Strategy

DEFAULT_RISK_MULT = 1.0

class VolBreakout(Strategy):
    """Simple volatility breakout strategy."""

    def __init__(self, lookback=15, range_threshold=0.10, breakout_threshold=0.05, risk_mult=DEFAULT_RISK_MULT):
        """Initialize breakout parameters.

        Parameters
        ----------
        lookback : int
            Rolling high/low lookback in minutes.
        range_threshold : float
            Minimum range relative to low price to activate breakout.
        breakout_threshold : float
            Breakout distance from the high/low.
        risk_mult : float
            Multiplier applied to position size.
        """
        self.lookback = lookback
        self.range_threshold = range_threshold
        self.breakout_threshold = breakout_threshold
        self.risk_mult = risk_mult

    def generate_signals(self, df: pd.DataFrame) -> pd.Series:
        high_roll = df['high'].shift(1).rolling(self.lookback).max()
        low_roll = df['low'].shift(1).rolling(self.lookback).min()
        rng = high_roll - low_roll
        df['range'] = rng
        cond = (rng / low_roll) >= self.range_threshold
        long_cond = cond & (df['close'] > high_roll * (1 + self.breakout_threshold))
        short_cond = cond & (df['close'] < low_roll * (1 - self.breakout_threshold))
        signal = pd.Series(0, index=df.index)
        signal[long_cond] = 1
        signal[short_cond] = -1
        df['range'] = rng
        return signal
