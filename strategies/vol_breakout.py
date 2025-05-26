import pandas as pd
from backtests.core import Strategy

class VolBreakout(Strategy):
    def __init__(self, lookback=30, range_threshold=0.0015, breakout_threshold=0.0010):
        self.lookback = lookback
        self.range_threshold = range_threshold
        self.breakout_threshold = breakout_threshold

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
