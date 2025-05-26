import pandas as pd
from backtests.core import Strategy
from utils.funding import minutes_to_settlement, predicted_funding


class FundingCarry(Strategy):
    """Carry strategy based on predicted funding."""

    def generate_signals(self, df: pd.DataFrame) -> pd.Series:
        mark = df[["close"]].copy()
        idx = df[["index_close"]].rename(columns={"index_close": "close"})
        pred = predicted_funding(mark, idx)
        minutes = df.index.map(lambda ts: minutes_to_settlement(int(ts.timestamp() * 1000)))
        signal = pd.Series(0, index=df.index)
        cond_short = (pred > 0.003) & (minutes >= 5)
        cond_long = (pred < -0.003) & (minutes >= 5)
        signal[cond_short] = -1
        signal[cond_long] = 1
        signal[minutes <= 3] = 0
        return signal
