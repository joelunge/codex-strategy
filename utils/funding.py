import pandas as pd


def minutes_to_settlement(ts_ms: int) -> int:
    """Return minutes until the next 8h funding settlement."""
    ts = pd.Timestamp(ts_ms, unit="ms", tz="UTC")
    next_settlement = ts.floor("8H") + pd.Timedelta(hours=8)
    return int((next_settlement - ts).total_seconds() // 60)


def predicted_funding(mark_df: pd.DataFrame, index_df: pd.DataFrame) -> pd.Series:
    """Calculate predicted funding rate from mark and index price series."""
    premium = (mark_df["close"] - index_df["close"]) / index_df["close"]
    return premium.clip(-0.0075, 0.0075)
