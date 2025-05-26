import pandas as pd
from strategies.vol_breakout import VolBreakout, DEFAULT_RISK_MULT

def test_default_risk_mult():
    assert DEFAULT_RISK_MULT == 1.0


def test_generate_signals_produces_signal():
    # create sample data around Feb 1 2024
    index = pd.date_range('2024-02-01', periods=20, freq='1min')
    data = {
        'open':  [90]*15 + [110, 111, 112, 113, 114],
        'high':  [100]*15 + [120, 121, 122, 123, 124],
        'low':   [90]*15 + [109, 110, 111, 112, 113],
        'close': [95]*15 + [110, 112, 113, 114, 115],
    }
    df = pd.DataFrame(data, index=index)
    strat = VolBreakout()
    signals = strat.generate_signals(df)
    assert (signals != 0).any()
