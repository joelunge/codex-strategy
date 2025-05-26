import argparse
import asyncio
import csv
import os
from datetime import datetime
from pathlib import Path

import pandas as pd

from strategies.vol_breakout import VolBreakout
from strategies.funding_carry import FundingCarry
from utils.bybit import (
    get_mark_price,
    get_index_price,
    place_order_post_only,
    fetch_funding,
)


class LiveBot:
    def __init__(self, net: str, symbols: list[str], risk_mult: float):
        self.net = net
        self.symbols = symbols
        self.risk_mult = risk_mult
        key_var = "BYBIT_KEY_TEST" if net == "testnet" else "BYBIT_KEY"
        sec_var = "BYBIT_SECRET_TEST" if net == "testnet" else "BYBIT_SECRET"
        self.api_key = os.getenv(key_var)
        self.api_secret = os.getenv(sec_var)
        if not self.api_key or not self.api_secret:
            raise SystemExit(f"Missing API keys in {key_var}/{sec_var}")
        self.running = True
        self.price_hist: dict[str, pd.DataFrame] = {
            sym: pd.DataFrame(columns=["close", "index_close"]) for sym in symbols
        }
        logdir = Path("logs")
        logdir.mkdir(exist_ok=True)
        fname = logdir / f"live_{datetime.utcnow():%Y%m%d}.csv"
        self.log = open(fname, "a", newline="")
        self.writer = csv.writer(self.log)
        if self.log.tell() == 0:
            self.writer.writerow(
                [
                    "timestamp",
                    "symbol",
                    "strategy",
                    "side",
                    "qty",
                    "price",
                    "fee",
                    "funding",
                    "realised_pnl",
                ]
            )

    async def run(self):
        print(f"Connected to {self.net}")
        while self.running:
            await self.loop_once()
            await asyncio.sleep(60)

    async def loop_once(self):
        ts = datetime.utcnow().replace(tzinfo=pd.Timestamp.utcnow().tzinfo)
        for sym in self.symbols:
            mark_price, index_price = await asyncio.gather(
                asyncio.to_thread(get_mark_price, sym, self.net),
                asyncio.to_thread(get_index_price, sym, self.net),
            )
            df = self.price_hist[sym]
            df.loc[ts, "close"] = mark_price
            df.loc[ts, "index_close"] = index_price
            df = df.tail(60)
            self.price_hist[sym] = df
            vb = VolBreakout(risk_mult=self.risk_mult)
            fc = FundingCarry()
            fc.risk_mult = self.risk_mult
            vb_data = df[["close"]].copy()
            fc_data = df[["close", "index_close"]].copy()
            for strat, name, data in [
                (vb, "vol_breakout", vb_data),
                (fc, "funding_carry", fc_data),
            ]:
                signals = strat.generate_signals(data)
                if signals.empty:
                    continue
                signal = signals.iloc[-1]
                if signal == 0:
                    continue
                side = "Buy" if signal > 0 else "Sell"
                qty = getattr(strat, "risk_mult", 1.0)
                place_order_post_only(
                    sym,
                    side,
                    qty,
                    mark_price,
                    self.api_key,
                    self.api_secret,
                    self.net,
                )
                funding = fetch_funding(sym, self.net)
                self.writer.writerow(
                    [
                        ts.isoformat(),
                        sym,
                        name,
                        side,
                        qty,
                        mark_price,
                        0,
                        funding,
                        0,
                    ]
                )
                self.log.flush()

    def stop(self):
        self.running = False
        self.log.close()


def parse_args():
    parser = argparse.ArgumentParser(description="Run live trading bot")
    parser.add_argument("--net", choices=["testnet", "mainnet"], default="testnet")
    parser.add_argument(
        "--symbols", default="BTCUSDT,ETHUSDT", help="Comma separated symbols"
    )
    parser.add_argument("--risk-mult", type=float, default=1.0)
    return parser.parse_args()


async def main():
    args = parse_args()
    symbols = [s.strip() for s in args.symbols.split(",") if s.strip()]
    bot = LiveBot(args.net, symbols, args.risk_mult)
    try:
        await bot.run()
    except KeyboardInterrupt:
        pass
    finally:
        bot.stop()


if __name__ == "__main__":
    asyncio.run(main())
