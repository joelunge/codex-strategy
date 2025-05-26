# codex-strategy

## Data Ingest

This repository contains a simple ingestion script for Bybit market data.

### Usage

Set the MySQL connection environment variables before running:

```sh
export DB_HOST=localhost
export DB_USER=myuser
export DB_PASSWORD=mypass
export DB_NAME=bybit
```

Install dependencies:

```sh
pip install -r requirements.txt
```

Run the backfill for specific symbols:

```sh
python run_ingest.py --symbols BTCUSDT ETHUSDT
```

By default the script only fetches data that is missing from the database. To
force a full backfill, pass `--full`.

Incremental example fetching the latest data for a single symbol:

```sh
python run_ingest.py --symbols ETHUSDT
```

If no symbols are provided the script will read them from the `symbols` table in the database.

## Backtesting

This repo includes a simple backtest runner. Price data is fetched from the
MySQL table `mark1`. Connection settings are read from the environment variables
`DB_HOST`, `DB_USER`, `DB_PASSWORD` and `DB_NAME`.
Run a strategy with:

```sh
python -m backtests.run_backtest \
       --symbol BTCUSDT \
       --strategy vol_breakout \
       --start 2024-11-20 \
       --end   2024-11-27 \
       --range-thr 0.001 \
       --breakout-thr 0.0005 \
       --risk-mult 1.0
```


## Grid search usage

Run a parameter sweep for the `vol_breakout` strategy:

```sh
python run_grid.py --start 2024-02-01 --end 2024-05-01 --symbols BTCUSDT ETHUSDT
```

Results are saved to `grid_results.csv`.


## Funding-Carry Strategy

`funding_carry` trades when the predicted funding rate deviates from spot. The prediction is clamped to ±0.75% and positions are opened when it exceeds ±0.3% with at least five minutes to the next funding event.

## Portfolio Backtest Usage

Multiple strategies can be combined with `run_portfolio.py`:

```sh
python run_portfolio.py \
       --symbols BTCUSDT ETHUSDT \
       --strategies vol_breakout funding_carry \
       --start 2024-02-01 --end 2024-05-01
```

The script prints metrics for each strategy and for the total portfolio.

## Live Trading (Testnet)

Set your testnet API keys and run the live bot:

```sh
export BYBIT_KEY_TEST=xxx
export BYBIT_SECRET_TEST=yyy
python live_bot.py --net testnet --risk-mult 0.5
```

## Go Live (Mainnet)

When ready for real trading use your mainnet keys and typically a lower risk multiplier:

```sh
export BYBIT_KEY=xxx
export BYBIT_SECRET=yyy
python live_bot.py --net mainnet --risk-mult 0.25
```
