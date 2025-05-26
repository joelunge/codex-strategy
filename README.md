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

If no symbols are provided the script will read them from the `symbols` table in the database.

## Backtesting

This repo includes a simple backtest runner. Price data is expected as CSV files in `data/` with the columns `timestamp,open,high,low,close,spread`.
Run a strategy with:

```sh
python backtests/run_backtest.py --symbol BTCUSDT \
       --strategy vol_breakout --start 2023-01-01 --end 2023-06-01
```

