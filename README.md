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

This repo includes a simple backtest runner. Price data is fetched from the
MySQL table `mark1`. Connection settings are read from the environment variables
`DB_HOST`, `DB_USER`, `DB_PASSWORD` and `DB_NAME` or can be supplied via CLI
flags.
Run a strategy with:

```sh
python -m backtests.run_backtest --symbol BTCUSDT \
       --strategy vol_breakout --start 2024-11-20 --end 2024-11-27 \
       --db-user <user> --db-pass <pass>
```

