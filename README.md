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
