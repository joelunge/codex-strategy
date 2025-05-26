# Data Ingest

This folder contains a simple ingestion script for Bybit market data.

## Usage

Set the MySQL connection environment variables before running:

```
export DB_HOST=localhost
export DB_USER=myuser
export DB_PASSWORD=mypass
export DB_NAME=bybit
```

Install dependencies:

```
pip install -r requirements.txt
```

Run the backfill for specific symbols:

```
python data_ingest/run_ingest.py --symbols BTCUSDT ETHUSDT
```

If no symbols are provided the script will read them from the `symbols` table in the database.
