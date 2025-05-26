import os
import time
import argparse
import logging
import requests
import pymysql
from tqdm import tqdm

BASE_URL = "https://api.bybit.com"

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

def db_conn():
    return pymysql.connect(
        host=os.getenv("DB_HOST", "localhost"),
        user=os.getenv("DB_USER", "root"),
        password=os.getenv("DB_PASSWORD", ""),
        database=os.getenv("DB_NAME", "bybit"),
        port=int(os.getenv("DB_PORT", 3306)),
        autocommit=False
    )

def create_tables(conn):
    """Create or update required MySQL tables."""
    with conn.cursor() as cur:
        cur.execute(
            """CREATE TABLE IF NOT EXISTS mark1 (
                symbol VARCHAR(20) NOT NULL,
                startTime BIGINT NOT NULL,
                open DOUBLE,
                high DOUBLE,
                low DOUBLE,
                close DOUBLE,
                PRIMARY KEY(symbol, startTime)
            )"""
        )
        cur.execute(
            """CREATE TABLE IF NOT EXISTS index1 (
                symbol VARCHAR(20) NOT NULL,
                startTime BIGINT NOT NULL,
                open DOUBLE,
                high DOUBLE,
                low DOUBLE,
                close DOUBLE,
                PRIMARY KEY(symbol, startTime)
            )"""
        )
        cur.execute(
            """CREATE TABLE IF NOT EXISTS funding8h (
                symbol VARCHAR(20) NOT NULL,
                startTime BIGINT NOT NULL,
                fundingRate DOUBLE,
                fundingRateTimestamp BIGINT,
                PRIMARY KEY(symbol, startTime)
            )"""
        )
        cur.execute(
            """CREATE TABLE IF NOT EXISTS anomalies (
                symbol VARCHAR(20) NOT NULL,
                startTime BIGINT NOT NULL,
                field VARCHAR(64) NOT NULL,
                value DOUBLE,
                PRIMARY KEY(symbol, startTime, field)
            )"""
        )
        cur.execute(
            """CREATE TABLE IF NOT EXISTS symbols (
                symbol VARCHAR(20) PRIMARY KEY
            )"""
        )
        # ensure new column exists
        cur.execute("SHOW COLUMNS FROM funding8h LIKE 'fundingRateTimestamp'")
        if not cur.fetchone():
            cur.execute(
                "ALTER TABLE funding8h ADD COLUMN fundingRateTimestamp BIGINT"
            )
    conn.commit()

def fetch_with_paging(endpoint, params, list_key="list"):
    end_ts = int(time.time() * 1000)
    while True:
        params["end"] = end_ts
        try:
            resp = requests.get(BASE_URL + endpoint, params=params, timeout=10)
            resp.raise_for_status()
        except Exception as exc:
            logging.error("Request failed: %s", exc)
            break
        data = resp.json().get("result", {}).get(list_key) or []
        if not data:
            break
        for row in data:
            yield row
        last = data[-1]
        if isinstance(last, dict):
            ts = int(last.get("fundingRateTimestamp") or last.get("startTime") or last.get("timestamp"))
        else:
            ts = int(last[0])
        end_ts = ts - 1
        time.sleep(0.05)

def insert_mark(conn, symbol, rows, table):
    if not rows:
        return
    with conn.cursor() as cur:
        sql = f"""
        INSERT INTO {table} (symbol, startTime, open, high, low, close)
        VALUES (%s,%s,%s,%s,%s,%s)
        ON DUPLICATE KEY UPDATE open=VALUES(open), high=VALUES(high), low=VALUES(low), close=VALUES(close)
        """
        values = [(symbol, int(r[0]), r[1], r[2], r[3], r[4]) for r in rows]
        cur.executemany(sql, values)
    conn.commit()

def insert_funding(conn, symbol, rows):
    """Insert funding rate data returned as list of dicts."""
    if not rows:
        return
    with conn.cursor() as cur:
        sql = """
        INSERT INTO funding8h (symbol, startTime, fundingRate)
        VALUES (%s,%s,%s)
        ON DUPLICATE KEY UPDATE fundingRate=VALUES(fundingRate)
        """
        anomaly_sql = """
        INSERT INTO anomalies (symbol, startTime, field, value)
        VALUES (%s,%s,'fundingRate',%s)
        ON DUPLICATE KEY UPDATE value=VALUES(value)
        """
        values = []
        anomalies = []
        for r in rows:
            try:
                ts = int(r["fundingRateTimestamp"])
                rate = float(r["fundingRate"])
            except KeyError as exc:
                logging.warning("Missing key in funding row %s: %s", r, exc)
                continue
            except (TypeError, ValueError) as exc:
                logging.warning("Bad funding value in row %s: %s", r, exc)
                continue
            values.append((symbol, ts, rate))
            if abs(rate) > 0.05:
                anomalies.append((symbol, ts, rate))
        if values:
            cur.executemany(sql, values)
        if anomalies:
            cur.executemany(anomaly_sql, anomalies)
    conn.commit()

def check_gaps(conn, table, symbol):
    with conn.cursor() as cur:
        cur.execute(f"SELECT startTime FROM {table} WHERE symbol=%s ORDER BY startTime", (symbol,))
        times = [r[0] for r in cur.fetchall()]
    for prev, curr in zip(times, times[1:]):
        if curr - prev > 60_000:
            logging.warning("Gap >1m in %s for %s: %s -> %s", table, symbol, prev, curr)
            break

def ingest_symbol(conn, symbol):
    logging.info("Ingesting %s", symbol)
    mark_rows = []
    for row in tqdm(fetch_with_paging("/v5/market/mark-price-kline", {
            "category": "linear",
            "symbol": symbol,
            "interval": 1,
            "limit": 1000
        }), desc=f"mark1 {symbol}"):
        mark_rows.append(row)
        if len(mark_rows) >= 1000:
            insert_mark(conn, symbol, mark_rows, "mark1")
            mark_rows = []
    if mark_rows:
        insert_mark(conn, symbol, mark_rows, "mark1")
    check_gaps(conn, "mark1", symbol)

    index_rows = []
    for row in tqdm(fetch_with_paging("/v5/market/index-price-kline", {
            "category": "linear",
            "symbol": symbol,
            "interval": 1,
            "limit": 1000
        }), desc=f"index1 {symbol}"):
        index_rows.append(row)
        if len(index_rows) >= 1000:
            insert_mark(conn, symbol, index_rows, "index1")
            index_rows = []
    if index_rows:
        insert_mark(conn, symbol, index_rows, "index1")
    check_gaps(conn, "index1", symbol)

    funding_rows = []
    for row in tqdm(fetch_with_paging("/v5/market/funding/history", {
            "category": "linear",
            "symbol": symbol,
            "limit": 200
        }), desc=f"funding8h {symbol}"):
        funding_rows.append(row)
        if len(funding_rows) >= 200:
            insert_funding(conn, symbol, funding_rows)
            funding_rows = []
    if funding_rows:
        insert_funding(conn, symbol, funding_rows)
    check_gaps(conn, "funding8h", symbol)

def main():
    parser = argparse.ArgumentParser(description="Backfill Bybit data")
    parser.add_argument("--symbols", nargs="*", help="Symbols to ingest")
    args = parser.parse_args()
    conn = db_conn()
    # ensure tables exist and upgrade schema if necessary
    with conn.cursor() as cur:
        cur.execute("SHOW TABLES LIKE 'funding8h'")
        if not cur.fetchone():
            create_tables(conn)
        else:
            cur.execute("SHOW COLUMNS FROM funding8h LIKE 'fundingRateTimestamp'")
            if not cur.fetchone():
                create_tables(conn)
    if args.symbols:
        symbols = args.symbols
    else:
        with conn.cursor() as cur:
            cur.execute("SELECT symbol FROM symbols")
            symbols = [r[0] for r in cur.fetchall()]
    for sym in symbols:
        ingest_symbol(conn, sym)
    conn.close()

if __name__ == "__main__":
    main()
