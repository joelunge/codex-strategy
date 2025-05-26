import os
import pymysql

def db_conn():
    return pymysql.connect(
        host=os.getenv("DB_HOST", "localhost"),
        user=os.getenv("DB_USER", "root"),
        password=os.getenv("DB_PASSWORD", "root"),
        database=os.getenv("DB_NAME", "sct_2024"),
        port=int(os.getenv("DB_PORT", 3306)),
        autocommit=False,
    )
