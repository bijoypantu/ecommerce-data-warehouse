# etl/extract/generator/db.py
# ============================================================
# Database connection and shared helper functions.
# Every generator script imports from here.
# ============================================================

import os
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

load_dotenv()


def get_connection():
    """
    Returns a psycopg2 connection using credentials from .env
    """
    return psycopg2.connect(
        host=os.getenv("DB_HOST"),
        port=int(os.getenv("DB_PORT", 5432)),
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD")
    )


def bulk_insert(conn, table, columns, rows, page_size=1000):
    """
    Inserts a list of tuples into the given table.

    Args:
        conn      : active psycopg2 connection
        table     : fully qualified table name e.g. 'dw.dim_category'
        columns   : list of column names e.g. ['category_id', 'category_name']
        rows      : list of tuples matching the column order
        page_size : how many rows to insert per batch
    """
    if not rows:
        print(f"  No rows to insert for {table}. Skipping.")
        return

    col_str = ", ".join(columns)
    sql = f"INSERT INTO {table} ({col_str}) VALUES %s"

    with conn.cursor() as cur:
        execute_values(cur, sql, rows, page_size=page_size)
    conn.commit()
    print(f"  Inserted {len(rows)} rows into {table}.")


def fetch_all(conn, query, params=None):
    """
    Executes a SELECT query and returns all rows as a list of tuples.

    Args:
        conn   : active psycopg2 connection
        query  : SQL query string
        params : optional tuple of query parameters
    """
    with conn.cursor() as cur:
        cur.execute(query, params or ())
        return cur.fetchall()


def date_to_sk(d):
    """
    Converts a date object to an integer date_sk in YYYYMMDD format.
    Example: date(2024, 3, 15) → 20240315
    """
    return int(d.strftime("%Y%m%d"))