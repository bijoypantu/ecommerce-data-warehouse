# etl/extract/generator/db.py
# ============================================================
# Database connection and shared helper functions.
# Every generator script imports from here.
# ============================================================

import os
import psycopg2
from psycopg2.extras import execute_values
from datetime import date, timedelta, datetime, timezone
from dotenv import load_dotenv
import random

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


def random_date(start: date, end: date) -> date:
    delta = (end - start).days
    return start + timedelta(days=random.randint(0, delta))


def random_datetime(start: date, end: date):
    """
    Returns a timezone-aware datetime (UTC) between two dates,
    with a random time component.
    """
    random_day = random_date(start, end)
    random_time = datetime(
        random_day.year,
        random_day.month,
        random_day.day,
        random.randint(0, 23),   # hour
        random.randint(0, 59),   # minute
        random.randint(0, 59),   # second
        tzinfo=timezone.utc
    )
    return random_time


def execute_many(conn, sql, rows, page_size=1000):
    """
    Executes the same SQL statement for multiple rows.
    Used for UPDATE operations where execute_values isn't suitable.

    Args:
        conn      : active psycopg2 connection
        sql       : SQL string with %s placeholders
        rows      : list of tuples, one per row
        page_size : batch size for executemany
    """
    if not rows:
        return

    with conn.cursor() as cur:
        cur.executemany(sql, rows)
    conn.commit()
    print(f"  Updated {len(rows)} rows.")


def resolve_customer_at_time(all_customers, target_dt):
    """
    Picks a random customer version active at target_dt.
    all_customers rows: (customer_sk, customer_id, effective_start, effective_end, country)
    Returns (customer_sk, country) or (None, None)
    """
    matches = [
        row for row in all_customers
        if row[2] <= target_dt
        and (row[3] is None or row[3] > target_dt)
    ]
    if not matches:
        return None, None
    row = random.choice(matches)
    return row[0], row[4]  # customer_sk, country


def resolve_product_at_time(all_products, target_dt):
    """
    Picks a random product version active at target_dt.
    all_products rows: (product_id, product_sk, effective_start, effective_end)
    Returns product_sk or None
    """
    matches = [
        row for row in all_products
        if row[2] <= target_dt
        and (row[3] is None or row[3] > target_dt)
    ]
    if not matches:
        return None
    row = random.choice(matches)
    return row[1]  # product_sk