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

def random_datetime_between(start_dt, end_dt):
    """
    Returns a random timezone-aware datetime between two datetimes.
    Unlike random_datetime() which takes date objects, this takes datetimes.
    """
    delta = end_dt - start_dt
    random_seconds = random.randint(0, int(delta.total_seconds()))
    return start_dt + timedelta(seconds=random_seconds)


# New — pandas DataFrame
def resolve_customer_at_time(customers_df, target_dt):
    matches = customers_df[
        (customers_df["effective_start"] <= target_dt) &
        (customers_df["effective_end"].isna() | 
         (customers_df["effective_end"] > target_dt))
    ]
    if matches.empty:
        return None, None
    row = matches.sample(1).iloc[0]
    return row["customer_sk"], row["country"]


def resolve_product_at_time(products_df, target_dt):
    matches = products_df[
        (products_df["effective_start"] <= target_dt) &
        (products_df["effective_end"].isna() | 
         (products_df["effective_end"] > target_dt))
    ]
    if matches.empty:
        return None, None
    row = matches.sample(1).iloc[0]
    return row["product_sk"], row["category_name"]