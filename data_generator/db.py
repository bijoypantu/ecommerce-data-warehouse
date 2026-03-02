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
import pandas as pd
from collections import defaultdict


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


def build_product_versions(products_df):
    """
    Builds product version lookup from raw events.
    Derives effective_end from next event's effective_start.
    Returns dict: product_id -> list of version dicts
    """    
    versions = defaultdict(list)
    
    # Sort by product_id and effective_start
    sorted_df = products_df.sort_values(["product_id", "effective_start"])
    
    for product_id, group in sorted_df.groupby("product_id"):
        group_rows = group.to_dict("records")
        for idx, row in enumerate(group_rows):
            # effective_end = next row's effective_start, or None if last
            if idx < len(group_rows) - 1:
                row["effective_end"] = group_rows[idx + 1]["effective_start"]
                row["is_current"]    = False
            else:
                row["effective_end"] = None
                row["is_current"]    = True
            versions[product_id].append(row)
    
    return versions


def build_customer_versions(customers_df):
    versions = defaultdict(list)
    sorted_df = customers_df.sort_values(["customer_id", "effective_start"])
    
    for customer_id, group in sorted_df.groupby("customer_id"):
        group_rows = group.to_dict("records")
        for idx, row in enumerate(group_rows):
            if idx < len(group_rows) - 1:
                row["effective_end"] = group_rows[idx + 1]["effective_start"]
                row["is_current"]    = False
            else:
                row["effective_end"] = None
                row["is_current"]    = True
            versions[customer_id].append(row)
    
    return versions


def resolve_customer_at_time(customer_versions, target_dt):
    for _ in range(10):
        customer_id = random.choice(list(customer_versions.keys()))
        versions    = customer_versions[customer_id]
        matches     = [
            row for row in versions
            if row["effective_start"] <= target_dt
            and (pd.isna(row["effective_end"]) or row["effective_end"] > target_dt)
        ]
        if matches:
            row = random.choice(matches)
            return row["customer_id"], row["country"]
    return None, None

def resolve_product_at_time(product_versions, target_dt):
    # Try up to 10 times to find a matching product
    for _ in range(10):
        product_id = random.choice(list(product_versions.keys()))
        versions   = product_versions[product_id]
        matches    = [
            row for row in versions
            if row["effective_start"] <= target_dt
            and (pd.isna(row["effective_end"]) or row["effective_end"] > target_dt)
        ]
        if matches:
            row = random.choice(matches)
            return row["product_id"], row["category_name"]
    return None, None

def write_jsonl(df, filepath):
    """
    Writes a pandas DataFrame to a JSONL file.
    Each row becomes one JSON line with proper datetime serialization.
    """
    import json
    from pathlib import Path
    
    Path(filepath).parent.mkdir(parents=True, exist_ok=True)
    
    with open(filepath, 'w') as f:
        for _, row in df.iterrows():
            record = {}
            for col, val in row.items():
                if hasattr(val, 'isoformat'):
                    record[col] = val.isoformat()
                elif val is None or (isinstance(val, float) and pd.isna(val)):
                    record[col] = None
                else:
                    record[col] = val
            f.write(json.dumps(record) + '\n')
    
    print(f"  Written: {filepath}")