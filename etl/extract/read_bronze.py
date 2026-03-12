# etl/extract/read_bronze.py
# ============================================================
# Centralized Bronze layer reader for all ETL pipeline scripts.
# Every Silver and Gold transform calls this — never reads
# JSONL files directly.
#
# Responsibilities:
#   - Resolves file paths from a single root constant
#   - Skips and logs corrupt JSONL lines (never crashes)
#   - Parses timestamps to timezone-aware datetime objects
#   - Optionally filters by one or more event_type values
#   - Logs every read with row counts for auditability
# ============================================================

import json
from pathlib import Path
from typing import Optional, Union

import pandas as pd

from etl.utils.logger import get_logger
from etl.utils.auditor import _get_connection

logger = get_logger(__name__)

# ------------------------------------------------------------
# Bronze layer root — single source of truth for all paths.
# Change this one constant if the data lake location ever moves.
# ------------------------------------------------------------
BRONZE_ROOT = Path(__file__).resolve().parents[2] / "data_lake" / "raw"

# ------------------------------------------------------------
# Timestamp columns per file — parsed to datetime on read.
# Add new files here as the project grows.
# ------------------------------------------------------------
TIMESTAMP_COLUMNS: dict[str, list[str]] = {
    "dim_category":      ["ingested_at"],
    "dim_customer":      ["signup_timestamp", "effective_start", "ingested_at"],
    "dim_product":       ["effective_start", "ingested_at"],
    "fact_orders":       ["order_created_at", "order_last_updated_at", "ingested_at"],
    "fact_order_items":  ["order_created_at", "ingested_at"],
    "fact_payments":     ["payment_timestamp", "ingested_at"],
    "fact_shipments":    ["shipped_at", "delivered_at", "ingested_at"],
    "fact_refunds":      ["initiated_at", "processed_at", "ingested_at"],
}

conn = _get_connection()

def get_last_generation_date() -> str:
    sql = """
        SELECT CAST(MAX(started_at)::date AS VARCHAR)
        FROM audit.pipeline_runs
        WHERE layer='generation' AND status = 'success'
    """
    conn = _get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
            res = cur.fetchone()[0]
        if res is None:
            raise RuntimeError("No successful data_generator run found in audit table.")
        return res
    finally:
        conn.close()

def read_bronze(
    filename: str,
    event_type: Optional[Union[str, list[str]]] = None,
    execution_date: Optional[str] = None,
) -> pd.DataFrame:
    
    if execution_date is None:
        execution_date = get_last_generation_date()

    filepath = BRONZE_ROOT / execution_date / f"{filename}.jsonl"

    # ----------------------------------------------------------
    # Guard: file must exist before we attempt to read it
    # ----------------------------------------------------------
    if not filepath.exists():
        logger.error(f"Bronze file not found: {filepath}")
        raise FileNotFoundError(f"Bronze file not found: {filepath}")

    logger.info(f"Reading Bronze file: {filepath}")

    # ----------------------------------------------------------
    # Read JSONL line by line — skip and log corrupt lines.
    # A single bad row must never crash the entire pipeline.
    # ----------------------------------------------------------
    records       = []
    total_lines   = 0
    corrupt_lines = 0

    with open(filepath, "r", encoding="utf-8") as f:
        for line_num, line in enumerate(f, start=1):
            line = line.strip()
            if not line:
                continue  # skip blank lines silently

            total_lines += 1
            try:
                records.append(json.loads(line))
            except json.JSONDecodeError as e:
                corrupt_lines += 1
                logger.warning(
                    f"Corrupt line skipped | file={filename}.jsonl "
                    f"line={line_num} | error={e}"
                )

    if corrupt_lines > 0:
        logger.warning(
            f"Corrupt lines summary | file={filename}.jsonl | "
            f"corrupt={corrupt_lines}/{total_lines}"
        )

    # ----------------------------------------------------------
    # Build DataFrame — handle empty file gracefully
    # ----------------------------------------------------------
    if not records:
        logger.warning(f"No valid records found in {filename}.jsonl — returning empty DataFrame.")
        return pd.DataFrame()

    df = pd.DataFrame(records)
    rows_before_filter = len(df)

    # ----------------------------------------------------------
    # Parse timestamp columns to timezone-aware datetime.
    # Only parses columns that actually exist in this file.
    # ----------------------------------------------------------
    ts_cols = TIMESTAMP_COLUMNS.get(filename, [])
    for col in ts_cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], utc=True, errors="coerce")

    # ----------------------------------------------------------
    # Filter by event_type if requested.
    # Normalise to a list so the logic is always the same.
    # ----------------------------------------------------------
    if event_type is not None:
        if isinstance(event_type, str):
            event_type = [event_type]

        if "event_type" not in df.columns:
            logger.warning(
                f"event_type filter requested but 'event_type' column "
                f"not found in {filename}.jsonl — returning unfiltered."
            )
        else:
            df = df[df["event_type"].isin(event_type)].reset_index(drop=True)

    rows_after_filter = len(df)

    # ----------------------------------------------------------
    # Log the result — every read is observable
    # ----------------------------------------------------------
    logger.info(
        f"Bronze read complete | file={filename}.jsonl | "
        f"total_lines={total_lines} | corrupt={corrupt_lines} | "
        f"rows_before_filter={rows_before_filter} | "
        f"rows_returned={rows_after_filter}"
        + (f" | event_type={event_type}" if event_type else "")
    )

    return df, execution_date
