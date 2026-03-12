# etl/extract/read_silver.py
# ============================================================
# Centralized Silver layer reader for all ETL pipeline scripts.
# Every Gold transform calls this — never reads
# parquet files directly.
#
# Responsibilities:
#   - Resolves file paths from a single root constant
#   - Logs every read with row counts for auditability
# ============================================================

from pathlib import Path
from typing import Optional, Union

import pandas as pd

from etl.utils.logger import get_logger
from etl.utils.auditor import _get_connection

logger = get_logger(__name__)

# ------------------------------------------------------------
# Silver layer root — single source of truth for all paths.
# Change this one constant if the data lake location ever moves.
# ------------------------------------------------------------
SILVER_ROOT = Path(__file__).resolve().parents[2] / "data_lake" / "processed"


def get_last_silver_date() -> str:
    sql = """
        SELECT CAST(MAX(started_at)::date AS VARCHAR)
        FROM audit.pipeline_runs
        WHERE layer='silver' AND status = 'success'
    """
    conn = _get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
            res = cur.fetchone()[0]
        if res is None:
            raise RuntimeError("No successful silver run found in audit table.")
        return res
    finally:
        conn.close()

def read_silver(
    filename: str,
    event_type: Optional[Union[str, list[str]]] = None,
    execution_date: Optional[str] = None,
) -> pd.DataFrame:
    
    if execution_date is None:
        execution_date = get_last_silver_date()

    filepath = SILVER_ROOT / execution_date / f"{filename}.parquet"

    # ----------------------------------------------------------
    # Guard: file must exist before we attempt to read it
    # ----------------------------------------------------------
    if not filepath.exists():
        logger.error(f"Silver file not found: {filepath}")
        raise FileNotFoundError(f"Silver file not found: {filepath}")

    logger.info(f"Reading Silver file: {filepath}")
    df = pd.read_parquet(filepath)
    rows_before_filter = len(df)

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
                f"not found in {filename}.parquet — returning unfiltered."
            )
        else:
            df = df[df["event_type"].isin(event_type)].reset_index(drop=True)

    rows_after_filter = len(df)

    # ----------------------------------------------------------
    # Log the result — every read is observable
    # ----------------------------------------------------------
    logger.info(
        f"Silver read complete | file={filename}.parquet | "
        f"rows_before_filter={rows_before_filter} | "
        f"rows_returned={rows_after_filter}"
        + (f" | event_type={event_type}" if event_type else "")
    )

    return df, execution_date