# etl/utils/auditor.py
# ============================================================
# Audit trail writer for all ETL pipeline scripts.
# Writes run records, data quality results, and rejected rows
# to the audit schema in PostgreSQL.
# ============================================================

import os
import json
from datetime import datetime, timezone
import pandas as pd
from typing import Optional

import psycopg2
from psycopg2.extras import Json
from dotenv import load_dotenv

from etl.utils.logger import get_logger

load_dotenv()
logger = get_logger(__name__)


def _get_connection():
    """
    Opens and returns a psycopg2 connection using .env credentials.
    Caller is responsible for closing it.
    """
    return psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        port=os.getenv("DB_PORT", 5432),
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
    )


class PipelineAuditor:
    """
    Context manager + manual interface for auditing ETL pipeline runs.

    Usage (recommended — context manager):
    ---------------------------------------
        with PipelineAuditor("silver_customers", "dim_customer", "silver") as auditor:
            # ... do ETL work ...
            auditor.log_quality_check("no_null_customer_id", rows_checked=1000, rows_failed=3)
            auditor.log_rejected_record("cust_999", "null customer_id", raw_row)
            auditor.set_row_counts(rows_read=1000, rows_written=995, rows_rejected=5)
            # on clean exit  → status = 'success'
            # on exception   → status = 'failed' + error_message captured

    Usage (manual):
    ---------------
        auditor = PipelineAuditor("silver_orders", "fact_orders", "silver")
        auditor.start()
        try:
            # ... ETL work ...
            auditor.end(status="success", rows_read=500, rows_written=498, rows_rejected=2)
        except Exception as e:
            auditor.end(status="failed", error_message=str(e))
            raise
    """

    def __init__(self, pipeline_name: str, table_name: str, layer: str):
        """
        Args:
            pipeline_name : Script identifier e.g. 'silver_customers'
            table_name    : Target table e.g. 'dim_customer'
            layer         : One of: bronze, silver, gold, warehouse
        """
        self.pipeline_name = pipeline_name
        self.table_name = table_name
        self.layer = layer

        self.run_id: Optional[int] = None
        self._rows_read = 0
        self._rows_written = 0
        self._rows_rejected = 0
        self._conn = None

    # ----------------------------------------------------------
    # Context manager interface
    # ----------------------------------------------------------

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is not None:
            # An exception was raised inside the with block
            self.end(
                status="failed",
                error_message=f"{exc_type.__name__}: {exc_val}"
            )
        else:
            self.end(status="success")
        return False  # don't suppress the exception

    # ----------------------------------------------------------
    # Core methods
    # ----------------------------------------------------------

    def start(self) -> int:
        """
        Inserts a 'running' row into audit.pipeline_runs.
        Returns the generated run_id.
        """
        sql = """
            INSERT INTO audit.pipeline_runs
                (pipeline_name, table_name, layer, status, started_at)
            VALUES (%s, %s, %s, 'running', %s)
            RETURNING run_id
        """
        try:
            self._conn = _get_connection()
            with self._conn.cursor() as cur:
                cur.execute(sql, (
                    self.pipeline_name,
                    self.table_name,
                    self.layer,
                    datetime.now(timezone.utc)
                ))
                self.run_id = cur.fetchone()[0]
            self._conn.commit()
            logger.info(f"Audit started | run_id={self.run_id} | {self.pipeline_name} → {self.table_name}")
            return self.run_id

        except Exception as e:
            logger.error(f"Failed to start audit run: {e}")
            raise

    def end(
        self,
        status: str,
        rows_read: Optional[int] = None,
        rows_written: Optional[int] = None,
        rows_rejected: Optional[int] = None,
        error_message: Optional[str] = None,
    ):
        """
        Updates the pipeline_runs row with final status and row counts.

        Args:
            status        : 'success' or 'failed'
            rows_read     : Total rows read from source (overrides set_row_counts)
            rows_written  : Total rows written to target
            rows_rejected : Total rows that failed validation
            error_message : Exception message if status='failed'
        """
        if self.run_id is None:
            logger.warning("end() called before start() — nothing to update.")
            return

        # Allow overriding counts here or use what was set via set_row_counts()
        rows_read     = rows_read     if rows_read     is not None else self._rows_read
        rows_written  = rows_written  if rows_written  is not None else self._rows_written
        rows_rejected = rows_rejected if rows_rejected is not None else self._rows_rejected

        sql = """
            UPDATE audit.pipeline_runs SET
                status          = %s,
                rows_read       = %s,
                rows_written    = %s,
                rows_rejected   = %s,
                error_message   = %s,
                ended_at        = %s
            WHERE run_id = %s
        """
        try:
            with self._conn.cursor() as cur:
                cur.execute(sql, (
                    status,
                    rows_read,
                    rows_written,
                    rows_rejected,
                    error_message,
                    datetime.now(timezone.utc),
                    self.run_id
                ))
            self._conn.commit()
            logger.info(
                f"Audit ended   | run_id={self.run_id} | status={status} | "
                f"read={rows_read} written={rows_written} rejected={rows_rejected}"
            )
        except Exception as e:
            logger.error(f"Failed to end audit run: {e}")
            raise
        finally:
            if self._conn:
                self._conn.close()

    # ----------------------------------------------------------
    # Row count helper (call during ETL, before end())
    # ----------------------------------------------------------

    def set_row_counts(self, rows_read: int = 0, rows_written: int = 0, rows_rejected: int = 0):
        """
        Stores row counts to be flushed on end().
        Useful when counts are accumulated incrementally.

        Example:
            auditor.set_row_counts(rows_read=1000, rows_written=994, rows_rejected=6)
        """
        self._rows_read     = rows_read
        self._rows_written  = rows_written
        self._rows_rejected = rows_rejected

    # ----------------------------------------------------------
    # Data quality checks
    # ----------------------------------------------------------

    def log_quality_check(
        self,
        check_name: str,
        rows_checked: int,
        rows_failed: int,
        status: Optional[str] = None,
    ):
        """
        Logs a data quality check result to audit.data_quality_checks.
        Status is auto-derived if not provided:
            rows_failed == 0  → 'passed'
            rows_failed > 0   → 'failed'

        Args:
            check_name   : Short descriptive name e.g. 'no_null_customer_id'
            rows_checked : How many rows were evaluated
            rows_failed  : How many rows failed the check
            status       : Override auto-derived status if needed ('passed','failed','warning')

        Example:
            auditor.log_quality_check("no_null_customer_id", rows_checked=1000, rows_failed=3)
        """
        if self.run_id is None:
            logger.warning("log_quality_check() called before start().")
            return

        if status is None:
            status = "passed" if rows_failed == 0 else "failed"

        sql = """
            INSERT INTO audit.data_quality_checks
                (run_id, table_name, check_name, rows_checked, rows_failed, status, checked_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
        try:
            with self._conn.cursor() as cur:
                cur.execute(sql, (
                    self.run_id,
                    self.table_name,
                    check_name,
                    rows_checked,
                    rows_failed,
                    status,
                    datetime.now(timezone.utc),
                ))
            self._conn.commit()
            logger.info(f"QC check | {check_name} | {status} | failed={rows_failed}/{rows_checked}")

        except Exception as e:
            logger.error(f"Failed to log quality check '{check_name}': {e}")
            raise

    # ----------------------------------------------------------
    # Rejected records
    # ----------------------------------------------------------

    def log_rejected_record(
        self,
        record_id: str,
        rejection_reason: str,
        raw_data: Optional[dict] = None,
    ):
        """
        Logs a single rejected row to audit.rejected_records.

        Args:
            record_id        : Business key of the bad row e.g. 'cust_9923'
            rejection_reason : Why it was rejected e.g. 'null customer_id'
            raw_data         : The actual row as a dict (stored as JSONB)

        Example:
            auditor.log_rejected_record(
                record_id="cust_9923",
                rejection_reason="null customer_id",
                raw_data=row.to_dict()
            )
        """
        if self.run_id is None:
            logger.warning("log_rejected_record() called before start().")
            return

        # Serialize raw_data — convert Timestamps and other
        # non-JSON-serializable types to strings before storing as JSONB
        if raw_data is not None:
            raw_data = {
                k: v.isoformat() if hasattr(v, "isoformat") else
                   None if (isinstance(v, float) and pd.isna(v)) else v
                for k, v in raw_data.items()
            }

        sql = """
            INSERT INTO audit.rejected_records
                (run_id, table_name, layer, record_id, rejection_reason, raw_data, rejected_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
        try:
            with self._conn.cursor() as cur:
                cur.execute(sql, (
                    self.run_id,
                    self.table_name,
                    self.layer,
                    record_id,
                    rejection_reason,
                    Json(raw_data) if raw_data else None,
                    datetime.now(timezone.utc),
                ))
            self._conn.commit()
            logger.debug(f"Rejected record | {record_id} | {rejection_reason}")

        except Exception as e:
            logger.error(f"Failed to log rejected record '{record_id}': {e}")
            raise