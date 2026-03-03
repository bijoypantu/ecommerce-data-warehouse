# etl/utils/logger.py
# ============================================================
# Shared logging setup for all ETL pipeline scripts.
# Logs to both console and a daily rotating log file.
# ============================================================

import logging
import os
from datetime import datetime
from logging.handlers import TimedRotatingFileHandler


def get_logger(name: str) -> logging.Logger:
    """
    Returns a configured logger that writes to both:
    - Console (INFO level and above)
    - Daily log file in logs/ directory (DEBUG level and above)

    Args:
        name: Logger name — use __name__ from the calling script
    
    Usage:
        from etl.utils.logger import get_logger
        logger = get_logger(__name__)
        logger.info("Starting Silver ETL for dim_customer")
        logger.error("Failed to process row: invalid date")
    """

    logger = logging.getLogger(name)

    # Avoid adding duplicate handlers if logger already exists
    if logger.handlers:
        return logger

    logger.setLevel(logging.DEBUG)

    # ----------------------------------------------------------
    # Log format
    # ----------------------------------------------------------
    formatter = logging.Formatter(
        fmt="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )

    # ----------------------------------------------------------
    # Console handler — INFO and above
    # ----------------------------------------------------------
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)

    # ----------------------------------------------------------
    # File handler — DEBUG and above, rotates daily
    # ----------------------------------------------------------
    log_dir = "logs"
    os.makedirs(log_dir, exist_ok=True)

    log_filename = os.path.join(
        log_dir,
        f"pipeline_{datetime.now().strftime('%Y-%m-%d')}.log"
    )

    file_handler = TimedRotatingFileHandler(
        filename=log_filename,
        when="midnight",        # rotate at midnight
        interval=1,             # every 1 day
        backupCount=30,         # keep last 30 days
        encoding="utf-8"
    )
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)

    # ----------------------------------------------------------
    # Attach handlers
    # ----------------------------------------------------------
    logger.addHandler(console_handler)
    logger.addHandler(file_handler)

    return logger

"""
How it works:

get_logger(__name__) — every ETL script calls this once at the top.
__name__ gives the module name automatically so log messages show:

2026-03-03 10:15:32 | INFO     | etl.transform.silver_customers | Starting Silver ETL
2026-03-03 10:15:33 | ERROR    | etl.transform.silver_customers | Null value in customer_id
2026-03-03 10:15:34 | DEBUG    | etl.transform.silver_customers | Processing row 10234
"""