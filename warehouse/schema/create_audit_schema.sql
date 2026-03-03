-- ============================================================
-- Audit Schema DDL
-- Tracks pipeline runs, data quality checks, and rejected records
-- ============================================================

CREATE SCHEMA IF NOT EXISTS audit;

-- ------------------------------------------------------------
-- Pipeline Runs
-- Records every ETL pipeline execution with row counts and status
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS audit.pipeline_runs (
    run_id          BIGSERIAL PRIMARY KEY,
    pipeline_name   VARCHAR(100)    NOT NULL,
    table_name      VARCHAR(100)    NOT NULL,
    layer           VARCHAR(20)     NOT NULL,   -- bronze, silver, gold, warehouse
    rows_read       INTEGER         DEFAULT 0,
    rows_written    INTEGER         DEFAULT 0,
    rows_rejected   INTEGER         DEFAULT 0,
    status          VARCHAR(20)     NOT NULL,   -- running, success, failed
    error_message   TEXT,
    started_at      TIMESTAMPTZ     NOT NULL    DEFAULT NOW(),
    ended_at        TIMESTAMPTZ
);

-- ------------------------------------------------------------
-- Data Quality Checks
-- Records results of each data quality check per pipeline run
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS audit.data_quality_checks (
    check_id        BIGSERIAL PRIMARY KEY,
    run_id          BIGINT          NOT NULL REFERENCES audit.pipeline_runs(run_id),
    table_name      VARCHAR(100)    NOT NULL,
    check_name      VARCHAR(200)    NOT NULL,
    rows_checked    INTEGER         DEFAULT 0,
    rows_failed     INTEGER         DEFAULT 0,
    status          VARCHAR(20)     NOT NULL,   -- passed, failed, warning
    checked_at      TIMESTAMPTZ     NOT NULL    DEFAULT NOW()
);

-- ------------------------------------------------------------
-- Rejected Records
-- Stores rows that failed validation during ETL processing
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS audit.rejected_records (
    rejection_id    BIGSERIAL PRIMARY KEY,
    run_id          BIGINT          NOT NULL REFERENCES audit.pipeline_runs(run_id),
    table_name      VARCHAR(100)    NOT NULL,
    layer           VARCHAR(20)     NOT NULL,
    record_id       VARCHAR(100),               -- business key of rejected record
    rejection_reason TEXT           NOT NULL,
    raw_data        JSONB,                      -- stores the actual rejected row
    rejected_at     TIMESTAMPTZ     NOT NULL    DEFAULT NOW()
);

-- ------------------------------------------------------------
-- Indexes for audit tables
-- ------------------------------------------------------------
CREATE INDEX idx_pipeline_runs_table_name 
    ON audit.pipeline_runs(table_name);
CREATE INDEX idx_pipeline_runs_status 
    ON audit.pipeline_runs(status);
CREATE INDEX idx_pipeline_runs_started_at 
    ON audit.pipeline_runs(started_at);
CREATE INDEX idx_data_quality_checks_run_id 
    ON audit.data_quality_checks(run_id);
CREATE INDEX idx_rejected_records_run_id 
    ON audit.rejected_records(run_id);
CREATE INDEX idx_rejected_records_table_name 
    ON audit.rejected_records(table_name);