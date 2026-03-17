# E-Commerce Data Warehouse

A production-grade end-to-end data engineering project simulating a real-world e-commerce data platform. Built from scratch covering schema design, data generation, medallion architecture, incremental ETL pipeline, orchestration, and analytics.

---

## Architecture
```
Source System          Bronze Layer                Silver Layer              Gold Layer                Warehouse
──────────────         ────────────                ────────────              ──────────                ─────────
data_generator/   →    data_lake/raw/         →    data_lake/           →    data_lake/           →    PostgreSQL
(Faker + Pandas)       YYYY-MM-DD/                 processed/               curated/                  (Star Schema)
                       (JSONL files)               YYYY-MM-DD/              YYYY-MM-DD/
                                                   (Parquet files)          (Parquet files)
```

This follows the **Medallion Architecture** (Bronze → Silver → Gold) used by modern data platforms like Databricks, AWS, and Azure Data Lake. Each layer is **date-partitioned** for incremental daily processing.

---

## Tech Stack

| Layer | Technology |
|---|---|
| Data Generation | Python, Faker, Pandas |
| Data Lake | JSONL (Bronze), Parquet (Silver/Gold) |
| Data Warehouse | PostgreSQL |
| ETL Pipeline | Python, Pandas, Pyarrow |
| Orchestration | Apache Airflow 2.8.1 |
| Containerization | Docker |
| Big Data | Apache Spark (upcoming) |
| Version Control | Git, GitHub |

---

## Project Structure
```
ecommerce-data-warehouse/
├── data_generator/              # Simulated source system
│   ├── config.py                # Constants, brand lists, price ranges
│   ├── db.py                    # Helper functions
│   ├── gen_categories.py        # dim_category generation
│   ├── gen_products.py          # dim_product generation (SCD2)
│   ├── gen_customers.py         # dim_customer generation (SCD2)
│   ├── gen_orders.py            # fact_orders generation
│   ├── gen_order_items.py       # fact_order_items generation
│   ├── gen_payments.py          # fact_payments generation
│   ├── gen_shipments.py         # fact_shipments generation
│   ├── gen_refunds.py           # fact_refunds generation
│   └── run_generator.py         # Master orchestration script
│
├── data_lake/
│   ├── raw/                     # Bronze layer — date-partitioned JSONL event files
│   │   └── YYYY-MM-DD/
│   ├── processed/               # Silver layer — date-partitioned cleaned Parquet files
│   │   └── YYYY-MM-DD/
│   └── curated/                 # Gold layer — date-partitioned business-ready Parquet files
│       └── YYYY-MM-DD/
│
├── warehouse/
│   ├── schema/
│   │   ├── create_tables.sql    # Star schema DDL (12 tables)
│   │   ├── indexes.sql          # FK indexes on all fact tables
│   │   ├── reset_schema.sql     # Development reset script
│   │   └── create_audit_schema.sql # Audit trail DDL
│   ├── queries/
│   │   └── business_questions.sql  # 6 analytical queries
│   └── seeds/
│       ├── dim_date_seed.sql        # Populates dim_date 2000-2035
│       └── dim_exchange_rate_seed.py # Populates daily exchange rates via yfinance
│
├── etl/
│   ├── extract/
│   │   ├── read_bronze.py       # Centralized Bronze JSONL reader with date partition detection
│   │   ├── read_silver.py       # Centralized Silver Parquet reader with date partition detection
│   │   └── read_gold.py         # Centralized Gold Parquet reader with date partition detection
│   ├── transform/
│   │   ├── silver/              # Silver layer transforms
│   │   │   ├── silver_dim_category.py
│   │   │   ├── silver_dim_product.py
│   │   │   ├── silver_dim_customer.py
│   │   │   ├── silver_fact_orders.py
│   │   │   ├── silver_fact_order_items.py
│   │   │   ├── silver_fact_payments.py
│   │   │   ├── silver_fact_shipments.py
│   │   │   └── silver_fact_refunds.py
│   │   └── gold/                # Gold layer transforms
│   │       ├── gold_fact_orders.py
│   │       ├── gold_fact_order_items.py
│   │       ├── gold_fact_payments.py
│   │       ├── gold_fact_refunds.py
│   │       └── gold_fact_customer_segment_snapshot.py
│   ├── load/
│   │   ├── run_loader.py        # Master orchestration script
│   │   ├── load_dim_category.py
│   │   ├── load_dim_customer.py
│   │   ├── load_dim_product.py
│   │   ├── load_fact_orders.py
│   │   ├── load_fact_order_items.py
│   │   ├── load_fact_payments.py
│   │   ├── load_fact_shipments.py
│   │   ├── load_fact_refunds.py
│   │   └── load_customer_segment_snapshot.py
│   └── utils/
│       ├── logger.py            # Shared logging setup
│       └── auditor.py           # PipelineAuditor — audit trail writer
│
├── airflow/
│   └── dags/
│       ├── data_generator_dag.py  # Daily generator — seeds exchange rates then generates Bronze
│       ├── silver_etl_dag.py      # Silver ETL — 8 tasks with parallel execution
│       ├── gold_etl_dag.py        # Gold ETL — 5 tasks
│       └── warehouse_load_dag.py  # Warehouse load — 9 tasks
│
├── spark/                       # PySpark ETL (upcoming)
├── tests/                       # Unit and integration tests (upcoming)
├── docs/
│   └── db_schema.png            # ER diagram
├── logs/                        # Pipeline log files (gitignored)
├── docker-compose.yaml          # Airflow + services orchestration
├── .env.example                 # Environment variable template
└── requirements.txt             # Python dependencies
```

---

## Data Model

Star schema with 6 dimension tables and 6 fact tables.

### Dimensions
| Table | Description | Type |
|---|---|---|
| `dim_date` | Date dimension 2000-2035 | Static |
| `dim_currency` | 16 supported currencies | Static |
| `dim_exchange_rate` | Daily exchange rates to INR | Daily incremental |
| `dim_category` | Product categories (2-level hierarchy) | One-time seed |
| `dim_product` | Products with brand, model, size | SCD Type 2 |
| `dim_customer` | Customers with location | SCD Type 2 |

### Facts
| Table | Description | Grain |
|---|---|---|
| `fact_orders` | Order header with totals | One row per order |
| `fact_order_items` | Line items per order | One row per product per order |
| `fact_payments` | Payment attempts | One row per attempt |
| `fact_shipments` | Shipment and delivery | One row per item per order |
| `fact_refunds` | Refund requests | One row per refund |
| `fact_customer_segment_snapshot` | Monthly RFM snapshots | One row per customer per month |

---

## Incremental Pipeline

The pipeline runs daily, generating and processing one date partition at a time.

| Generator | Schedule | Logic |
|---|---|---|
| dim_category | One-time seed | Skipped if warehouse already has categories |
| dim_product | Weekly (Mondays) | 100 products on first run, 3-5 weekly thereafter |
| dim_customer | Daily | 5-15 new customers per day |
| fact_orders | Daily | 10-20% of active customer base |
| fact_shipments | Daily | Queries warehouse for processing/shipped orders |
| fact_refunds | Daily | Queries warehouse for delivered orders in last 3 days |

**Date detection** — Each layer detects its partition date by scanning the latest folder in its data lake directory, ensuring the pipeline always processes the correct date regardless of when it runs.

---

## Airflow DAG Chain

Four chained DAGs run daily using `TriggerDagRunOperator`:

```
data_generator_dag (scheduled: 8 PM IST)
    └── triggers → silver_etl_dag
                      └── triggers → gold_etl_dag
                                        └── triggers → warehouse_load_etl_dag
```

Only `data_generator_dag` has a schedule. The remaining three DAGs run with `schedule_interval=None` and are triggered automatically on success of the previous DAG.

---

## Setup & Installation

### Prerequisites
- Python 3.10+
- PostgreSQL 14+
- Docker Desktop

### 1. Clone the repository
```bash
git clone https://github.com/bijoypantu/ecommerce-data-warehouse.git
cd ecommerce-data-warehouse
```

### 2. Create virtual environment
```bash
python -m venv venv
venv\Scripts\activate      # Windows
source venv/bin/activate   # Mac/Linux
```

### 3. Install dependencies
```bash
pip install -r requirements.txt
```

### 4. Configure environment variables
```bash
cp .env.example .env
# Edit .env with your PostgreSQL credentials
# Also add AIRFLOW_UID=50000 for Airflow Docker setup
```

### 5. Start Airflow and all services
```bash
docker compose up -d
```

Wait for all containers to become healthy, then open `http://localhost:8080`.

### 6. Initialize the warehouse schema
Run these scripts in DBeaver or psql in order:
```
warehouse/schema/create_tables.sql
warehouse/schema/indexes.sql
warehouse/schema/create_audit_schema.sql
warehouse/seeds/dim_date_seed.sql
```

### 7. Enable and trigger the pipeline

Enable `data_generator_dag` in the Airflow UI and trigger it manually for the first run. It will automatically:
1. Seed exchange rates via Yahoo Finance
2. Generate Bronze JSONL files for the current date partition
3. Trigger `silver_etl_dag` → `gold_etl_dag` → `warehouse_load_etl_dag` in sequence

The full pipeline runs daily at 8 PM IST automatically after the initial setup.

---

## Business Questions

Six analytical queries built on the star schema covering:

1. **Monthly Revenue Trends** — Revenue and order volume by month
2. **Top 10 Products by Revenue** — Best performing products with INR conversion
3. **Customer Segmentation** — RFM-based customer segments
4. **Category Performance** — Revenue breakdown by product category
5. **Customer Lifetime Value** — LTV percentiles with SCD2-aware calculation
6. **Refund Analysis** — Refund rates and reasons by category

See `warehouse/queries/business_questions.sql` for full queries.

---

## Project Phases

| Phase | Description | Status |
|---|---|---|
| 1 | Schema Design & Business Questions | ✅ Complete |
| 2 | Data Generation — Bronze Layer | ✅ Complete |
| 3 | Silver Layer ETL — Clean & Structure | ✅ Complete |
| 4 | Gold Layer ETL — Enrich & Convert | ✅ Complete |
| 5 | Warehouse Load | ✅ Complete |
| 6 | Airflow Orchestration | ✅ Complete |
| 7 | Incremental Architecture Redesign | ✅ Complete |
| 8 | Apache Spark ETL | ⏳ Upcoming |
| 9 | Analytics & Dashboards | ⏳ Upcoming |
| 10 | Documentation & Polish | ⏳ Upcoming |

---

## ETL Audit Trail

Every pipeline run is tracked in PostgreSQL under the `audit` schema:

- **`audit.pipeline_runs`** — records every execution with row counts and status
- **`audit.data_quality_checks`** — records results of each validation check
- **`audit.rejected_records`** — stores rows that failed validation with specific rejection reasons

Query pipeline health:
```sql
SELECT pipeline_name, status, rows_read, rows_written, rows_rejected, started_at
FROM audit.pipeline_runs
ORDER BY started_at DESC;
```

---

## Key Design Decisions

**Medallion Architecture** — Bronze/Silver/Gold separation ensures raw data is never modified. Each layer has a clear purpose and can be reprocessed independently.

**Date-Partitioned Data Lake** — Each layer stores data in `YYYY-MM-DD/` partitions. This enables incremental processing, independent reprocessing of any date, and efficient reads without full scans.

**Partition Date Detection** — Each reader (`read_bronze`, `read_silver`, `read_gold`) detects the latest partition by scanning the data lake directory. This decouples date logic from ETL code and works correctly regardless of when the pipeline runs.

**Event-Driven Bronze Layer** — Raw JSONL files store events not states. `fact_orders.jsonl` contains `order_created`, `order_totals_updated`, `order_processing`, `order_shipped`, `order_cancelled` and `order_delivered` events separately — simulating real CDC (Change Data Capture) patterns.

**SCD Type 2 Dimensions** — Both `dim_customer` and `dim_product` track historical changes. Customer location updates and product discontinuations are preserved as separate versions, enabling point-in-time analysis.

**Currency Strategy** — All amounts stored in original transaction currency in Bronze and Silver. INR conversion happens in the Gold ETL layer using daily exchange rates from `dim_exchange_rate`, ensuring clean separation between raw data and derived values.

**Surrogate Keys** — Generated by the warehouse loader at insert time using `GENERATED ALWAYS AS IDENTITY`. Business keys are preserved alongside surrogate keys for traceability. FK resolution (e.g. `parent_category_sk`, `customer_sk`) happens exclusively in the loader.

**Centralized Layer Readers** — `read_bronze`, `read_silver`, and `read_gold` are the single entry points for reads at each layer. Each handles partition path resolution, corrupt record handling (Bronze), type inference (Silver/Gold), and event_type filtering — DRY principle applied across all transforms.

**UPSERT Loaders** — Fact tables with evolving state (`fact_orders`, `fact_shipments`, `fact_refunds`) use `ON CONFLICT DO UPDATE` to apply status changes incrementally. Append-only facts (`fact_order_items`, `fact_payments`) use `ON CONFLICT DO NOTHING`.

**Chained DAG Orchestration** — Four separate DAGs connected via `TriggerDagRunOperator` provide independent scheduling, monitoring, and reprocessing per layer. Only the generator DAG has a schedule — downstream DAGs run on trigger only, allowing any layer to be rerun independently.

**Audit-First Pipeline** — Every ETL script wraps execution in `PipelineAuditor` — tracking row counts, data quality check results, and rejected records directly to PostgreSQL in real time.

---

## Author

**Bijoy Pantu**  
**Data Engineering Portfolio Project**