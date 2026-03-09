# E-Commerce Data Warehouse

A production-grade end-to-end data engineering project simulating a real-world e-commerce data platform. Built from scratch covering schema design, data generation, medallion architecture, ETL pipeline, orchestration, and analytics.

---

## Architecture
```
Source System          Bronze Layer           Silver Layer          Gold Layer            Warehouse
──────────────         ────────────           ────────────          ──────────            ─────────
data_generator/   →    data_lake/raw/    →    data_lake/       →    data_lake/       →    PostgreSQL
(Faker + Pandas)       (JSONL files)          processed/            curated/              (Star Schema)
                                              (Parquet files)       (Parquet files)
```

This follows the **Medallion Architecture** (Bronze → Silver → Gold) used by modern data platforms like Databricks, AWS, and Azure Data Lake.

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
│   ├── raw/                     # Bronze layer — JSONL event files
│   ├── processed/               # Silver layer — cleaned Parquet files
│   └── curated/                 # Gold layer — business-ready Parquet files
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
│       └── dim_exchange_rate_seed.py # Populates exchange rates via yfinance
│
├── etl/
│   ├── extract/
│   │   └── read_bronze.py       # Centralized Bronze JSONL reader
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
│       ├── silver_etl_dag.py    # Silver ETL — 8 tasks with parallel execution
│       ├── gold_etl_dag.py      # Gold ETL — 5 tasks
│       └── warehouse_load_dag.py # Warehouse load — 9 tasks
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
| `dim_exchange_rate` | Daily exchange rates to INR | Static |
| `dim_category` | Product categories (2-level hierarchy) | Static |
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

## Generated Dataset

| Table | Rows | Notes |
|---|---|---|
| dim_category | 60 | 10 parents, 50 sub-categories |
| dim_product | ~120 | 100 base + ~20 SCD2 versions |
| dim_customer | ~1,050 | 1,000 base + 50 SCD2 versions |
| fact_orders | ~9,985 | All events combined |
| fact_order_items | ~18,029 | ~1.8 items per order average |
| fact_payments | ~12,934 | Includes retry attempts |
| fact_shipments | ~12,161 | One record per item per order |
| fact_refunds | ~895 | ~10% of delivered orders |
| fact_customer_segment_snapshot | ~29,451 | Monthly RFM snapshots |

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

### 5. Start PostgreSQL with Docker
```bash
docker compose up -d postgres
```

### 6. Initialize the warehouse schema
Run these scripts in DBeaver or psql in order:
```
warehouse/schema/create_tables.sql
warehouse/schema/indexes.sql
warehouse/schema/create_audit_schema.sql
warehouse/seeds/dim_date_seed.sql
warehouse/seeds/dim_exchange_rate_seed.py
```

### 7. Generate the dataset
```bash
python -m data_generator.run_generator
```

This generates ~85K rows across 8 JSONL files in `data_lake/raw/`. Takes approximately 2 minutes.

### 8. Run Silver ETL
```bash
python -m etl.transform.silver.silver_dim_category
python -m etl.transform.silver.silver_dim_product
python -m etl.transform.silver.silver_dim_customer
python -m etl.transform.silver.silver_fact_orders
python -m etl.transform.silver.silver_fact_order_items
python -m etl.transform.silver.silver_fact_payments
python -m etl.transform.silver.silver_fact_shipments
python -m etl.transform.silver.silver_fact_refunds
```

### 9. Run Gold ETL
```bash
python -m etl.transform.gold.gold_fact_orders
python -m etl.transform.gold.gold_fact_order_items
python -m etl.transform.gold.gold_fact_payments
python -m etl.transform.gold.gold_fact_refunds
python -m etl.transform.gold.gold_fact_customer_segment_snapshot
```

### 10. Load the warehouse
```bash
python -m etl.load.run_loader
```
This runs all 9 loaders in dependency order. Takes approximately 1 minute.

### 11. Start Airflow
```bash
docker compose up -d
```

Wait for all containers to become healthy, then open `http://localhost:8080`.

Enable and trigger DAGs in this order:
1. `silver_etl_dag`
2. `gold_etl_dag`
3. `warehouse_load_dag`

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
| 7 | Incremental Architecture Redesign | 🔄 In Progress |
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

**Event-Driven Bronze Layer** — Raw JSONL files store events not states. `fact_orders.jsonl` contains `order_created`, `order_totals_updated`, `order_processing`, `order_shipped`, `order_cancelled` and `order_delivered` events separately — simulating real CDC (Change Data Capture) patterns.

**SCD Type 2 Dimensions** — Both `dim_customer` and `dim_product` track historical changes. Customer location updates and product discontinuations are preserved as separate versions, enabling point-in-time analysis.

**Currency Strategy** — All amounts stored in original transaction currency in Bronze and Silver. INR conversion happens in the Gold ETL layer using daily exchange rates from `dim_exchange_rate`, ensuring clean separation between raw data and derived values.

**Surrogate Keys** — Generated by the warehouse loader at insert time using `GENERATED ALWAYS AS IDENTITY`. Business keys are preserved alongside surrogate keys for traceability. FK resolution (e.g. `parent_category_sk`, `customer_sk`) happens exclusively in the loader.

**Centralized Bronze Reader** — `etl/extract/read_bronze.py` is the single entry point for all Bronze reads. Handles corrupt line skipping, timestamp parsing, and event_type filtering — DRY principle applied across all 8 Silver transforms.

**Audit-First Pipeline** — Every ETL script wraps execution in `PipelineAuditor` — tracking row counts, data quality check results, and rejected records directly to PostgreSQL in real time.

**Gold Layer Enrichment** — Currency conversion (_inr columns) happens in Gold using daily exchange rates. `fact_customer_segment_snapshot` is built entirely in Gold via LTM RFM aggregation across 63 monthly snapshots. Dims and `fact_shipments` flow directly Silver → Loader.

**Airflow Orchestration** — Three separate DAGs for Silver, Gold, and Warehouse Load provide independent scheduling, monitoring, and reprocessing per layer. Tasks within each DAG run in parallel where FK dependencies allow, matching the same dependency order as manual execution. All DAGs run inside Docker via the official Airflow 2.8.1 CeleryExecutor setup.

---

## Author

**Bijoy Pantu**  
**Data Engineering Portfolio Project**