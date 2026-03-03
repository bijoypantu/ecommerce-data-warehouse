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
| Orchestration | Apache Airflow (upcoming) |
| Big Data | Apache Spark (upcoming) |
| Containerization | Docker |
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
│   ├── extract/                 # Reads from Bronze layer
│   ├── transform/               # Silver and Gold transformations
│   ├── load/                    # Loads to PostgreSQL warehouse
│   └── utils/                   # Shared logging and audit utilities
│
├── airflow/
│   └── dags/                    # Airflow DAGs (upcoming)
│
├── spark/                       # PySpark ETL (upcoming)
├── tests/                       # Unit and integration tests (upcoming)
├── docs/
│   └── db_schema.png            # ER diagram
├── logs/                        # Pipeline log files (gitignored)
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
| dim_product | ~1,200 | 1,000 base + ~200 SCD2 versions |
| dim_customer | ~10,500 | 10,000 base + 500 SCD2 versions |
| fact_orders | ~367,000 | All events combined |
| fact_order_items | ~180,000 | ~1.8 items per order average |
| fact_payments | ~129,000 | Includes retry attempts |
| fact_shipments | ~122,000 | One record per item per order |
| fact_refunds | ~9,000 | ~10% of delivered orders |

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
```

### 5. Start PostgreSQL with Docker
```bash
docker compose up -d
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

This generates ~800K rows across 8 JSONL files in `data_lake/raw/`. Takes approximately 10 minutes.

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
| 3 | Silver Layer ETL — Clean & Structure | 🔄 In Progress |
| 4 | Gold Layer ETL — Enrich & Convert | ⏳ Upcoming |
| 5 | Warehouse Load | ⏳ Upcoming |
| 6 | Airflow Orchestration | ⏳ Upcoming |
| 7 | Apache Spark ETL | ⏳ Upcoming |
| 8 | Analytics & Dashboards | ⏳ Upcoming |
| 9 | Documentation & Polish | ⏳ Upcoming |

---

## Key Design Decisions

**Medallion Architecture** — Bronze/Silver/Gold separation ensures raw data is never modified. Each layer has a clear purpose and can be reprocessed independently.

**Event-Driven Bronze Layer** — Raw JSONL files store events not states. `fact_orders.jsonl` contains `order_created`, `order_totals_updated`, `order_shipped`, and `order_delivered` events separately — simulating real CDC (Change Data Capture) patterns.

**SCD Type 2 Dimensions** — Both `dim_customer` and `dim_product` track historical changes. Customer location updates and product discontinuations are preserved as separate versions, enabling point-in-time analysis.

**Currency Strategy** — All amounts stored in original transaction currency. INR conversion happens in the Gold ETL layer using daily exchange rates from `dim_exchange_rate`, ensuring clean separation between raw data and derived values.

**Surrogate Keys** — Generated by the ETL pipeline, not the source system or database sequences. Business keys (`customer_id`, `product_id`) are preserved alongside surrogate keys for traceability.

---

## Author

Bijoy Pantu
Data Engineering Portfolio Project