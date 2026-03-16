-- ============================================================
-- E-Commerce Data Warehouse
-- Schema: dw
-- All tables in correct creation order (respects FK dependencies)
-- Change: revenue_ltm renamed to revenue_ltm_inr in fact_customer_segment_snapshot
-- ============================================================

CREATE SCHEMA IF NOT EXISTS dw;

-- ------------------------------------------------------------
-- 1. dim_date
-- ------------------------------------------------------------
CREATE TABLE dw.dim_date (
    date_sk         INT NOT NULL,
    full_date       DATE NOT NULL,
    year            SMALLINT NOT NULL,
    quarter         SMALLINT NOT NULL,
    month_number    SMALLINT NOT NULL,
    month_name      VARCHAR(10) NOT NULL,
    day_of_month    SMALLINT NOT NULL,
    day_name        VARCHAR(10) NOT NULL,
    day_of_week     SMALLINT NOT NULL,
    week_of_year    SMALLINT NOT NULL,
    is_weekend      BOOLEAN NOT NULL,

    CONSTRAINT pk_dim_date PRIMARY KEY (date_sk),
    CONSTRAINT uq_dim_date_full_date UNIQUE (full_date),
    CONSTRAINT chk_dim_date_quarter CHECK (quarter BETWEEN 1 AND 4),
    CONSTRAINT chk_dim_date_month CHECK (month_number BETWEEN 1 AND 12),
    CONSTRAINT chk_dim_date_day CHECK (day_of_month BETWEEN 1 AND 31),
    CONSTRAINT chk_dim_date_isodow CHECK (day_of_week BETWEEN 1 AND 7),
    CONSTRAINT chk_dim_date_week CHECK (week_of_year BETWEEN 1 AND 53)
);

-- ------------------------------------------------------------
-- 2. dim_currency
-- ------------------------------------------------------------
CREATE TABLE dw.dim_currency (
    currency_code   VARCHAR(3) NOT NULL,
    currency_name   TEXT NULL,

    CONSTRAINT pk_dim_currency PRIMARY KEY (currency_code)
);

-- ------------------------------------------------------------
-- 3. dim_category
-- ------------------------------------------------------------
CREATE TABLE dw.dim_category (
    category_sk         BIGINT GENERATED ALWAYS AS IDENTITY NOT NULL,
    category_id         VARCHAR(50) NOT NULL,
    category_name       VARCHAR(100) NOT NULL,
    parent_category_sk  BIGINT NULL,

    CONSTRAINT pk_dim_category PRIMARY KEY (category_sk),
    CONSTRAINT uq_dim_category UNIQUE (category_id),
    CONSTRAINT fk_dim_category_parent FOREIGN KEY (parent_category_sk)
        REFERENCES dw.dim_category(category_sk) ON DELETE RESTRICT
);

-- ------------------------------------------------------------
-- 4. dim_customer  (SCD Type 2)
-- ------------------------------------------------------------
CREATE TABLE dw.dim_customer (
    customer_sk         BIGINT GENERATED ALWAYS AS IDENTITY NOT NULL,
    customer_id         VARCHAR(50) NOT NULL,
    first_name          TEXT NOT NULL,
    last_name           TEXT NULL,
    date_of_birth       DATE NULL,
    email               VARCHAR(100) NULL,
    mobile_no           VARCHAR(15) NULL,
    city                VARCHAR(100) NULL,
    state               VARCHAR(100) NULL,
    country             VARCHAR(100) NOT NULL,
    signup_timestamp    TIMESTAMPTZ NOT NULL,
    effective_start     TIMESTAMPTZ NOT NULL,
    effective_end       TIMESTAMPTZ NULL,
    is_current          BOOLEAN NOT NULL DEFAULT TRUE,
    gender              VARCHAR(20) NULL,

    CONSTRAINT pk_dim_customer PRIMARY KEY (customer_sk),
    CONSTRAINT uq_dim_customer UNIQUE (customer_id, effective_start),
    CONSTRAINT chk_cust_date_range CHECK (effective_end IS NULL OR effective_end > effective_start),
    CONSTRAINT chk_dim_customer_gender CHECK (gender IN ('male', 'female', 'other'))
);

-- ------------------------------------------------------------
-- 5. dim_product  (SCD Type 2)
-- ------------------------------------------------------------
CREATE TABLE dw.dim_product (
    product_sk      BIGINT GENERATED ALWAYS AS IDENTITY NOT NULL,
    product_id      VARCHAR(50) NOT NULL,
    product_name    TEXT NOT NULL,
    brand           VARCHAR(100) NOT NULL,
    model           VARCHAR(100) NOT NULL,
    color           VARCHAR(50) NULL,
    size            VARCHAR(50) NULL,
    category_sk     BIGINT NOT NULL,
    product_status  VARCHAR(20) NOT NULL,
    effective_start TIMESTAMPTZ NOT NULL,
    effective_end   TIMESTAMPTZ NULL,
    is_current      BOOLEAN NOT NULL DEFAULT TRUE,

    CONSTRAINT pk_dim_product PRIMARY KEY (product_sk),
    CONSTRAINT uk_dim_product UNIQUE (product_id, effective_start),
    CONSTRAINT fk_dim_product_category FOREIGN KEY (category_sk)
        REFERENCES dw.dim_category(category_sk) ON DELETE RESTRICT,
    CONSTRAINT chk_dim_product_status CHECK (product_status IN ('active', 'discontinued')),
    CONSTRAINT chk_dim_product_date_range CHECK (effective_end IS NULL OR effective_end > effective_start)
);

-- ------------------------------------------------------------
-- 6. dim_exchange_rate
-- ------------------------------------------------------------
CREATE TABLE dw.dim_exchange_rate (
    exchange_rate_sk    BIGINT GENERATED ALWAYS AS IDENTITY NOT NULL,
    date_sk             INT NOT NULL,
    currency_code       VARCHAR(3) NOT NULL,
    rate_to_inr         NUMERIC(12, 6) NOT NULL,
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP NULL,

    CONSTRAINT pk_exchange_rate PRIMARY KEY (exchange_rate_sk),
    CONSTRAINT uq_exchange_rate UNIQUE (date_sk, currency_code),
    CONSTRAINT fk_exchange_rate_date FOREIGN KEY (date_sk)
        REFERENCES dw.dim_date(date_sk),
    CONSTRAINT fk_exchange_rate_currency FOREIGN KEY (currency_code)
        REFERENCES dw.dim_currency(currency_code)
);

-- ------------------------------------------------------------
-- 7. fact_orders
-- ------------------------------------------------------------
CREATE TABLE dw.fact_orders (
    order_sk                    BIGINT GENERATED BY DEFAULT AS IDENTITY NOT NULL,
    order_id                    VARCHAR(50) NOT NULL,
    customer_sk                 BIGINT NOT NULL,
    date_sk                     INT NOT NULL,
    order_created_at            TIMESTAMPTZ NOT NULL,
    order_last_updated_at       TIMESTAMPTZ NOT NULL,
    order_status                VARCHAR(20) NOT NULL,
    order_channel               VARCHAR(20) NOT NULL,
    total_order_amount          NUMERIC(18, 2) NOT NULL,
    order_discount_total        NUMERIC(18, 2) NOT NULL,
    currency_code               VARCHAR(3) NOT NULL,
    total_order_amount_inr      NUMERIC(18, 2) NULL,
    order_discount_total_inr    NUMERIC(18, 2) NULL,

    CONSTRAINT pk_fact_orders PRIMARY KEY (order_sk),
    CONSTRAINT uq_fact_orders_order_id UNIQUE (order_id),
    CONSTRAINT fk_fact_orders_cust FOREIGN KEY (customer_sk)
        REFERENCES dw.dim_customer(customer_sk),
    CONSTRAINT fk_fact_orders_date FOREIGN KEY (date_sk)
        REFERENCES dw.dim_date(date_sk),
    CONSTRAINT fk_fact_orders_currency FOREIGN KEY (currency_code)
        REFERENCES dw.dim_currency(currency_code),
    CONSTRAINT chk_fact_orders_status CHECK (order_status IN ('created','processing','shipped','cancelled','delivered')),
    CONSTRAINT chk_fact_orders_channel CHECK (order_channel IN ('web','mobile','marketplace')),
    CONSTRAINT chk_fact_orders_amount CHECK (total_order_amount >= 0),
    CONSTRAINT chk_fact_orders_discnt CHECK (
        order_discount_total >= 0 
        AND order_discount_total <= total_order_amount
    ),
    CONSTRAINT chk_fact_orders_date_range CHECK (order_last_updated_at >= order_created_at)
);

-- ------------------------------------------------------------
-- 8. fact_order_items
-- ------------------------------------------------------------
CREATE TABLE dw.fact_order_items (
    order_item_sk           BIGINT GENERATED BY DEFAULT AS IDENTITY NOT NULL,
    order_item_id           VARCHAR(50) NOT NULL,
    order_sk                BIGINT NOT NULL,
    product_sk              BIGINT NOT NULL,
    customer_sk             BIGINT NOT NULL,
    date_sk                 INT NOT NULL,
    quantity                INT NOT NULL,
    unit_price_at_order     NUMERIC(18, 2) NOT NULL,
    discount_amount         NUMERIC(18, 2) NOT NULL,
    line_total_amount       NUMERIC(18, 2) NOT NULL,
    line_total_amount_inr   NUMERIC(18, 2) NULL,

    CONSTRAINT pk_fact_order_items PRIMARY KEY (order_item_sk),
    CONSTRAINT uq_fact_order_items_id UNIQUE (order_sk, order_item_id),
    CONSTRAINT fk_fact_order_items_order FOREIGN KEY (order_sk)
        REFERENCES dw.fact_orders(order_sk) ON DELETE RESTRICT,
    CONSTRAINT fk_fact_order_items_prdct FOREIGN KEY (product_sk)
        REFERENCES dw.dim_product(product_sk) ON DELETE RESTRICT,
    CONSTRAINT fk_fact_order_items_cust FOREIGN KEY (customer_sk)
        REFERENCES dw.dim_customer(customer_sk) ON DELETE RESTRICT,
    CONSTRAINT fk_fact_order_items_date FOREIGN KEY (date_sk)
        REFERENCES dw.dim_date(date_sk) ON DELETE RESTRICT,
    CONSTRAINT chk_fact_order_items_qntity CHECK (quantity > 0),
    CONSTRAINT chk_fact_order_items_unit_price CHECK (unit_price_at_order >= 0),
    CONSTRAINT chk_fact_order_items_discnt_amnt CHECK (
        discount_amount >= 0 AND discount_amount <= quantity * unit_price_at_order
    ),
    CONSTRAINT chk_fact_order_items_total_amnt CHECK (line_total_amount >= 0)
);

-- ------------------------------------------------------------
-- 9. fact_payments
-- ------------------------------------------------------------
CREATE TABLE dw.fact_payments (
    payment_attempt_sk      BIGINT GENERATED BY DEFAULT AS IDENTITY NOT NULL,
    payment_attempt_id      VARCHAR(50) NOT NULL,
    order_sk                BIGINT NOT NULL,
    customer_sk             BIGINT NOT NULL,
    payment_date_sk         INT NOT NULL,
    payment_timestamp       TIMESTAMPTZ NOT NULL,
    payment_method          VARCHAR(20) NOT NULL,
    payment_provider        VARCHAR(100) NULL,
    payment_status          VARCHAR(20) NOT NULL,
    gateway_response_code   VARCHAR(50) NULL,
    amount                  NUMERIC(18, 2) NOT NULL,
    currency_code           VARCHAR(3) NOT NULL,
    amount_inr              NUMERIC(18, 2) NULL,

    CONSTRAINT pk_fact_payments PRIMARY KEY (payment_attempt_sk),
    CONSTRAINT uq_fact_payments_order_attempt UNIQUE (order_sk, payment_attempt_id),
    CONSTRAINT fk_fact_payments_order FOREIGN KEY (order_sk)
        REFERENCES dw.fact_orders(order_sk) ON DELETE RESTRICT,
    CONSTRAINT fk_fact_payments_cust FOREIGN KEY (customer_sk)
        REFERENCES dw.dim_customer(customer_sk) ON DELETE RESTRICT,
    CONSTRAINT fk_fact_payments_date FOREIGN KEY (payment_date_sk)
        REFERENCES dw.dim_date(date_sk) ON DELETE RESTRICT,
    CONSTRAINT fk_fact_payments_currency FOREIGN KEY (currency_code)
        REFERENCES dw.dim_currency(currency_code),
    CONSTRAINT chk_fact_payments_methods CHECK (
        payment_method IN ('credit_card','debit_card','upi','net_banking','cod','wallet')
    ),
    CONSTRAINT chk_fact_payments_status CHECK (
        payment_status IN ('success','failed','pending','cancelled')
    ),
    CONSTRAINT chk_fact_payments_amnt CHECK (amount > 0)
);

-- ------------------------------------------------------------
-- 10. fact_shipments
-- ------------------------------------------------------------
CREATE TABLE dw.fact_shipments (
    shipment_sk         BIGINT GENERATED BY DEFAULT AS IDENTITY NOT NULL,
    shipment_id         VARCHAR(50) NOT NULL,
    order_sk            BIGINT NOT NULL,
    order_item_sk       BIGINT NOT NULL,
    customer_sk         BIGINT NOT NULL,
    shipment_date_sk    INT NOT NULL,
    delivery_date_sk    INT NULL,
    shipped_at          TIMESTAMPTZ NOT NULL,
    delivered_at        TIMESTAMPTZ NULL,
    shipment_status     VARCHAR(10) NOT NULL,
    carrier             VARCHAR(50),
    tracking_id         VARCHAR(100) NULL,
    shipped_quantity    INT NOT NULL,

    CONSTRAINT pk_fact_shipments PRIMARY KEY (shipment_sk),
    CONSTRAINT uq_fact_shipments_attempt UNIQUE (order_item_sk, shipment_id),
    CONSTRAINT fk_fact_shipments_order FOREIGN KEY (order_sk)
        REFERENCES dw.fact_orders(order_sk) ON DELETE RESTRICT,
    CONSTRAINT fk_fact_shipments_order_items FOREIGN KEY (order_item_sk)
        REFERENCES dw.fact_order_items(order_item_sk) ON DELETE RESTRICT,
    CONSTRAINT fk_fact_shipments_cust FOREIGN KEY (customer_sk)
        REFERENCES dw.dim_customer(customer_sk) ON DELETE RESTRICT,
    CONSTRAINT fk_fact_shipments_date FOREIGN KEY (shipment_date_sk)
        REFERENCES dw.dim_date(date_sk) ON DELETE RESTRICT,
    CONSTRAINT fk_fact_shipments_delivery_date FOREIGN KEY (delivery_date_sk)
        REFERENCES dw.dim_date(date_sk) ON DELETE RESTRICT,
    CONSTRAINT chk_fact_shipments_status CHECK (shipment_status IN ('shipped','delivered','failed')),
    CONSTRAINT chk_fact_shipments_qnty CHECK (shipped_quantity > 0),
    CONSTRAINT chk_fact_shipments_date_range CHECK (delivered_at IS NULL OR delivered_at >= shipped_at),
    CONSTRAINT chk_fact_shipments_delivery_criterias CHECK (
        (shipment_status = 'delivered' AND delivered_at IS NOT NULL AND delivery_date_sk IS NOT NULL)
        OR
        (shipment_status <> 'delivered' AND delivered_at IS NULL AND delivery_date_sk IS NULL)
    )
);

-- ------------------------------------------------------------
-- 11. fact_refunds
-- ------------------------------------------------------------
CREATE TABLE dw.fact_refunds (
    refund_sk           BIGINT GENERATED BY DEFAULT AS IDENTITY NOT NULL,
    refund_id           VARCHAR(50) NOT NULL,
    order_sk            BIGINT NOT NULL,
    order_item_sk       BIGINT NOT NULL,
    customer_sk         BIGINT NOT NULL,
    refund_date_sk      INT NOT NULL,
    initiated_at        TIMESTAMPTZ NOT NULL,
    processed_at        TIMESTAMPTZ NULL,
    refund_quantity     INT NOT NULL,
    refund_amount       NUMERIC(18, 2) NOT NULL,
    refund_reason       TEXT NOT NULL,
    refund_status       VARCHAR(20) NOT NULL,
    currency_code       VARCHAR(3) NOT NULL,
    refund_amount_inr   NUMERIC(18, 2) NULL,

    CONSTRAINT pk_fact_refunds PRIMARY KEY (refund_sk),
    CONSTRAINT uq_fact_refunds_attempt UNIQUE (order_item_sk, refund_id),
    CONSTRAINT fk_fact_refunds_order FOREIGN KEY (order_sk)
        REFERENCES dw.fact_orders(order_sk) ON DELETE RESTRICT,
    CONSTRAINT fk_fact_refunds_order_items FOREIGN KEY (order_item_sk)
        REFERENCES dw.fact_order_items(order_item_sk) ON DELETE RESTRICT,
    CONSTRAINT fk_fact_refunds_cust FOREIGN KEY (customer_sk)
        REFERENCES dw.dim_customer(customer_sk) ON DELETE RESTRICT,
    CONSTRAINT fk_fact_refunds_date FOREIGN KEY (refund_date_sk)
        REFERENCES dw.dim_date(date_sk) ON DELETE RESTRICT,
    CONSTRAINT fk_fact_refunds_currency FOREIGN KEY (currency_code)
        REFERENCES dw.dim_currency(currency_code),
    CONSTRAINT chk_fact_refunds_qnty CHECK (refund_quantity > 0),
    CONSTRAINT chk_fact_refunds_amnt CHECK (refund_amount > 0),
    CONSTRAINT chk_fact_refunds_status CHECK (
        refund_status IN ('initiated','approved','processed','rejected')
    ),
    CONSTRAINT chk_fact_refunds_date_range CHECK (processed_at IS NULL OR processed_at >= initiated_at),
    CONSTRAINT chk_fact_refunds_status_criteria CHECK (
        (refund_status = 'processed' AND processed_at IS NOT NULL)
        OR
        (refund_status <> 'processed' AND processed_at IS NULL)
    )
);

-- ------------------------------------------------------------
-- 12. fact_customer_segment_snapshot
-- Note: revenue_ltm renamed to revenue_ltm_inr for consistency
-- ------------------------------------------------------------
CREATE TABLE dw.fact_customer_segment_snapshot (
    snapshot_month          DATE NOT NULL,
    customer_sk             BIGINT NOT NULL,
    revenue_ltm_inr         NUMERIC(18, 2) NOT NULL,
    order_count_ltm         INT NOT NULL,
    revenue_percentile      NUMERIC(6, 5) NOT NULL,
    frequency_percentile    NUMERIC(6, 5) NOT NULL,
    composite_score         NUMERIC(6, 5) NOT NULL,
    segment_label           VARCHAR(10) NOT NULL,
    created_at              TIMESTAMP DEFAULT CURRENT_TIMESTAMP NULL,

    CONSTRAINT pk_customer_segment_snapshot PRIMARY KEY (snapshot_month, customer_sk),
    CONSTRAINT fk_customer_segment_snapshot_customer FOREIGN KEY (customer_sk)
        REFERENCES dw.dim_customer(customer_sk),
    CONSTRAINT chk_segment_label CHECK (segment_label IN ('high','medium','low')),
    CONSTRAINT chk_composite_score CHECK (composite_score >= 0 AND composite_score <= 1),
    CONSTRAINT chk_snapshot_month_end CHECK (
        snapshot_month = (date_trunc('month', snapshot_month) + interval '1 month - 1 day')::date
    )
);
