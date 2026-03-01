-- Indexing on FK's in fact_orders
create index idx_fact_orders_customer_sk
    on dw.fact_orders(customer_sk);
create index idx_fact_orders_date_sk
    on dw.fact_orders(date_sk);

-- Indexing on Fk's in fact_order_items
create index idx_fact_order_items_customer_sk
    on dw.fact_order_items(customer_sk);
create index idx_fact_order_items_date_sk
    on dw.fact_order_items(date_sk);
create index idx_fact_order_items_order_sk
    on dw.fact_order_items(order_sk);
create index idx_fact_order_items_product_sk
    on dw.fact_order_items(product_sk);

-- Indexing on FK's in fact_payments
create index idx_fact_payments_customer_sk
    on dw.fact_payments(customer_sk);
create index idx_fact_payments_payment_date_sk
    on dw.fact_payments(payment_date_sk);
create index idx_fact_payments_order_sk
    on dw.fact_payments(order_sk);

-- Indexing on FK's in fact_shipments
create index idx_fact_shipments_customer_sk
    on dw.fact_shipments(customer_sk);
create index idx_fact_shipments_shipment_date_sk
    on dw.fact_shipments(shipment_date_sk);
create index idx_fact_shipments_delivery_date_sk
    on dw.fact_shipments(delivery_date_sk);
create index idx_fact_shipments_order_sk
    on dw.fact_shipments(order_sk);
create index idx_fact_shipments_order_item_sk
    on dw.fact_shipments(order_item_sk);

-- Indexing on FK's in fact_refunds
create index idx_fact_refunds_customer_sk
    on dw.fact_refunds(customer_sk);
create index idx_fact_refunds_refund_date_sk
    on dw.fact_refunds(refund_date_sk);
create index idx_fact_refunds_order_sk
    on dw.fact_refunds(order_sk);
create index idx_fact_refunds_order_item_sk
    on dw.fact_refunds(order_item_sk);

-- Indexing on FK's in fact_customer_segment_snapshot
create INDEX idx_fact_customer_segment_snapshot_customer_sk 
    on dw.fact_customer_segment_snapshot(customer_sk);

-- Indexing on FK's in dim_product
create index idx_dim_product_category_sk
    on dw.dim_product(category_sk);

-- Indexing on FK's in dim_category
create index idx_dim_category_parent_category_sk
    on dw.dim_category(parent_category_sk);