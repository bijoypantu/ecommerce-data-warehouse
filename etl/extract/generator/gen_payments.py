# etl/extract/generator/gen_payments.py
# ============================================================
# Generates fact_payments DataFrame.
# Appends order_cancelled and order_processing events to orders_df.
# Returns payments_df, orders_df for Bronze layer writing.
# ============================================================

import random
import pandas as pd
from datetime import date, datetime, timezone, timedelta

from .config import (
    PAYMENT_PROVIDERS, PAYMENT_METHODS,
    PAYMENT_METHOD_WEIGHTS, GATEWAY_CODES
)
from .db import random_datetime_between


def generate_payments(orders_df):
    """
    Generates fact_payments DataFrame.
    Determines order cancellation and processing status.
    Appends status events to orders_df.
    Returns payments_df, orders_df.
    """

    print("\n[fact_payments] Generating payments...")

    # ----------------------------------------------------------
    # Get one row per order with real amounts
    # ----------------------------------------------------------
    order_totals = orders_df[
        orders_df["event_type"] == "order_totals_updated"
    ].copy()

    rows        = []
    order_events = []

    for _, order in order_totals.iterrows():
        order_id             = order["order_id"]
        customer_id          = order["customer_id"]
        order_created_at     = order["order_created_at"]
        currency_code        = order["currency_code"]
        total_order_amount   = order["total_order_amount"]

        # ----------------------------------------------------------
        # Decide if order gets cancelled — 15% chance
        # ----------------------------------------------------------
        is_cancelled = random.random() < 0.15

        if is_cancelled:
            # One failed/cancelled payment attempt
            payment_method = random.choices(
                PAYMENT_METHODS,
                weights=PAYMENT_METHOD_WEIGHTS,
                k=1
            )[0]
            # COD can't be cancelled with failed status
            if payment_method == "cod":
                payment_method = "credit_card"

            payment_timestamp = random_datetime_between(
                order_created_at,
                order_created_at + timedelta(minutes=30)
            )
            payment_status           = random.choice(["failed", "cancelled"])
            gateway_response_code    = random.choice(GATEWAY_CODES[payment_status])
            payment_provider         = random.choice(PAYMENT_PROVIDERS[payment_method])

            rows.append({
                "payment_attempt_id":   f"PAY-{order_id}-1",
                "order_id":             order_id,
                "customer_id":          customer_id,
                "payment_timestamp":    payment_timestamp,
                "payment_method":       payment_method,
                "payment_provider":     payment_provider,
                "payment_status":       payment_status,
                "gateway_response_code": gateway_response_code,
                "amount":               total_order_amount,
                "currency_code":        currency_code,
                "amount_inr":           None,
                "event_type":           "payment_attempted",
                "ingested_at":          datetime.now(timezone.utc).isoformat(),
            })

            # Append order_cancelled event
            order_events.append({
                "order_id":              order_id,
                "customer_id":           customer_id,
                "order_created_at":      order_created_at,
                "order_last_updated_at": payment_timestamp,
                "order_status":          "cancelled",
                "order_channel":         order["order_channel"],
                "total_order_amount":    total_order_amount,
                "order_discount_total":  order["order_discount_total"],
                "currency_code":         currency_code,
                "total_order_amount_inr":   None,
                "order_discount_total_inr": None,
                "event_type":            "order_cancelled",
                "ingested_at":           datetime.now(timezone.utc).isoformat(),
            })

        else:
            # ----------------------------------------------------------
            # Determine attempt count and payment method
            # ----------------------------------------------------------
            attempt_count = random.choices(
                [1, 2, 3], weights=[0.75, 0.15, 0.10], k=1
            )[0]

            attempt_timestamp = order_created_at

            for attempt_num in range(1, attempt_count + 1):
                # COD only on single attempt orders
                if attempt_count == 1:
                    payment_method = random.choices(
                        PAYMENT_METHODS,
                        weights=PAYMENT_METHOD_WEIGHTS,
                        k=1
                    )[0]
                else:
                    # No COD for retry attempts
                    non_cod_methods  = [m for m in PAYMENT_METHODS if m != "cod"]
                    non_cod_weights  = [
                        PAYMENT_METHOD_WEIGHTS[i]
                        for i, m in enumerate(PAYMENT_METHODS)
                        if m != "cod"
                    ]
                    # Normalize weights
                    total_weight     = sum(non_cod_weights)
                    non_cod_weights  = [w / total_weight for w in non_cod_weights]
                    payment_method   = random.choices(
                        non_cod_methods,
                        weights=non_cod_weights,
                        k=1
                    )[0]

                payment_timestamp = random_datetime_between(
                    attempt_timestamp,
                    attempt_timestamp + timedelta(minutes=30)
                )
                attempt_timestamp = payment_timestamp

                # Determine payment status
                if payment_method == "cod":
                    payment_status = "pending"
                elif attempt_num == attempt_count:
                    payment_status = "success"
                else:
                    payment_status = "failed"

                gateway_response_code = random.choice(GATEWAY_CODES[payment_status])
                payment_provider      = random.choice(PAYMENT_PROVIDERS[payment_method])

                rows.append({
                    "payment_attempt_id":    f"PAY-{order_id}-{attempt_num}",
                    "order_id":              order_id,
                    "customer_id":           customer_id,
                    "payment_timestamp":     payment_timestamp,
                    "payment_method":        payment_method,
                    "payment_provider":      payment_provider,
                    "payment_status":        payment_status,
                    "gateway_response_code": gateway_response_code,
                    "amount":                total_order_amount,
                    "currency_code":         currency_code,
                    "amount_inr":            None,
                    "event_type":            "payment_attempted",
                    "ingested_at":           datetime.now(timezone.utc).isoformat(),
                })

            # Append order_processing event after all attempts
            order_events.append({
                "order_id":              order_id,
                "customer_id":           customer_id,
                "order_created_at":      order_created_at,
                "order_last_updated_at": payment_timestamp,
                "order_status":          "processing",
                "order_channel":         order["order_channel"],
                "total_order_amount":    total_order_amount,
                "order_discount_total":  order["order_discount_total"],
                "currency_code":         currency_code,
                "total_order_amount_inr":   None,
                "order_discount_total_inr": None,
                "event_type":            "order_processing",
                "ingested_at":           datetime.now(timezone.utc).isoformat(),
            })

    payments_df = pd.DataFrame(rows)
    orders_df   = pd.concat(
        [orders_df, pd.DataFrame(order_events)],
        ignore_index=True
    )

    print(f"  Done. {len(payments_df)} payment attempts generated.")
    print(f"  Done. {len(order_events)} order status events appended.")
    return payments_df, orders_df