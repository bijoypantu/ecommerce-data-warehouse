# data_generator/gen_customers.py
# ============================================================
# Generates dim_customer rows for incremental daily generation.
# Pass 1 — generate 5-15 new customers for today
# Pass 2 — weekly SCD2: update location for 1-2 existing customers
#           only customers older than 3 months are eligible
# Returns customer_df for Bronze layer writing.
# ============================================================

import random
import pandas as pd
from datetime import date, datetime, timezone, timedelta
from faker import Faker

from .config import (
    COUNTRY_LOCALE, COUNTRY_WEIGHTS,
    MAIL_DOMAINS, COUNTRY_MOBILE_CODE,
)
from .db import random_date, random_datetime_between


def generate_customers(conn, generation_date):

    print("\n[dim_customer] Generating customers...")

    gen_dt      = datetime.combine(generation_date, datetime.min.time()).replace(tzinfo=timezone.utc)
    ingested_at = datetime.now(timezone.utc).isoformat()

    # Build Faker instances once per locale
    faker_instances = {
        country: Faker(locale)
        for country, locale in COUNTRY_LOCALE.items()
    }

    # ----------------------------------------------------------
    # Get current max customer number from warehouse
    # so new IDs continue from where we left off
    # ----------------------------------------------------------
    with conn.cursor() as cur:
        cur.execute("SELECT MAX(CAST(SUBSTRING(customer_id FROM 6) AS INTEGER)) FROM dw.dim_customer")
        result = cur.fetchone()[0]
    start_index = (result or 0) + 1

    rows = []

    # ----------------------------------------------------------
    # PASS 1 — Generate 5-15 new customers for today
    # ----------------------------------------------------------
    num_customers = random.randint(1, 5)

    for i in range(start_index, start_index + num_customers):
        customer_id = f"CUST-{i:05d}"

        gender  = random.choice(["male", "female", "other"])
        country = random.choices(
            list(COUNTRY_WEIGHTS.keys()),
            weights=list(COUNTRY_WEIGHTS.values()),
            k=1
        )[0]

        fake = faker_instances[country]

        try:
            if gender == "male":
                first_name = fake.first_name_male()
            elif gender == "female":
                first_name = fake.first_name_female()
            else:
                first_name = fake.first_name()
        except AttributeError:
            first_name = fake.first_name()

        last_name = fake.last_name()
        date_of_birth = random_date(date(1960, 1, 1), date(2008, 1, 1))

        clean_first = first_name.lower().replace(" ", "").replace("'", "")
        clean_last  = last_name.lower().replace(" ", "").replace("'", "")
        email_prefix = random.choice([
            fake.user_name(),
            f"{clean_first}.{clean_last}{random.randint(1, 999)}"
        ])
        email     = f"{email_prefix}@{random.choice(MAIL_DOMAINS)}"
        mobile_no = f"{COUNTRY_MOBILE_CODE[country]}{fake.msisdn()[3:13]}"

        try:
            city = fake.city()
        except AttributeError:
            city = None

        try:
            state = fake.state()
        except AttributeError:
            state = None

        signup_timestamp = random_datetime_between(
            gen_dt,
            gen_dt + timedelta(hours=23)
        )

        rows.append({
            "customer_id":      customer_id,
            "first_name":       first_name,
            "last_name":        last_name,
            "date_of_birth":    date_of_birth,
            "email":            email,
            "mobile_no":        mobile_no,
            "city":             city,
            "state":            state,
            "country":          country,
            "signup_timestamp": signup_timestamp,
            "effective_start":  signup_timestamp,
            "gender":           gender,
            "event_type":       "created",
            "ingested_at":      ingested_at,
        })

    print(f"  Pass 1 complete — {num_customers} new customers generated.")

    # ----------------------------------------------------------
    # PASS 2 — Weekly SCD2: update location for 1-2 customers
    # Only runs on Mondays
    # Only customers older than 3 months are eligible
    # ----------------------------------------------------------
    new_rows = []

    if generation_date.weekday() == 0:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT customer_id, first_name, last_name, date_of_birth,
                    email, mobile_no, gender, signup_timestamp, country
                FROM dw.dim_customer
                WHERE is_current = true
                AND effective_start <= %(gen_date)s::date - INTERVAL '3 months'
            """, {"gen_date": generation_date.isoformat()})
            eligible_customers = cur.fetchall()

        if eligible_customers:
            num_to_update = random.randint(1, 3)
            customers_to_update = random.sample(
                eligible_customers,
                min(num_to_update, len(eligible_customers))
            )

            change_dt = random_datetime_between(
                gen_dt,
                gen_dt + timedelta(hours=23)
            )

            for row in customers_to_update:
                (customer_id, first_name, last_name, date_of_birth,
                 email, mobile_no, gender, signup_timestamp, old_country) = row

                # Pick a new country different from original
                available_countries = [c for c in COUNTRY_WEIGHTS.keys() if c != old_country]
                available_weights   = [COUNTRY_WEIGHTS[c] for c in available_countries]
                new_country = random.choices(available_countries, weights=available_weights, k=1)[0]

                fake = faker_instances[new_country]

                try:
                    new_city = fake.city()
                except AttributeError:
                    new_city = None

                try:
                    new_state = fake.state()
                except AttributeError:
                    new_state = None

                new_rows.append({
                    "customer_id":      customer_id,
                    "first_name":       first_name,
                    "last_name":        last_name,
                    "date_of_birth":    date_of_birth,
                    "email":            email,
                    "mobile_no":        mobile_no,
                    "city":             new_city,
                    "state":            new_state,
                    "country":          new_country,
                    "signup_timestamp": signup_timestamp,
                    "effective_start":  change_dt,
                    "gender":           gender,
                    "event_type":       "location_updated",
                    "ingested_at":      ingested_at,
                })

            print(f"  Pass 2 complete — {len(new_rows)} customers location updated via SCD2.")
        else:
            print("  Pass 2 — no eligible customers for SCD2 yet (none older than 3 months).")

    df = pd.concat([pd.DataFrame(rows), pd.DataFrame(new_rows)], ignore_index=True)

    print(f"  Done. {len(df)} total customer rows generated.")
    return df