# etl/extract/generator/gen_customers.py
# ============================================================
# Generates and inserts dim_customer rows.
# Pass 1: 10000 active customers
# Pass 2 — 5% get location update via SCD2
# Returns all customer versions for SCD2 lookup.
# ============================================================

import random
from datetime import date
from faker import Faker

from .config import (
    COUNTRY_LOCALE, COUNTRY_WEIGHTS,
    MAIL_DOMAINS, COUNTRY_MOBILE_CODE,
    NUM_CUSTOMERS, SCD2_CUST_RATE
)
from .db import bulk_insert, fetch_all, execute_many, random_date, random_datetime


def generate_customers(conn):
    """
    Generates dim_customer rows in two passes.
    Pass 1 — 10000 active customers
    Pass 2 — 5% get discontinued via SCD2
    Returns all_customers for all versions.
    """

    print("\n[dim_customer] Generating customers...")

    # Build Faker instances once per locale
    faker_instances = {
        country: Faker(locale)
        for country, locale in COUNTRY_LOCALE.items()
    }

    # ------------------------------------------------------
    # PASS 1: Generate 10000 active customers
    # ------------------------------------------------------
    pass1_rows = []

    for i in range(1, NUM_CUSTOMERS + 1):
        customer_id = f"CUST-{i:05d}"

        # Pick a random gender
        gender = random.choice(['male', 'female', 'other'])

        # Pick first name, last name
        country = random.choices(
            list(COUNTRY_WEIGHTS.keys()),
            weights=list(COUNTRY_WEIGHTS.values()),
            k=1
        )[0]

        fake = faker_instances[country]

        # fake first_name, last_name, date_of_birth, email, mobile_no
        try:
            if gender == 'male':
                first_name = fake.first_name_male()
            elif gender == 'female':
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
        email = f"{email_prefix}@{random.choice(MAIL_DOMAINS)}"

        mobile_no = f"{COUNTRY_MOBILE_CODE[country]}{fake.msisdn()[3:10]}"

        try:
            city = fake.city()
        except AttributeError:
            city = None

        try:
            state = fake.state()
        except AttributeError:
            state = None

        signup_timestamp = random_datetime(date(2020, 1, 1), date(2023, 12, 31))
        effective_start = signup_timestamp


        pass1_rows.append((
            customer_id,
            first_name,
            last_name,
            date_of_birth,
            email,
            mobile_no,
            city,
            state,
            country,
            signup_timestamp,
            effective_start,
            None,    # effective_end
            True,    # is_current
            gender,
        ))

    bulk_insert(
        conn,
        table="dw.dim_customer",
        columns=[
            "customer_id", "first_name", "last_name", "date_of_birth",
            "email", "mobile_no", "city", "state",
            "country", "signup_timestamp", "effective_start", "effective_end",
            "is_current", "gender"
        ],
        rows=pass1_rows
    )

    print(f"  Pass 1 complete — {NUM_CUSTOMERS} active customers inserted.")

    # ------------------------------------------------------
    # PASS 2: SCD2 — update location for 5% of customers
    # ------------------------------------------------------
    all_customers = fetch_all(
        conn,
        """
        SELECT customer_sk, customer_id, first_name, last_name,
            date_of_birth, email, mobile_no, country, gender, signup_timestamp
        FROM dw.dim_customer
        WHERE is_current = TRUE
        """
    )

    num_to_discontinue = int(len(all_customers) * SCD2_CUST_RATE)
    customers_to_discontinue = random.sample(all_customers, num_to_discontinue)

    update_rows = []
    new_rows    = []

    for row in customers_to_discontinue:
        (customer_sk, customer_id, first_name, last_name, date_of_birth,
        email, mobile_no, original_country, gender, signup_timestamp) = row

        # SCD2 change date — after the customer's effective_start
        change_timestamp = random_datetime(date(2024, 1, 1), date(2025, 12, 31))

        # Close the current row
        update_rows.append((change_timestamp, customer_sk))

        # Pick a new country different from the original
        available_countries = [c for c in COUNTRY_WEIGHTS.keys() if c != original_country]
        available_weights   = [COUNTRY_WEIGHTS[c] for c in available_countries]

        new_country = random.choices(
            available_countries,
            weights=available_weights,
            k=1
        )[0]

        # Get the faker instance for the new country
        fake = faker_instances[new_country]

        # Regenerate location
        try:
            new_city = fake.city()
        except AttributeError:
            new_city = None

        try:
            new_state = fake.state()
        except AttributeError:
            new_state = None

        # Open a new row for the customer with updated location
        new_rows.append((
            customer_id,
            first_name,
            last_name,
            date_of_birth,
            email,
            mobile_no,
            new_city,
            new_state,
            new_country,
            signup_timestamp,
            change_timestamp,
            None,    # effective_end
            True,    # is_current
            gender,
        ))

    # Run updates first, then inserts
    execute_many(
        conn,
        """
        UPDATE dw.dim_customer
        SET effective_end = %s,
            is_current = FALSE
        WHERE customer_sk = %s
        AND is_current = TRUE
        """,
        update_rows
    )

    bulk_insert(
        conn,
        table="dw.dim_customer",
        columns=[
           "customer_id", "first_name", "last_name", "date_of_birth",
            "email", "mobile_no", "city", "state",
            "country", "signup_timestamp", "effective_start", "effective_end",
            "is_current", "gender"
        ],
        rows=new_rows
    )

    print(f"  Pass 2 complete — {num_to_discontinue} customers discontinued via SCD2.")

    # ------------------------------------------------------
    # Return all customer versions for SCD2 lookup in gen_orders.py
    # ------------------------------------------------------
    all_customers = fetch_all(
        conn,
        """
        SELECT customer_sk, customer_id, effective_start, effective_end, country
        FROM dw.dim_customer
        """
    )

    return all_customers