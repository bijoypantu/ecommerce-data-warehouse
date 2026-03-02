# etl/extract/generator/gen_customers.py
# ============================================================
# Generates and inserts dim_customer rows.
# Pass 1: 10000 active customers
# Pass 2 — 5% get location update via SCD2
# Returns all customer versions for SCD2 lookup.
# ============================================================

import random
import pandas as pd
from datetime import date, datetime, timezone
from faker import Faker
from dateutil.relativedelta import relativedelta

from .config import (
    COUNTRY_LOCALE, COUNTRY_WEIGHTS,
    MAIL_DOMAINS, COUNTRY_MOBILE_CODE,
    NUM_CUSTOMERS, SCD2_CUST_RATE
)
from .db import random_datetime, random_date, random_datetime_between


def generate_customers():
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

    rows = []
    # ----------------------------------------------------------
    # PASS 1: Generate 1000 active customers
    # ----------------------------------------------------------
    for i in range(1, NUM_CUSTOMERS + 1):
        customer_id = f"CUST-{i:05d}"
        # Pick a random gender
        gender = random.choice(['male', 'female', 'other'])

        # Pick country
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

        signup_timestamp = random_datetime(date(2020, 1, 1), date(2024, 12, 31))
        effective_start = signup_timestamp

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
            "effective_start":  effective_start,
            "gender":           gender,
            "event_type":      "created",
            "ingested_at":     datetime.now(timezone.utc).isoformat(),
        })

    print(f"  Pass 1 complete — {NUM_CUSTOMERS} active customers generated.")

    # ----------------------------------------------------------
    # PASS 2: SCD2 — update location for 5% of customers
    # ----------------------------------------------------------
    df = pd.DataFrame(rows)

    num_to_discontinue = int(len(df) * SCD2_CUST_RATE)
    customers_to_discontinue = df.sample(num_to_discontinue)

    new_rows = []
    for _, old_row in customers_to_discontinue.iterrows():
        # change_date is at least 6 months after signup
        earliest_change = old_row["signup_timestamp"] + relativedelta(months=6)
        latest_change   = datetime(2025, 12, 31, tzinfo=timezone.utc)

        # Only change if there's a valid window
        if earliest_change < latest_change:
            change_date = random_datetime_between(earliest_change, latest_change)
        else:
            continue  # skip this customer — not enough time has passed

        # Pick a new country different from the original
        available_countries = [c for c in COUNTRY_WEIGHTS.keys() if c != old_row["country"]]
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

        new_rows.append({
            "customer_id":      old_row["customer_id"],
            "first_name":       old_row["first_name"],
            "last_name":        old_row["last_name"],
            "date_of_birth":    old_row["date_of_birth"],
            "email":            old_row["email"],
            "mobile_no":        old_row["mobile_no"],
            "city":             new_city,
            "state":            new_state,
            "country":          new_country,
            "signup_timestamp": old_row["signup_timestamp"],
            "effective_start":  change_date,
            "gender":           old_row["gender"],
            "event_type":      "location_updated",
            "ingested_at":     datetime.now(timezone.utc).isoformat(),
        })

    df = pd.concat([df, pd.DataFrame(new_rows)], ignore_index=True)

    print(f"  Pass 2 complete — {len(new_rows)} customers location updated via SCD2.")
    print(f"  Done. {len(df)} total customer rows generated.")
    return df