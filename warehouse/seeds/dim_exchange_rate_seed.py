"""
Multi-Currency to INR Historical Exchange Rate Fetcher & Loader
Uses Yahoo Finance via yfinance (free, no API key required)

Fetches daily rates for all currencies vs INR, forward-fills missing dates,
and loads everything into dw.dim_exchange_rate in PostgreSQL.

Currencies covered (17 total):
    Base        : INR (rate = 1.0 for all dates)
    Americas    : USD, CAD, BRL, MXN
    Europe      : EUR, GBP, CHF, SEK
    Middle East : AED, SAR
    Asia-Pacific: JPY, CNY, SGD, AUD, KRW, HKD

Direct tickers (XXXINR=X available on Yahoo Finance):
    USD, CAD, EUR, GBP, CHF, SEK, AED, JPY, AUD, SGD, KRW, HKD

Cross-rate via USD (no direct XXXINR=X ticker available):
    MXN, BRL, SAR, CNY → rate_to_inr = USD_to_INR / USD_per_XXX

Install dependencies:
    pip install yfinance pandas psycopg2-binary python-dotenv
"""

import os
from dotenv import load_dotenv
from pathlib import Path

import yfinance as yf
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime, timedelta

load_dotenv()

DB_CONFIG = {
    "host":     os.getenv("DB_HOST"),
    "port":     int(os.getenv("DB_PORT", 5432)),
    "dbname":   os.getenv("DB_NAME"),
    "user":     os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD")
}

START_DATE  = "2020-01-01"
END_DATE = (datetime.today() - timedelta(days=1)).strftime("%Y-%m-%d")
PROJECT_ROOT = Path(__file__).resolve().parents[2]
OUTPUT_FILE = PROJECT_ROOT / "warehouse" / "seeds" / "all_currencies_to_inr.csv"

# Yahoo Finance direct tickers: XXXINR=X → how many INR per 1 XXX
DIRECT_CURRENCIES = {
    # Americas
    "USD": "INR=X",        # US Dollar        — Special case on Yahoo Finance
    "CAD": "CADINR=X",    # Canadian Dollar   — Large e-commerce market

    # Europe
    "EUR": "EURINR=X",    # Euro              — Covers 20 EU countries
    "GBP": "GBPINR=X",    # British Pound     — UK, top 5 global e-commerce
    "CHF": "CHFINR=X",    # Swiss Franc       — High purchasing power
    "SEK": "SEKINR=X",    # Swedish Krona     — Sweden, strong online retail culture

    # Middle East
    "AED": "AEDINR=X",    # UAE Dirham        — Dubai, large Indian diaspora market

    # Asia-Pacific
    "JPY": "JPYINR=X",    # Japanese Yen      — 3rd largest e-commerce globally
    "AUD": "AUDINR=X",    # Australian Dollar — Top 10 global e-commerce
    "SGD": "SGDINR=X",    # Singapore Dollar  — SE Asia e-commerce hub
    "KRW": "KRWINR=X",    # South Korean Won  — 5th largest e-commerce globally
    "HKD": "HKDINR=X",    # Hong Kong Dollar  — Major trade & payments hub
}

# No direct XXXINR=X ticker on Yahoo Finance for these.
# We fetch USD/XXX and calculate: rate_to_inr = USD_to_INR / USD_per_XXX
# e.g. 1 USD = 84 INR, 1 USD = 7.2 CNY → 1 CNY = 84 / 7.2 = 11.67 INR
CROSS_RATE_CURRENCIES = {
    "MXN": "MXN=X",   # Mexican Peso    — Fast-growing e-commerce market
    "BRL": "BRL=X",   # Brazilian Real  — Biggest e-commerce in Latin America
    "SAR": "SAR=X",   # Saudi Riyal     — Largest e-commerce in Middle East
    "CNY": "CNY=X",   # Chinese Yuan    — Largest e-commerce market globally
}


def fetch_usd_inr():
    """Fetch USD/INR once and reuse for all cross-rate calculations."""
    print("  Fetching USD/INR (INR=X) for cross-rate base...")
    df = yf.Ticker("INR=X").history(start=START_DATE, end=END_DATE, interval="1d")[["Close"]]
    df.index = pd.to_datetime(df.index).tz_localize(None).normalize()
    df.columns = ["usd_to_inr"]
    return df


def fetch_direct_currency(currency_code, ticker_symbol):
    """Fetch a currency that has a direct XXXINR=X ticker on Yahoo Finance."""
    print(f"  Fetching {currency_code}/INR ({ticker_symbol})...")
    try:
        df = yf.Ticker(ticker_symbol).history(start=START_DATE, end=END_DATE, interval="1d")[["Close"]]

        if df.empty:
            print(f"  WARNING: No data found for {currency_code}. Skipping.")
            return None

        df.index = pd.to_datetime(df.index).tz_localize(None).normalize()
        df.index.name = "date"
        df.rename(columns={"Close": "rate_to_inr"}, inplace=True)
        df["currency_code"] = currency_code
        return df

    except Exception as e:
        print(f"  ERROR fetching {currency_code}: {e}")
        return None


def fetch_cross_rate_currency(currency_code, usd_ticker, usd_inr_df):
    """
    For currencies without a direct XXX/INR ticker, calculate via USD cross-rate.
    rate_to_inr = USD_to_INR / USD_per_XXX
    """
    print(f"  Fetching {currency_code}/INR via cross-rate ({usd_ticker})...")
    try:
        usd_xxx = yf.Ticker(usd_ticker).history(start=START_DATE, end=END_DATE, interval="1d")[["Close"]]
        usd_xxx.index = pd.to_datetime(usd_xxx.index).tz_localize(None).normalize()
        usd_xxx.columns = ["usd_per_xxx"]

        combined = usd_inr_df.join(usd_xxx, how="inner")

        if combined.empty:
            print(f"  WARNING: No cross-rate data found for {currency_code}. Skipping.")
            return None

        combined["rate_to_inr"] = combined["usd_to_inr"] / combined["usd_per_xxx"]
        combined["currency_code"] = currency_code
        combined.index.name = "date"
        return combined[["rate_to_inr", "currency_code"]].copy()

    except Exception as e:
        print(f"  ERROR fetching {currency_code} via cross-rate: {e}")
        return None


def generate_inr(start_date, end_date):
    """
    Generate INR rows with rate_to_inr = 1.0 for every calendar day
    from START_DATE to END_DATE. INR is always 1 INR = 1 INR.
    """
    print("  Generating INR rows (rate = 1.0 for all dates)...")
    full_range = pd.date_range(start=start_date, end=end_date, freq="D")
    df = pd.DataFrame({
        "rate_to_inr": 1.0,
        "currency_code": "INR"
    }, index=full_range)
    df.index.name = "date"
    print(f"  Generated {len(df)} INR rows from {start_date} to {end_date}.")
    return df


def forward_fill(df, currency_code):
    """Fill gaps (weekends, holidays) with the last known rate."""
    full_range = pd.date_range(start=df.index.min(), end=END_DATE, freq="D")  # extend to END_DATE
    df = df.reindex(full_range)
    df.index.name = "date"
    df["currency_code"] = df["currency_code"].ffill()
    filled = df["rate_to_inr"].isna().sum()
    df["rate_to_inr"] = df["rate_to_inr"].ffill().round(4)
    print(f"  Forward-filled {filled} missing dates for {currency_code}.")
    return df


def fetch_all_currencies():
    """Fetch and combine all currencies into a single DataFrame."""
    all_dfs = []

    # Step 1: INR base currency — rate is always 1.0
    print("\n--- Base Currency ---")
    all_dfs.append(generate_inr(START_DATE, END_DATE))

    # Step 2: Fetch all direct ticker currencies
    print("\n--- Direct Ticker Currencies ---")
    for currency_code, ticker_symbol in DIRECT_CURRENCIES.items():
        df = fetch_direct_currency(currency_code, ticker_symbol)
        if df is not None:
            df = forward_fill(df, currency_code)
            all_dfs.append(df)

    # Step 3: Fetch USD/INR once, reuse for all cross-rate currencies
    print("\n--- Cross-Rate Currencies (via USD) ---")
    usd_inr_df = fetch_usd_inr()
    for currency_code, usd_ticker in CROSS_RATE_CURRENCIES.items():
        df = fetch_cross_rate_currency(currency_code, usd_ticker, usd_inr_df)
        if df is not None:
            df = forward_fill(df, currency_code)
            all_dfs.append(df)

    combined = pd.concat(all_dfs)
    combined = combined.reset_index()
    combined["date_sk"] = combined["date"].dt.strftime("%Y%m%d").astype(int)
    combined = combined[["date_sk", "currency_code", "rate_to_inr"]]
    combined = combined.sort_values(["currency_code", "date_sk"]).reset_index(drop=True)
    return combined


def save_csv(df):
    df.to_csv(OUTPUT_FILE, index=False)
    print(f"\nCSV saved to: {OUTPUT_FILE}  ({len(df)} rows)")


def load_to_postgres(df):
    """Insert all rows into dw.dim_exchange_rate, skipping existing rows."""
    rows = list(df.itertuples(index=False, name=None))

    insert_sql = """
        INSERT INTO dw.dim_exchange_rate (date_sk, currency_code, rate_to_inr)
        VALUES %s
        ON CONFLICT (date_sk, currency_code) DO NOTHING
    """

    print(f"\nConnecting to PostgreSQL...")
    with psycopg2.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cur:
            execute_values(cur, insert_sql, rows, page_size=1000)
        conn.commit()
    print(f"Inserted/skipped {len(rows)} rows into dw.dim_exchange_rate.")


def main():
    print(f"Fetching exchange rates from {START_DATE} to {END_DATE}...")
    df = fetch_all_currencies()

    print(f"\n--- Summary ---")
    print(f"Total rows: {len(df)}")
    print(f"\nRows per currency:")
    print(df.groupby("currency_code").size().to_string())

    save_csv(df)
    load_to_postgres(df)

    print("\nAll done!")


if __name__ == "__main__":
    main()