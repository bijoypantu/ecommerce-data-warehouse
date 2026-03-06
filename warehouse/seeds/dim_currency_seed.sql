-- warehouse/seeds/dim_currency_seed.sql
-- ============================================================
-- Seed data for dim_currency
-- 16 currencies used in the e-commerce platform
-- ============================================================

INSERT INTO dw.dim_currency (currency_code, currency_name) VALUES
    ('INR', 'Indian Rupee'),
    ('USD', 'US Dollar'),
    ('GBP', 'British Pound'),
    ('EUR', 'Euro'),
    ('AED', 'UAE Dirham'),
    ('SAR', 'Saudi Riyal'),
    ('JPY', 'Japanese Yen'),
    ('CNY', 'Chinese Yuan'),
    ('SGD', 'Singapore Dollar'),
    ('AUD', 'Australian Dollar'),
    ('CAD', 'Canadian Dollar'),
    ('BRL', 'Brazilian Real'),
    ('MXN', 'Mexican Peso'),
    ('HKD', 'Hong Kong Dollar'),
    ('KRW', 'South Korean Won'),
    ('CHF', 'Swiss Franc'),
    ('SEK', 'Swedish Krona')
ON CONFLICT (currency_code) DO NOTHING;