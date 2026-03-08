-- ============================================================
-- Reset Script — drops and recreates the entire dw schema
-- WARNING: This deletes ALL data. Only run during development.
-- ============================================================

DROP SCHEMA IF EXISTS dw CASCADE;

drop schema if exists audit cascade;