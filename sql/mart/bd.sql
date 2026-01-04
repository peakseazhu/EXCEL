-- TODO: Replace placeholder logic with real business metrics.
CREATE OR REPLACE TABLE mart.bd AS
SELECT *
FROM raw.chart_k41395f63a5134401908ebb5
WHERE run_date = DATE '{{ run_date }}';