-- TODO: Replace placeholder logic with real business metrics.
CREATE OR REPLACE TABLE mart.region AS
SELECT *
FROM raw.chart_dd60461b434f9465fb3c6cff
WHERE run_date = DATE '{{ run_date }}';