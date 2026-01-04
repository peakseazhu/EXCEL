-- TODO: Replace placeholder logic with real business metrics.
CREATE OR REPLACE TABLE mart.province_sandbox AS
SELECT *
FROM raw.chart_t5ff658e34e0740c38e192e0
WHERE run_date = DATE '{{ run_date }}';