-- TODO: Replace placeholder logic with real business metrics.
CREATE OR REPLACE TABLE mart.province AS
SELECT *
FROM raw.chart_r29b8748abc9a44e88365b63
WHERE run_date = DATE '{{ run_date }}';