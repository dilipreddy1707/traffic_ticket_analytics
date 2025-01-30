sql
{{ config(
    materialized='table'
) }}

SELECT
    EVENT_UNIQUE_ID,
    OCC_DATE,
    OCC_MONTH,
    OCC_DOW,
    OCC_YEAR,
    OCC_HOUR,
    DIVISION
FROM `TTC-GCP.ttcdataset.events`;