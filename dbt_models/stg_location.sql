sql
{{ config(
    materialized='table'
) }}


SELECT
  HOOD_158,
  NEIGHBOURHOOD_158,
  LONG_WGS84,
  LAT_WGS84
FROM  `TTC-GCP.ttcdataset.location`;


