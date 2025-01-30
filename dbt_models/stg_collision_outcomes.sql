sql
{{ config(
    materialized='table'
) }}

SELECT
    EVENT_UNIQUE_ID,
    FATALITIES,
    INJURY_COLLISIONS,
    FTR_COLLISIONS,
    PD_COLLISIONS
FROM  `TTC-GCP.ttcdataset.collision_outcomes` ;
