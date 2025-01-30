
sql
{{ config(
    materialized='table'
) }}


SELECT
       EVENT_UNIQUE_ID,
       AUTOMOBILE,
       MOTORCYCLE,
       PASSENGER,
       BICYCLE,
       PEDESTRIAN
FROM `TTC-GCP.ttcdataset.collision_details` ;

