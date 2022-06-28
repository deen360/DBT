{{ config(materialized='table') }}

select 
    tripid, 
    dispatching_base_num, 
    pickup_datetime, 
    dropoff_datetime,
    Affiliated_base_number 
from {{ ref('stg_fhv_tripdata') }}

