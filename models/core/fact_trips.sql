{{ config(materialized='table') }}

with green_data as (
    select *, 
    from {{ ref('stg_fhv_tripdata_external_table') }}
), 

trips_unioned as (
    select * from green_data
), 

dim_zones as (
    select * from {{ ref('dim_zones') }}
    where borough != 'Unknown'
)
select 
    trips_unioned.tripid,
    trips_unioned.dispatching_base_num, 
    trips_unioned.pickup_locationid, 
    trips_unioned.dropoff_locationid,  
    trips_unioned.pickup_datetime, 
    trips_unioned.dropoff_datetime, 
    trips_unioned.Affiliated_base_number, 

from trips_unioned
