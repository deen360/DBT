{{ config(materialized='view') }}


with tripdata as 
(
  select *,
    row_number() over(partition by dispatching_base_num, pickup_datetime) as rn
  from {{ source('staging','fhv_tripdata_external_table') }}
)

select 

    -- identifiers
    {{ dbt_utils.surrogate_key(['dispatching_base_num', 'pickup_datetime']) }} as tripid,
    cast(dispatching_base_num as string) as dispatching_base_num,
    
    -- timestamps
    cast(pickup_datetime as timestamp) as pickup_datetime,
    cast(dropoff_datetime as timestamp) as dropoff_datetime,
    cast(PULocationID AS integer) AS PULocationID,
    cast(DOLocationID AS integer) AS DOLocationID,
    
    -- trip info
        cast(Affiliated_base_number as string) as Affiliated_base_number                            
    
from tripdata

where rn = 1
-- dbt build --m <model.sql> --var 'is_test_run: false'
{% if var('is_test_run', default=true) %}


{% endif %}