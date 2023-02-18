{{ config(materialized="view") }}

select
    cast(int64_field_0 as integer) as id,
    cast(pickup_datetime as timestamp) as pickup_datetime,
    cast(dropOff_datetime as timestamp) as dropoff_datetime,
    cast(PULocationID as integer) as pickup_locationid,
    cast(DOLocationID as integer) as dropoff_locationid,
    
    SR_Flag,
    dispatching_base_num,
    Affiliated_base_number,

from {{ source("staging_fhv", "fhv_2019") }}
