{{ config(materialized='view') }}

with tripdata as 
(
    select *,
        row_number() over(partition by cast(vendorid as integer), tpep_pickup_datetime) as rn
    from {{ source('staging', 'rides') }}
    where vendorid is not null
)

select 
    {{ dbt_utils.surrogate_key(['vendorid', 'tpep_pickup_datetime']) }} as trip_id,
    cast(vendorid as integer) as vendorid,
    cast(ratecodeid as integer) as ratecodeid,
    cast(pulocationid as integer) as  pickup_locationid,
    cast(dolocationid as integer) as dropoff_locationid,

    cast(tpep_pickup_datetime as timestamp) as pickup_time,
    cast(tpep_dropoff_datetime as timestamp) as dropoff_time,
    cast(passenger_count as integer) as passenger_count,

    cast(trip_distance as integer) as trip_distance,
    store_and_fwd_flag,
    cast(payment_type as integer) as payment_type,
    {{ get_payment_type_description('payment_type') }} as payment_type_description,

    cast(fare_amount as numeric) as fare_amount,
    cast(extra as numeric) as extra,
    cast(mta_tax as numeric) as mta_tax,
    cast(tip_amount as numeric) as tip_amount,
    cast(tolls_amount as numeric) as tolls_amount,
    cast(improvement_surcharge as numeric) as improvement_surcharge,
    cast(total_amount as numeric) as total_amount,
    cast(congestion_surcharge as numeric) as congestion_surcharge,


from tripdata
where rn = 1

--dbt build --m <model.sql> --var 'is_test_run: false'
{% if var('is_test_run', default=true) %}

    limit 100

{% endif %}