{{ config(materialized='table') }}

with trips_data as (
    select * from {{ ref('fact_trips') }}
)

    select 
    --revenue grouping
    pickup_zone as revenue_zone,
    date_trunc(pickup_time, month) as revenue_month,
    service_type,

    --revenue calculation
    sum(fare_amount) as revenue_monthly_fare,
    sum(extra) as revenue_monthly_extra,
    sum(mta_tax) as revenue_monthly__mta_tax,
    sum(tip_amount) as revenue_monthly_tip_amount,
    sum(tolls_amount) as revenue_monthyl_tolls_amount,
    sum(improvement_surcharge) as revenue_improvement_surcharge,
    sum(total_amount) as revenue_monthly_total_amount,
    sum(congestion_surcharge) as revenue_monthly_congestion_surcharge,

    --additional calculation
    count(trip_id) as total_monthly_trips,
    avg(passenger_count) as avg_montly_passenger_count,
    avg(trip_distance) as avg_montly_trip_distance

    from trips_data
    group by 1,2,3

