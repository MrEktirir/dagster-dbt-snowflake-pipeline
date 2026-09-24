with trips as (
    select * from {{ ref('stg_taxi_trips') }}
),

metrics as (
    select
        date_trunc('hour', pickup_at)       as pickup_hour,
        pickup_location_id,
        count(*)                            as trip_count,
        avg(trip_distance)                  as avg_trip_distance,
        avg(trip_duration_minutes)          as avg_trip_duration_minutes,
        avg(total_amount)                   as avg_total_amount,
        sum(total_amount)                   as total_revenue,
        avg(tip_amount)                     as avg_tip_amount
    from trips
    group by 1, 2
)

select * from metrics