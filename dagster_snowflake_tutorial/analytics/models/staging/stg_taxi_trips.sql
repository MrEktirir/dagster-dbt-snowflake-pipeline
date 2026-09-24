with source as (
    select * from {{ source('raw', 'taxi_trips') }}
),

renamed as (
    select
        VendorID                                        as vendor_id,
        tpep_pickup_datetime                            as pickup_at,
        tpep_dropoff_datetime                           as dropoff_at,
        passenger_count,
        trip_distance,
        PULocationID                                    as pickup_location_id,
        DOLocationID                                    as dropoff_location_id,
        payment_type,
        fare_amount,
        tip_amount,
        total_amount,
        datediff(
            'minute',
            tpep_pickup_datetime,
            tpep_dropoff_datetime
        )                                               as trip_duration_minutes
    from source
    where trip_distance > 0
      and total_amount > 0
      and tpep_pickup_datetime >= '2023-01-01'
      and tpep_pickup_datetime < '2023-02-01'
)

select * from renamed