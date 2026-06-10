with source as (
    select * from {{ source('raw', 'vessel_pings') }}
),

cleaned as (
    select
        mmsi,
        vessel_name,
        vessel_type,
        latitude,
        longitude,
        speed_knots,
        heading_deg,
        nav_status,
        nav_status_code,
        draught_m,
        destination,

        -- Clean timestamp: strip nanoseconds and +0000 UTC suffix
        TIMESTAMP(
            REGEXP_REPLACE(
                CAST(ais_timestamp AS STRING),
                r'\.\d+ \+0000 UTC$', ''
            )
        ) AS ais_timestamp,

        port_id,
        distance_nmi,
        is_congestion

    from source
    where latitude is not null
      and longitude is not null
      and mmsi is not null
)

select * from cleaned