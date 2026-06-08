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
        ais_timestamp,
        port_id,
        distance_nmi,
        is_congestion
    from source
    where latitude is not null
      and longitude is not null
      and mmsi is not null
)

select * from cleaned