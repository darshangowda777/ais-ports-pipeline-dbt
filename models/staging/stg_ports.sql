with source as (
    select * from {{ source('raw', 'world_port_index') }}
),

cleaned as (
    select
        port_id,
        port_name,
        country_code,
        region,
        water_body,
        latitude,
        longitude,
        harbor_size,
        harbor_type,
        shelter_quality,
        channel_depth_m,
        cargo_pier_depth_m
    from source
    where port_id is not null
      and latitude is not null
)

select * from cleaned