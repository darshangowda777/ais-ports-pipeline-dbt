with vessels as (
    select * from {{ ref('stg_vessel_pings') }}
),

ports as (
    select * from {{ ref('stg_ports') }}
),

-- Keep only the latest ping per vessel
latest_vessels as (
    select *
    from vessels
    qualify row_number() over (
        partition by mmsi
        order by ais_timestamp desc
    ) = 1
),

joined as (
    select
        v.mmsi,
        v.vessel_name,
        v.vessel_type,
        v.latitude,
        v.longitude,
        v.speed_knots,
        v.heading_deg,
        v.nav_status,
        v.nav_status_code,
        v.draught_m,
        v.destination,
        v.ais_timestamp,
        v.distance_nmi,
        v.is_congestion,

        p.port_id,
        p.port_name,
        p.country_code,
        p.region,
        p.water_body,
        p.harbor_size,
        p.harbor_type,
        p.shelter_quality,
        p.channel_depth_m,
        p.cargo_pier_depth_m,

        case
            when v.draught_m > 0 and p.channel_depth_m > 0
            then round(v.draught_m / p.channel_depth_m * 100, 1)
            else null
        end as draught_utilisation_pct,

        case
            when v.nav_status = 'At Anchor' then 'Waiting'
            when v.nav_status = 'Moored' then 'Berthed'
            when v.nav_status = 'Underway Using Engine' then 'Underway'
            when v.nav_status = 'Engaged In Fishing' then 'Fishing'
            else 'Other'
        end as vessel_activity,

        case
            when v.speed_knots = 0 then 'Stationary'
            when v.speed_knots < 5 then 'Slow'
            when v.speed_knots < 12 then 'Normal'
            else 'Fast'
        end as speed_category

    from latest_vessels v
    left join ports p on v.port_id = p.port_id
)

select * from joined