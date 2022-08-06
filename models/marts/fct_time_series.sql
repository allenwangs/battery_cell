{{
    config(
        materialized='table'
    )
}}

with stg_time_series_union as (
    select * from {{ ref('stg_time_series_union') }}
),

add_step_type as (
    select
        stg_time_series_union.*,
        case
            when abs(cell_current) < pow(10, -6) then 'idle'
            when cell_current > 0 then 'charge'
            when cell_current < 0 then 'discharge'
            else null
        end as step_type
    from stg_time_series_union
),

final as (
    select * from add_step_type
)

select * from final
