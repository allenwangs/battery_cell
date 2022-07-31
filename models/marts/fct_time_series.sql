{{
    config(
        materialized='table'
    )
}}

with stg_nature_paper_time_series as (
    select * from {{ ref('stg_nature_paper_time_series') }}
),

add_step_type as (
    select
        stg_nature_paper_time_series.*,
        case
            when abs(cell_current) < pow(10, -6) then 'idle'
            when cell_current > 0 then 'charge'
            when cell_current < 0 then 'discharge'
            else null
        end as step_type
    from stg_nature_paper_time_series
),

final as (
    select * from add_step_type
)

select * from final