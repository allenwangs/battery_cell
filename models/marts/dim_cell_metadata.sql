{{
    config(
        materialized='table'
    )
}}

-- import models
with fct_time_series as (
    select * from {{ ref('fct_time_series') }}
),

discharge_capacity_over_cycle as (
    select
        _DBT_SOURCE_RELATION,
        file_name,
        cycle_index,
        max(discharge_capacity) as discharge_capacity
    from fct_time_series
    group by _DBT_SOURCE_RELATION, file_name, cycle_index
),

-- cycle life is define as 80% of nominal capacity (80% of 1.1Ah = 0.88Ah)
end_of_life as (
    select *
    from discharge_capacity_over_cycle
    where discharge_capacity < 0.89
    qualify row_number() over (partition by file_name order by cycle_index asc) = 1
),

final as (
    select * from end_of_life
)

select * from final