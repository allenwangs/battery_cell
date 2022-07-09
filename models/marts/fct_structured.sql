{{
    config(
        materialized='table'
    )
}}

with stg_time_series as (
    select * from {{ ref('stg_time_series') }}
),

add_step_type as (
    select
        file_name,
        file_row_number,
        cycle_index,
        test_time,
        date_time,
        voltage,
        cell_current,
        charge_capacity,
        discharge_capacity,
        charge_energy,
        discharge_energy,
        cell_temperature,
        null as internal_resistance,

        -- add step_type
        case
            when abs(cell_current) < pow(10, -6) then 'idle'
            when cell_current > 0 then 'charge'
            when cell_current < 0 then 'discharge'
            else null
        end as step_type
     
     from stg_time_series
),

final as (
    select * from add_step_type
)

select * from final


