{{
    config(
        materialized='table'
    )
}}

with stg_time_series as (
    select * from {{ ref('stg_time_series') }}
),

groupby_cycles as (
    select
        file_name,
        cycle_index::int as cycle_index,
        
        max(test_time) as test_time,
        
        max(cell_current) as max_cell_current,
        min(cell_current) as min_cell_current,
        
        max(voltage) as max_voltage,
        min(voltage) as min_voltage,
        
        max(charge_capacity) as charge_capacity,
        max(discharge_capacity) as discharge_capacity,
        max(charge_energy) as charge_energy,
        max(discharge_energy) as discharge_energy,

        max(environment_temperature) as max_environment_temperature,
        min(environment_temperature) as min_environment_temperature,

        max(cell_temperature) as max_cell_temperature,
        min(cell_temperature) as min_cell_temperature

    from stg_time_series
    group by 1, 2
)

select * from groupby_cycles