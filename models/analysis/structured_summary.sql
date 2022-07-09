{{
    config(
        materialized='table'
    )
}}

with fct_structure as (
    select * from {{ ref('fct_structured') }}
),

groupby_cycles as (
    select
        file_name,
        cycle_index::int as cycle_index,
        max(discharge_capacity) as discharge_capacity,
        max(charge_capacity) as charge_capacity,
        max(charge_energy) as charge_energy,
        max(discharge_energy) as discharge_energy,
        null as dc_internal_resistance,

        max(cell_temperature) as temperature_maximum,
        avg(cell_temperature) as temperature_average,
        min(cell_temperature) as temperature_minimum,
        max(date_time) as date_time_iso,
        max(discharge_energy) / nullif(max(charge_energy), 0) as energy_efficiency -- need handle devide by zero

    from fct_structure
    group by 1, 2
),

add_throughput as (
    select
        groupby_cycles.*,
        sum(charge_capacity) over (order by cycle_index asc rows between unbounded preceding and current row) as charge_throughput,
        sum(charge_energy) over (order by cycle_index asc rows between unbounded preceding and current row) as energy_throughput
    from groupby_cycles       
)

select * from add_throughput