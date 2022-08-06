{{
    config(
        materialized='table'
    )
}}

-- import models
with fct_time_series as (
    select * from {{ ref('fct_time_series') }}
),
-- rows of voltage (2.000, 2.001, 2.002, ..., 3.499, 3.500)
int_voltage_range as (
    select * from {{ ref('int_voltage_range') }}
),

-- start the model

-- get discharge data only
discharge_time_series as (
    select
        file_name,
        cycle_index,
        voltage,
        discharge_capacity

    from fct_time_series
    where step_type = 'discharge'
    --and cycle_index = 100 -- sample for testing purpose
    --and file_name = 'cycler_data/2018-04-12_batch8_CH38.csv' -- sample for testing purpose
),

get_each_cycle_voltage_range as (
    select
        file_name,
        cycle_index,
        min(voltage) as voltage_min,
        max(voltage) as voltage_max

    from discharge_time_series
    group by file_name, cycle_index
),

each_cycle_voltage_spine as (
    select
        get_each_cycle_voltage_range.file_name,
        get_each_cycle_voltage_range.cycle_index,
        int_voltage_range.voltage

    from get_each_cycle_voltage_range
    cross join int_voltage_range

    where int_voltage_range.voltage > get_each_cycle_voltage_range.voltage_min
    and int_voltage_range.voltage < get_each_cycle_voltage_range.voltage_max
),

time_series_union_voltage_spine as (
    select
        file_name,
        cycle_index,
        voltage,
        discharge_capacity,
        'real' as voltage_source
    from discharge_time_series
    
    union all
    
    select
        file_name,
        cycle_index,
        voltage,
        null as discharge_capacity,
        'interpolate' as voltage_source
    from each_cycle_voltage_spine
),

add_interpolate_feature as (
    select
        file_name,
        cycle_index,
        voltage,
        discharge_capacity,
        voltage_source,
        first_value(discharge_capacity) ignore nulls over (partition by file_name, cycle_index order by voltage asc rows BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) as discharge_capacity_next,
        first_value(discharge_capacity) ignore nulls over (partition by file_name, cycle_index order by voltage desc rows BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) as discharge_capacity_previous,
        first_value(case when discharge_capacity is not null then voltage else null end) ignore nulls over (partition by file_name, cycle_index order by voltage asc rows BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) as voltage_next,
        first_value(case when discharge_capacity is not null then voltage else null end) ignore nulls over (partition by file_name, cycle_index order by voltage desc rows BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) as voltage_previous

    from time_series_union_voltage_spine
),

interpolate_discharge_capacity as (
    select
        file_name,
        cycle_index,
        voltage,
        voltage_source,
        case 
            when voltage_next = voltage_previous then discharge_capacity
            else (discharge_capacity_previous * (voltage_next - voltage) + discharge_capacity_next * (voltage - voltage_previous)) / (voltage_next - voltage_previous) 
        end as discharge_capacity_interpolate
    from add_interpolate_feature
),

remove_real_data as (
    select
        file_name,
        cycle_index,
        voltage,
        discharge_capacity_interpolate as discharge_capacity
    from interpolate_discharge_capacity
    where voltage_source = 'interpolate'
),

final as (
    select * 
    from remove_real_data
)

select * from final
