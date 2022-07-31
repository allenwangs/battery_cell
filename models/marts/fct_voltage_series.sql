
-- import models
with fct_time_series as (
    select * from {{ ref('fct_time_series') }}
),

int_voltage_range as (
    select * from {{ ref('int_voltage_range') }}
),

discharge_time_series as (
    select
        file_name,
        cycle_index,
        voltage,
        discharge_capacity

    from fct_time_series
    where step_type = 'discharge'
),

distinct_file_name_cycle_index as (
    select 
        file_name,
        cycle_index
    from discharge_time_series
    qualify row_number() over (partition by file_name, cycle_index order by file_name, cycle_index) = 1
),

-- for each (file_name, cycle_index) pair, create voltage range from 2V to 3.5V
create_voltage_grid as (
    select
        file_name,
        cycle_index,
        voltage
    from distinct_file_name_cycle_index
    cross join int_voltage_range
    order by file_name, cycle_index, voltage
),

concat_voltage_grid as (
    select
        file_name,
        cycle_index,
        voltage,
        null as discharge_capacity
    from create_voltage_grid
    union all
    select
        file_name,
        cycle_index,
        voltage,
        discharge_capacity
    from discharge_time_series
),

-- interpolate discharge_capacity by voltage
interp_discharge_capacity as (
    select
        file_name,
        cycle_index,
        voltage,
        discharge_capacity,
        discharge_capacity_interp

)

final as (
    select * from concat_voltage_grid
)

select * from final limit 1000
