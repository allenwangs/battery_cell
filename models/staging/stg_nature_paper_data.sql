with source as (
    select * from {{ source('nature_paper_data', 'time_series') }}
),

rename_column as (
    select
        file_name,
        data_point,
        test_time,
        datetime as date_time,
        step_time,
        step_index,
        cycle_index,
        cell_current,
        voltage,
        charge_capacity,
        discharge_capacity,
        charge_energy,
        discharge_energy,
        "dv/dt",
        internal_resistance,
        temperature,
        aux_voltage
    from source
),

cast_data_type as (
    select
        file_name,
        data_point,
        test_time::datetime as test_time,
        date_time::datetime as date_time,
        step_time,
        step_index,
        cycle_index,
        cell_current,
        voltage,
        charge_capacity,
        discharge_capacity,
        charge_energy,
        discharge_energy,
        "dv/dt",
        internal_resistance,
        temperature,
        aux_voltage
    from rename_column
)

select * from cast_data_type