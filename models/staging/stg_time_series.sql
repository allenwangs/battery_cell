with source as (

    select * from {{ source('battery_archive', 'time_series') }}

),

cast_datatype as (

    select
        file_name,
        file_row_number,
        date_time::timestamp as date_time,
        test_time::float as test_time,
        cycle_index::int as cycle_index,
        cell_current::float as cell_current,
        voltage::float as voltage,
        charge_capacity::float as charge_capacity,
        discharge_capacity::float as discharge_capacity,
        charge_energy::float as charge_energy,
        discharge_energy::float as discharge_energy,
        environment_temperature::float as environment_temperature,
        cell_temperature::float as cell_temperature

    from source

)

select * from cast_datatype