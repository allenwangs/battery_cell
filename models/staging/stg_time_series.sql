with source as (

    select * from {{ source('battery_archive', 'time_series') }}

),

cast_datatype as (

    select
        file_name,
        file_row_number,
        date_time::timestamp,
        test_time::float,
        cycle_index::int,
        cell_current::float,
        voltage::float,
        charge_capacity::float,
        discharge_capacity::float,
        charge_energy::float,
        discharge_energy::float,
        environment_temperature::float,
        cell_temperature::float

    from source

)

select * from cast_datatype