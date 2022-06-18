{{
    config(
        materialized='table'
    )
}}

with stg_time_sereis as (
    select * from {{ ref('stg_time_series') }}
)

select * from stg_time_sereis
