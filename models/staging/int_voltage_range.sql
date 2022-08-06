{{
    config(
        materialized='table'
    )
}}
-- generate row of data starting from 2.000, 2.001, 2.002 to 3.500
select 
    2.0 + seq4()/1000 as voltage
from table(generator(rowcount => 1500)) 