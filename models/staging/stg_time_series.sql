select *
from {{ source('battery_archive', 'time_series') }}