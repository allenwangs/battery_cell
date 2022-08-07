{{ dbt_utils.union_relations(
    relations=[ref('stg_battery_archive_time_series'), ref('stg_nature_paper_time_series')]
) }}