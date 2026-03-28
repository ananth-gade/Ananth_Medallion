{% test check_gaps_in_eff_disc_dt(model, unique_keys) %}
{{ 
    config(severity= 'WARN') 
}}

{% set unique_keys = ', '.join(unique_keys) %}

with date_gaps as (
    select
        {{ unique_keys }} as pk_columns, 
        eff_dt,
        disc_dt,
        is_current,
        LEAD(eff_dt) over (partition by {{ unique_keys }} order by eff_dt) as next_eff_dt
        from {{ model }} 
)
select
    pk_columns,
    eff_dt,
    disc_dt,
    is_current,
    'gaps in eff_dt' as error_msg
from date_gaps
where next_eff_dt is not null
and (next_eff_dt <= disc_dt
or DATEDIFF(day, next_eff_dt, disc_dt) > 1 )   -- This will check for gaps of more than 1 day (or adjust accordingly)


{% endtest %}