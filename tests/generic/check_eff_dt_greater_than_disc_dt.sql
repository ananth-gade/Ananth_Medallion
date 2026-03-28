{% test check_eff_dt_greater_than_disc_dt(model, unique_keys) %}
{{ 
    config(severity= 'WARN') 
}}

{% set unique_keys = ', '.join(unique_keys) %}

select     
    {{ unique_keys }} as pk_columns, 
    eff_dt,
    disc_dt,
    is_current,
    'eff_dt is greater than disc_dt' as error_msg
from 
    {{ model }} src
where disc_dt is not null
and date(disc_dt) < date(eff_dt)

{% endtest %}