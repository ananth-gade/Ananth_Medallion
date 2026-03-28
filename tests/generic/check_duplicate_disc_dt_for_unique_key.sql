{% test check_duplicate_disc_dt_for_unique_key(model, unique_keys) %}
{{ 
    config(severity= 'WARN') 
}}

{% set unique_keys = ', '.join(unique_keys) %}

select     
    {{ unique_keys }} as pk_columns, 
    eff_dt,
    disc_dt,
    is_current,
    'duplicate disc date for a PK' as error_msg
from 
    {{ model }} src
group by {{ unique_keys }}, 
          disc_dt 
having count(*) > 1

{% endtest %}