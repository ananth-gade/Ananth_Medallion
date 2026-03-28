{% test check_is_current_flg(model, unique_keys) %}
{{ 
    config(severity= 'WARN') 
}}

{% set unique_keys = ', '.join(unique_keys) %}

with is_current_validation as (
    select     
        {{ unique_keys }} as pk_columns, 
        case when disc_dt = '9999-12-31' and IS_CURRENT = 'FALSE'
            then 1
            else 0
            end as error_cd,
        eff_dt,
        disc_dt,
        is_current
    from 
        {{ model }} src
)

select 
    pk_columns,
    eff_dt,
    disc_dt,
    is_current,
    'is_current flag is false despite the disc dt is a future date' as error_msg
from 
    is_current_validation
where error_cd = 1

{% endtest %}