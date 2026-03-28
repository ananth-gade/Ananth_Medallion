{% test check_scd_integrity(model, unique_keys) %}
{{ 
    config(severity= 'WARN') 
}}

/***************************
This test is to ensure the integrity of your Slowly Changing Dimension (SCD) table in CORE, 
to validate the behavior of your SCD process to make sure the SCD is intact. 
1. Test for no gaps  in the eff_dt and ensure correct sequence in effective dates
2. Test for Is_Active flag, to ensure is_active flg is set for a record with Future disc_dt
3. Test to ensure the eff_dt is always lesser than disc_dt 
4. Test to ensure there is only 1 active record, future disc_dt (no duplicate future disc_dt for a PK)
****************************/
{% set unique_keys = ', '.join(unique_keys) %}

with date_gaps as (
    select
        {{ unique_keys }} as pk_columns, 
        eff_dt,
        disc_dt,
        is_current,
        LEAD(eff_dt) over (partition by {{ unique_keys }} order by eff_dt) as next_eff_dt
        from {{ model }} 
),

is_current_validation as (
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
    'gaps in eff_dt' as error_msg
from date_gaps
where next_eff_dt is not null
and (next_eff_dt <= disc_dt
or DATEDIFF(day, next_eff_dt, disc_dt) > 1 )  -- This will check for gaps of more than 1 day (or adjust accordingly)

union all

select 
    pk_columns,
    eff_dt,
    disc_dt,
    is_current,
    'is_current flag is false despite the disc dt is a future date' as error_msg
from 
    is_current_validation
where error_cd = 1

union all

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

union all

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