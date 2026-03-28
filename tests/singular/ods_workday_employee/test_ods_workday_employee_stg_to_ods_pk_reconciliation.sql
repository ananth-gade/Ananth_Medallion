-- Test: ODS_WORKDAY_EMPLOYEE - STG to ODS reconciliation by primary key
-- Validates every distinct EMPLOYEE_ID in STG_WORKDAY_EMPLOYEE exists in ODS_WORKDAY_EMPLOYEE
-- Ensures no records were lost during STG → ODS transformation (excludes deleted records)

with stg_keys as (
    select distinct EMPLOYEE_ID
    from {{ source('STAGING', 'STG_WORKDAY_EMPLOYEE') }}
    where EMPLOYEE_ID is not null
),
ods_keys as (
    select distinct EMPLOYEE_ID
    from {{ ref('ODS_WORKDAY_EMPLOYEE') }}
    where EMPLOYEE_ID is not null
      and SRC_DEL_IND = 'N'
)

select
    stg.EMPLOYEE_ID as MISSING_EMPLOYEE_ID,
    'EXISTS_IN_STG_NOT_IN_ODS' as RECONCILIATION_STATUS
from stg_keys stg
left join ods_keys ods
    on stg.EMPLOYEE_ID = ods.EMPLOYEE_ID
where ods.EMPLOYEE_ID is null
