-- Test: EMPLOYEE - End-to-end STG to CDM reconciliation by primary key
-- Validates every distinct EMPLOYEE_ID in STG_WORKDAY_EMPLOYEE exists as EMPLOYEE_EIN in CDM EMPLOYEE
-- Full pipeline reconciliation: STG → ODS → CDM

with stg_keys as (
    select distinct EMPLOYEE_ID
    from {{ source('STAGING', 'STG_WORKDAY_EMPLOYEE') }}
    where EMPLOYEE_ID is not null
),
cdm_keys as (
    select distinct EMPLOYEE_EIN
    from {{ ref('EMPLOYEE') }}
    where EMPLOYEE_EIN is not null
      and IS_DELETED = 0
)

select
    stg.EMPLOYEE_ID as MISSING_EMPLOYEE_EIN,
    'EXISTS_IN_STG_NOT_IN_CDM' as RECONCILIATION_STATUS
from stg_keys stg
left join cdm_keys cdm
    on stg.EMPLOYEE_ID = cdm.EMPLOYEE_EIN
where cdm.EMPLOYEE_EIN is null
