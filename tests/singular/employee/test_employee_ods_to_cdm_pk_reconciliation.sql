-- Test: EMPLOYEE - ODS to CDM reconciliation by primary key
-- Validates every distinct EMPLOYEE_ID in ODS_WORKDAY_EMPLOYEE exists as EMPLOYEE_EIN in CDM EMPLOYEE
-- Ensures no records were lost during ODS → CDM transformation

with ods_keys as (
    select distinct EMPLOYEE_ID
    from {{ ref('ODS_WORKDAY_EMPLOYEE') }}
    where EMPLOYEE_ID is not null
      and SRC_DEL_IND = 'N'
),
cdm_keys as (
    select distinct EMPLOYEE_EIN
    from {{ ref('EMPLOYEE') }}
    where EMPLOYEE_EIN is not null
      and IS_DELETED = 0
)

select
    ods.EMPLOYEE_ID as MISSING_EMPLOYEE_EIN,
    'EXISTS_IN_ODS_NOT_IN_CDM' as RECONCILIATION_STATUS
from ods_keys ods
left join cdm_keys cdm
    on ods.EMPLOYEE_ID = cdm.EMPLOYEE_EIN
where cdm.EMPLOYEE_EIN is null
