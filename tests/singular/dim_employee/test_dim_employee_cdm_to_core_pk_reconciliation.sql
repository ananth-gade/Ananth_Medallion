-- Test: DIM_EMPLOYEE - CDM to CORE reconciliation by primary key
-- Validates every distinct EMPLOYEE_EIN in CDM EMPLOYEE exists in CORE DIM_EMPLOYEE
-- Ensures no records were lost during CDM → CORE transformation

with cdm_keys as (
    select distinct EMPLOYEE_EIN
    from {{ ref('EMPLOYEE') }}
    where EMPLOYEE_EIN is not null
      and IS_DELETED = 0
),
core_keys as (
    select distinct EMPLOYEE_EIN
    from {{ ref('DIM_EMPLOYEE') }}
    where EMPLOYEE_EIN is not null
      and IS_DELETED = false
)

select
    cdm.EMPLOYEE_EIN as MISSING_EMPLOYEE_EIN,
    'EXISTS_IN_CDM_NOT_IN_CORE' as RECONCILIATION_STATUS
from cdm_keys cdm
left join core_keys core
    on cdm.EMPLOYEE_EIN = core.EMPLOYEE_EIN
where core.EMPLOYEE_EIN is null
