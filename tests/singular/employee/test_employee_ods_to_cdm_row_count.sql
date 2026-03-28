-- Test: EMPLOYEE - ODS to CDM row count reconciliation
-- Validates that distinct key counts match between ODS and CDM layers
-- Compares active (non-deleted) ODS_WORKDAY_EMPLOYEE records against CDM EMPLOYEE

with ods_count as (
    select count(distinct EMPLOYEE_ID) as cnt
    from {{ ref('ODS_WORKDAY_EMPLOYEE') }}
    where EMPLOYEE_ID is not null
      and SRC_DEL_IND = 'N'
),
cdm_count as (
    select count(distinct EMPLOYEE_EIN) as cnt
    from {{ ref('EMPLOYEE') }}
    where EMPLOYEE_EIN is not null
      and IS_DELETED = 0
)

select
    ods.cnt as ODS_DISTINCT_KEY_COUNT,
    cdm.cnt as CDM_DISTINCT_KEY_COUNT,
    ods.cnt - cdm.cnt as COUNT_DIFFERENCE
from ods_count ods
cross join cdm_count cdm
where ods.cnt != cdm.cnt
