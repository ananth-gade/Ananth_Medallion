-- Test: DIM_EMPLOYEE - CDM to CORE row count reconciliation
-- Validates that distinct EMPLOYEE_EIN counts match between CDM and CORE layers
-- Compares active (non-deleted) records in both layers

with cdm_count as (
    select count(distinct EMPLOYEE_EIN) as cnt
    from {{ ref('EMPLOYEE') }}
    where EMPLOYEE_EIN is not null
      and IS_DELETED = 0
),
core_count as (
    select count(distinct EMPLOYEE_EIN) as cnt
    from {{ ref('DIM_EMPLOYEE') }}
    where EMPLOYEE_EIN is not null
      and IS_DELETED = false
)

select
    cdm.cnt as CDM_DISTINCT_KEY_COUNT,
    core.cnt as CORE_DISTINCT_KEY_COUNT,
    cdm.cnt - core.cnt as COUNT_DIFFERENCE
from cdm_count cdm
cross join core_count core
where cdm.cnt != core.cnt
