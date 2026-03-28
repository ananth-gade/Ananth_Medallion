-- Test: FACT_EMPLOYEE_LIFECYCLE - Row count reconciliation with DIM_EMPLOYEE
-- Validates that distinct EMPLOYEE_EIN count in fact matches distinct count in dim
-- Every employee with at least one SCD2 version should have at least one lifecycle event

with fact_count as (
    select count(distinct EMPLOYEE_EIN) as cnt
    from {{ ref('FACT_EMPLOYEE_LIFECYCLE') }}
    where EMPLOYEE_EIN is not null
),
dim_count as (
    select count(distinct EMPLOYEE_EIN) as cnt
    from {{ ref('DIM_EMPLOYEE') }}
    where EMPLOYEE_EIN is not null
      and IS_DELETED = false
)

select
    dim.cnt as DIM_DISTINCT_EMPLOYEE_COUNT,
    fact.cnt as FACT_DISTINCT_EMPLOYEE_COUNT,
    dim.cnt - fact.cnt as COUNT_DIFFERENCE
from dim_count dim
cross join fact_count fact
where dim.cnt != fact.cnt
