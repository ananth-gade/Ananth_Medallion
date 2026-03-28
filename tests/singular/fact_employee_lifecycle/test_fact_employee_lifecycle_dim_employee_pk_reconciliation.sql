-- Test: FACT_EMPLOYEE_LIFECYCLE - EMPLOYEE_EIN reconciliation with DIM_EMPLOYEE
-- Validates every distinct EMPLOYEE_EIN in the fact table exists in DIM_EMPLOYEE
-- Ensures no orphaned fact records without a dimension counterpart

with fact_keys as (
    select distinct EMPLOYEE_EIN
    from {{ ref('FACT_EMPLOYEE_LIFECYCLE') }}
    where EMPLOYEE_EIN is not null
),
dim_keys as (
    select distinct EMPLOYEE_EIN
    from {{ ref('DIM_EMPLOYEE') }}
    where EMPLOYEE_EIN is not null
)

select
    fact.EMPLOYEE_EIN as MISSING_EMPLOYEE_EIN,
    'EXISTS_IN_FACT_NOT_IN_DIM' as RECONCILIATION_STATUS
from fact_keys fact
left join dim_keys dim
    on fact.EMPLOYEE_EIN = dim.EMPLOYEE_EIN
where dim.EMPLOYEE_EIN is null
