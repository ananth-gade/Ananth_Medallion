-- Test: FACT_EMPLOYEE_LIFECYCLE - Business key EMPLOYEE_EIN must not be null
-- Validates that every lifecycle event has an associated employee identifier

select *
from {{ ref('FACT_EMPLOYEE_LIFECYCLE') }}
where EMPLOYEE_EIN is null
