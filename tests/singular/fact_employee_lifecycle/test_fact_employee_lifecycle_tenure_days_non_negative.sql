-- Test: FACT_EMPLOYEE_LIFECYCLE - TENURE_DAYS must be non-negative
-- Validates that tenure calculation (EVENT_DATE - HIRE_DATE) never produces negative values

select *
from {{ ref('FACT_EMPLOYEE_LIFECYCLE') }}
where TENURE_DAYS is not null
  and TENURE_DAYS < 0
