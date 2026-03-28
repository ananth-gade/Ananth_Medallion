-- Test: FACT_EMPLOYEE_LIFECYCLE - Tenure measures must be consistent
-- Validates TENURE_MONTHS and TENURE_YEARS are correctly derived from TENURE_DAYS
-- Allows small rounding tolerance of ±1 unit

select *
from {{ ref('FACT_EMPLOYEE_LIFECYCLE') }}
where TENURE_DAYS is not null
  and (
    abs(TENURE_MONTHS - ROUND(TENURE_DAYS / 30.44, 1)) > 0.1
    or abs(TENURE_YEARS - ROUND(TENURE_DAYS / 365.25, 2)) > 0.01
  )
