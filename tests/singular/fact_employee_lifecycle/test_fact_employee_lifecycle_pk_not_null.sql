-- Test: FACT_EMPLOYEE_LIFECYCLE - Primary key must not be null
-- Validates that LIFECYCLE_SK is always populated

select *
from {{ ref('FACT_EMPLOYEE_LIFECYCLE') }}
where LIFECYCLE_SK is null
