-- Test: FACT_EMPLOYEE_LIFECYCLE - Primary key must be unique
-- Validates no duplicate LIFECYCLE_SK values exist

select
    LIFECYCLE_SK,
    count(*) as cnt
from {{ ref('FACT_EMPLOYEE_LIFECYCLE') }}
group by LIFECYCLE_SK
having count(*) > 1
