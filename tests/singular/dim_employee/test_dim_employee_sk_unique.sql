-- Test: DIM_EMPLOYEE - Surrogate key must be unique
-- Validates no duplicate EMPLOYEE_SK values exist across all SCD2 versions

select
    EMPLOYEE_SK,
    count(*) as cnt
from {{ ref('DIM_EMPLOYEE') }}
group by EMPLOYEE_SK
having count(*) > 1
