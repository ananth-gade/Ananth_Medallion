-- Test: DIM_EMPLOYEE - Surrogate key must not be null
-- Validates that EMPLOYEE_SK is always populated

select *
from {{ ref('DIM_EMPLOYEE') }}
where EMPLOYEE_SK is null
