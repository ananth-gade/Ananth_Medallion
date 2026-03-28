-- Test: DIM_EMPLOYEE - Natural key EMPLOYEE_EIN must not be null
-- Validates that every employee record has an EIN

select *
from {{ ref('DIM_EMPLOYEE') }}
where EMPLOYEE_EIN is null
