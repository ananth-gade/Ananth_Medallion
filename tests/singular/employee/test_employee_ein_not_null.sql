-- Test: EMPLOYEE - Business key EMPLOYEE_EIN must not be null
-- Validates that every employee record has an EIN

select *
from {{ ref('EMPLOYEE') }}
where EMPLOYEE_EIN is null
