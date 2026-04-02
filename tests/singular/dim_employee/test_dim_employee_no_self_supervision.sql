-- Test: DIM_EMPLOYEE - Employee cannot be their own supervisor
-- Validates that EMPLOYEE_EIN != SUPERVISOR_EIN

select *
from {{ ref('DIM_EMPLOYEE') }}
where EMPLOYEE_EIN is not null
  and SUPERVISOR_EIN is not null
  and EMPLOYEE_EIN = SUPERVISOR_EIN
