-- Test: DIM_EMPLOYEE - Employee cannot be their own supervisor
-- Validates that EMPLOYEE_EIN != SUPERVISOR

select *
from {{ ref('DIM_EMPLOYEE') }}
where EMPLOYEE_EIN is not null
  and SUPERVISOR is not null
  and EMPLOYEE_EIN = SUPERVISOR
