-- Test: ODS_WORKDAY_EMPLOYEE - Employee cannot be their own supervisor
-- Validates that EMPLOYEE_ID != SUPERVISOR_EMPLOYEE_ID

select *
from {{ ref('ODS_WORKDAY_EMPLOYEE') }}
where EMPLOYEE_ID is not null
  and SUPERVISOR_EMPLOYEE_ID is not null
  and EMPLOYEE_ID = SUPERVISOR_EMPLOYEE_ID
