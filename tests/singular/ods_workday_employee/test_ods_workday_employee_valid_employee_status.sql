-- Test: ODS_WORKDAY_EMPLOYEE - Employee status must be a valid value
-- Validates that EMPLOYEE_STATUS contains only expected status codes

select *
from {{ ref('ODS_WORKDAY_EMPLOYEE') }}
where EMPLOYEE_STATUS is not null
  and EMPLOYEE_STATUS not in ('A', 'T', 'L', 'R', 'S', 'D', 'P')
