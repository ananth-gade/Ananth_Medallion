-- Test: ODS_WORKDAY_EMPLOYEE - Business key EMPLOYEE_ID must not be null
-- Validates that every record has an EMPLOYEE_ID

select *
from {{ ref('ODS_WORKDAY_EMPLOYEE') }}
where EMPLOYEE_ID is null
