-- Test: ODS_WORKDAY_EMPLOYEE - Primary key must not be null
-- Validates that ODS_WORKDAY_EMPLOYEE_PK is always populated

select *
from {{ ref('ODS_WORKDAY_EMPLOYEE') }}
where ODS_WORKDAY_EMPLOYEE_PK is null
