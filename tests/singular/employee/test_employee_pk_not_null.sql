-- Test: EMPLOYEE - Primary key must not be null
-- Validates that CDM_EMPLOYEE_PK is always populated

select *
from {{ ref('EMPLOYEE') }}
where CDM_EMPLOYEE_PK is null
