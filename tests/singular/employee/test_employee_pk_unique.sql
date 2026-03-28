-- Test: EMPLOYEE - Primary key must be unique
-- Validates no duplicate CDM_EMPLOYEE_PK values exist

select
    CDM_EMPLOYEE_PK,
    count(*) as cnt
from {{ ref('EMPLOYEE') }}
group by CDM_EMPLOYEE_PK
having count(*) > 1
