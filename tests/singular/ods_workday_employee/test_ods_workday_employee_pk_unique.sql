-- Test: ODS_WORKDAY_EMPLOYEE - Primary key must be unique
-- Validates no duplicate ODS_WORKDAY_EMPLOYEE_PK values exist

select
    ODS_WORKDAY_EMPLOYEE_PK,
    count(*) as cnt
from {{ ref('ODS_WORKDAY_EMPLOYEE') }}
group by ODS_WORKDAY_EMPLOYEE_PK
having count(*) > 1
