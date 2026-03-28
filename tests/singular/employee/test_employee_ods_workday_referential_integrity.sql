-- Test: EMPLOYEE - Referential integrity with ODS_WORKDAY_EMPLOYEE
-- Validates every EMPLOYEE record has a corresponding ODS_WORKDAY_EMPLOYEE record

select emp.*
from {{ ref('EMPLOYEE') }} emp
left join {{ ref('ODS_WORKDAY_EMPLOYEE') }} ods
    on emp.ODS_WORKDAY_EMPLOYEE_PK = ods.ODS_WORKDAY_EMPLOYEE_PK
where ods.ODS_WORKDAY_EMPLOYEE_PK is null
  and emp.ODS_WORKDAY_EMPLOYEE_PK is not null
