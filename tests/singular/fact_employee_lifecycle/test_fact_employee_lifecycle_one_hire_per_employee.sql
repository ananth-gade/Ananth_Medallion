-- Test: FACT_EMPLOYEE_LIFECYCLE - Every employee must have exactly one HIRE event
-- Validates that each distinct EMPLOYEE_EIN has one and only one HIRE event row

select
    EMPLOYEE_EIN,
    sum(case when EVENT_TYPE = 'HIRE' then 1 else 0 end) as hire_count
from {{ ref('FACT_EMPLOYEE_LIFECYCLE') }}
group by EMPLOYEE_EIN
having hire_count != 1
