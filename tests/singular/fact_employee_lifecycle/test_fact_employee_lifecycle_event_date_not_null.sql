-- Test: FACT_EMPLOYEE_LIFECYCLE - EVENT_DATE must not be null
-- Validates that every lifecycle event has a date assigned

select *
from {{ ref('FACT_EMPLOYEE_LIFECYCLE') }}
where EVENT_DATE is null
