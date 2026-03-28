-- Test: EMPLOYEE - Termination date must be after hire date
-- Validates business rule that employees cannot be terminated before being hired

select *
from {{ ref('EMPLOYEE') }}
where HIRE_DATE is not null
  and TERMINATION_DATE is not null
  and TERMINATION_DATE <= HIRE_DATE
