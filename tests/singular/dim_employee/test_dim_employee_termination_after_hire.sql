-- Test: DIM_EMPLOYEE - Termination date must be after hire date
-- Validates business rule that employees cannot be terminated before being hired
-- Excludes default sentinel values (9999-12-31 for TERMINATION_DATE, 1900-01-01 for HIRE_DATE)

select *
from {{ ref('DIM_EMPLOYEE') }}
where HIRE_DATE is not null
  and TERMINATION_DATE is not null
  and TERMINATION_DATE != '9999-12-31'
  and HIRE_DATE != '1900-01-01'
  and TERMINATION_DATE <= HIRE_DATE
