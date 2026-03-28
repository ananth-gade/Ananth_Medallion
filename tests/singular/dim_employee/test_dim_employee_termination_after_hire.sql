-- Test: DIM_EMPLOYEE - Termination date must be after hire date
-- Validates business rule that employees cannot be terminated before being hired
-- Excludes default sentinel values (9999-12-31 for TERM_DT, 1900-01-01 for HIRE_DT)

select *
from {{ ref('DIM_EMPLOYEE') }}
where HIRE_DT is not null
  and TERM_DT is not null
  and TERM_DT != '9999-12-31'
  and HIRE_DT != '1900-01-01'
  and TERM_DT <= HIRE_DT
