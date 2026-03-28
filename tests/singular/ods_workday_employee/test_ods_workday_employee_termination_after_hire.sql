-- Test: ODS_WORKDAY_EMPLOYEE - Termination date must be after hire date
-- Validates business rule that employees cannot be terminated before being hired

select *
from {{ ref('ODS_WORKDAY_EMPLOYEE') }}
where DATE_OF_HIRE is not null
  and TERMINATION_DATE is not null
  and TERMINATION_DATE <= DATE_OF_HIRE
