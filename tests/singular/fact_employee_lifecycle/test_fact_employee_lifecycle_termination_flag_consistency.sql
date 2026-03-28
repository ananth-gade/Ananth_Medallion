-- Test: FACT_EMPLOYEE_LIFECYCLE - IS_TERMINATION flag must align with EVENT_TYPE
-- Validates IS_TERMINATION = true only when EVENT_TYPE = 'TERMINATION'

select *
from {{ ref('FACT_EMPLOYEE_LIFECYCLE') }}
where (IS_TERMINATION = true and EVENT_TYPE != 'TERMINATION')
   or (IS_TERMINATION = false and EVENT_TYPE = 'TERMINATION')
