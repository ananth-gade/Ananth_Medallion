-- Test: EMPLOYEE - Complete name should contain first and last name components
-- Validates that COMPLETE_NAME is consistent with FIRST_NAME and LAST_NAME

select *
from {{ ref('EMPLOYEE') }}
where FIRST_NAME is not null
  and LAST_NAME is not null
  and COMPLETE_NAME is not null
  and (
    COMPLETE_NAME not ilike '%' || FIRST_NAME || '%'
    or COMPLETE_NAME not ilike '%' || LAST_NAME || '%'
  )
