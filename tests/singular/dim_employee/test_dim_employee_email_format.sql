-- Test: DIM_EMPLOYEE - Email format validation
-- Validates that EMAIL_ADDRESS follows standard email format when populated

select *
from {{ ref('DIM_EMPLOYEE') }}
where EMAIL_ADDRESS is not null
  and EMAIL_ADDRESS not rlike '^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$'
