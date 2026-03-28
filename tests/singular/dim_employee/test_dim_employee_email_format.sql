-- Test: DIM_EMPLOYEE - Email format validation
-- Validates that EMAIL_ADDR follows standard email format when populated

select *
from {{ ref('DIM_EMPLOYEE') }}
where EMAIL_ADDR is not null
  and EMAIL_ADDR not rlike '^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$'
