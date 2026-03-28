-- Test: EMPLOYEE - Email format validation
-- Validates that EMAIL_ADDRESS follows standard email format when populated

select *
from {{ ref('EMPLOYEE') }}
where EMAIL_ADDRESS is not null
  and EMAIL_ADDRESS not rlike '^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$'
