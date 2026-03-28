-- Test: ODS_WORKDAY_EMPLOYEE - Email format validation
-- Validates that EMAIL_ID follows standard email format when populated

select *
from {{ ref('ODS_WORKDAY_EMPLOYEE') }}
where EMAIL_ID is not null
  and EMAIL_ID not rlike '^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$'
