-- Test: ODS_ES_EQAI_USERS - Email format validation
-- Validates that EMAIL follows standard email format when populated

select *
from {{ ref('ODS_ES_EQAI_USERS') }}
where EMAIL is not null
  and trim(EMAIL) != ''
  and EMAIL not rlike '^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$'
