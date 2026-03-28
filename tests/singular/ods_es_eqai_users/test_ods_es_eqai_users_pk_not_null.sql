-- Test: ODS_ES_EQAI_USERS - Primary key must not be null
-- Validates that ODS_ES_EQAI_USERS_PK is always populated

select *
from {{ ref('ODS_ES_EQAI_USERS') }}
where ODS_ES_EQAI_USERS_PK is null
