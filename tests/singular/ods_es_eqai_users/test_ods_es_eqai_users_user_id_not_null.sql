-- Test: ODS_ES_EQAI_USERS - Business key USER_ID must not be null
-- Validates that every record has a USER_ID

select *
from {{ ref('ODS_ES_EQAI_USERS') }}
where USER_ID is null
