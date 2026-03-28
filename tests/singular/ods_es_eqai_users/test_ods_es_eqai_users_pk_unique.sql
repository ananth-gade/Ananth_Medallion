-- Test: ODS_ES_EQAI_USERS - Primary key must be unique
-- Validates no duplicate ODS_ES_EQAI_USERS_PK values exist

select
    ODS_ES_EQAI_USERS_PK,
    count(*) as cnt
from {{ ref('ODS_ES_EQAI_USERS') }}
group by ODS_ES_EQAI_USERS_PK
having count(*) > 1
