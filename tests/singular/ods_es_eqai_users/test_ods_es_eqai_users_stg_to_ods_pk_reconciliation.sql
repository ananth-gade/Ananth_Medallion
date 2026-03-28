-- Test: ODS_ES_EQAI_USERS - STG to ODS reconciliation by primary key
-- Validates every distinct USER_ID in STG_ES_EQAI_USERS exists in ODS_ES_EQAI_USERS
-- Ensures no records were lost during STG → ODS transformation (excludes deleted records)

with stg_keys as (
    select distinct USER_ID
    from {{ source('STAGING', 'STG_ES_EQAI_USERS') }}
    where USER_ID is not null
),
ods_keys as (
    select distinct USER_ID
    from {{ ref('ODS_ES_EQAI_USERS') }}
    where USER_ID is not null
      and SRC_DEL_IND = 'N'
)

select
    stg.USER_ID as MISSING_USER_ID,
    'EXISTS_IN_STG_NOT_IN_ODS' as RECONCILIATION_STATUS
from stg_keys stg
left join ods_keys ods
    on stg.USER_ID = ods.USER_ID
where ods.USER_ID is null
