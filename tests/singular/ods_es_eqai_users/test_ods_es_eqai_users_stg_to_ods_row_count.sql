-- Test: ODS_ES_EQAI_USERS - STG to ODS row count reconciliation
-- Validates that distinct USER_ID counts match between STG and ODS layers
-- Detects record loss or unexpected duplication across layers (excludes deleted records)

with stg_count as (
    select count(distinct USER_ID) as cnt
    from {{ source('STAGING', 'STG_ES_EQAI_USERS') }}
    where USER_ID is not null
),
ods_count as (
    select count(distinct USER_ID) as cnt
    from {{ ref('ODS_ES_EQAI_USERS') }}
    where USER_ID is not null
      and SRC_DEL_IND = 'N'
)

select
    stg.cnt as STG_DISTINCT_KEY_COUNT,
    ods.cnt as ODS_DISTINCT_KEY_COUNT,
    stg.cnt - ods.cnt as COUNT_DIFFERENCE
from stg_count stg
cross join ods_count ods
where stg.cnt != ods.cnt
