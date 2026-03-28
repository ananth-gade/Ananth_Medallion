-- Test: ODS_ES_EQAI_USERS - Audit columns must not be null
-- Validates that essential ETL metadata columns are always populated

select *
from {{ ref('ODS_ES_EQAI_USERS') }}
where ODS_INS_BATCH_ID is null
   or ODS_UPD_BATCH_ID is null
   or ODS_INSERT_TIMESTAMP is null
   or ODS_UPDATE_TIMESTAMP is null
   or SOURCE_SYSTEM is null
   or SOURCE_TABLE is null
   or HASH_CHANGE is null
   or HASH_FULL is null
