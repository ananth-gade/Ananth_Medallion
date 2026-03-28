-- Test: EMPLOYEE - Audit columns must not be null
-- Validates that essential CDM metadata columns are always populated

select *
from {{ ref('EMPLOYEE') }}
where CDM_INS_BATCH_ID is null
   or CDM_UPD_BATCH_ID is null
   or CDM_INSERT_TIMESTAMP is null
   or CDM_UPDATE_TIMESTAMP is null
   or SOURCE_SYSTEM is null
   or PRIMARY_DATA_SOURCE is null
   or HASH_CHANGE is null
   or HASH_FULL is null
