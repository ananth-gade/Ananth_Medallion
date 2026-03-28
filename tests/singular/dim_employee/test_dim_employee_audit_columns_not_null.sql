-- Test: DIM_EMPLOYEE - Audit columns must not be null
-- Validates that essential audit and ETL metadata columns are always populated

select *
from {{ ref('DIM_EMPLOYEE') }}
where INS_BATCH_ID is null
   or UPD_BATCH_ID is null
   or CDM_INSERT_TIMESTAMP is null
   or CDM_UPDATE_TIMESTAMP is null
   or SOURCE_SYSTEM is null
   or PRIMARY_DATA_SOURCE is null
