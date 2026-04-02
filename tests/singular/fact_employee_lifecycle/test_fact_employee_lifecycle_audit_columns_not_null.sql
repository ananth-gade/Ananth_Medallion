-- Test: FACT_EMPLOYEE_LIFECYCLE - Audit columns must not be null
-- Validates that essential audit metadata columns are always populated

select *
from {{ ref('FACT_EMPLOYEE_LIFECYCLE') }}
where INS_BATCH_ID is null
   or UPD_BATCH_ID is null
   or CDM_INSERT_TIMESTAMP is null
   or CDM_UPDATE_TIMESTAMP is null
   or SOURCE_SYSTEM is null
   or PRIMARY_DATA_SOURCE is null
   or OPERATION_TYPE is null
