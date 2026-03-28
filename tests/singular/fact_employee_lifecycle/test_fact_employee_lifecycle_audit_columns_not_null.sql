-- Test: FACT_EMPLOYEE_LIFECYCLE - Audit columns must not be null
-- Validates that essential audit metadata columns are always populated

select *
from {{ ref('FACT_EMPLOYEE_LIFECYCLE') }}
where CDM_BATCH_ID is null
   or CDM_INSERT_TIMESTAMP is null
   or SOURCE_SYSTEM is null
