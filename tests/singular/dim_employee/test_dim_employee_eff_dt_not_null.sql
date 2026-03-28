-- Test: DIM_EMPLOYEE - SCD Type 2 EFF_DT must not be null
-- Validates that every dimension record version has an effective date

select *
from {{ ref('DIM_EMPLOYEE') }}
where EFF_DT is null
