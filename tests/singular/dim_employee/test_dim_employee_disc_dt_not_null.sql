-- Test: DIM_EMPLOYEE - SCD Type 2 DISC_DT must not be null
-- Validates that every dimension record version has a discontinued date
-- Active records should have DISC_DT = '9999-12-31'

select *
from {{ ref('DIM_EMPLOYEE') }}
where DISC_DT is null
