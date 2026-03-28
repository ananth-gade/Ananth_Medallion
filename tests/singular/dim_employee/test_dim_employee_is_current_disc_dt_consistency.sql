-- Test: DIM_EMPLOYEE - IS_CURRENT flag must align with DISC_DT
-- Validates SCD2 consistency: IS_CURRENT = true only when DISC_DT = '9999-12-31'
-- and IS_CURRENT = false when DISC_DT < '9999-12-31'

select *
from {{ ref('DIM_EMPLOYEE') }}
where (IS_CURRENT = true and DISC_DT != '9999-12-31')
   or (IS_CURRENT = false and DISC_DT = '9999-12-31')
