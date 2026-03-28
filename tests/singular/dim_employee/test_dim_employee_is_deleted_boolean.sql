-- Test: DIM_EMPLOYEE - IS_DELETED must be a valid boolean value
-- Validates the soft delete indicator contains only expected values

select *
from {{ ref('DIM_EMPLOYEE') }}
where IS_DELETED is null
   or IS_DELETED not in (true, false)
