-- Test: EMPLOYEE - IS_DELETED must be a valid boolean value (0 or 1)
-- Validates the soft delete indicator contains only expected values

select *
from {{ ref('EMPLOYEE') }}
where IS_DELETED not in (0, 1)
