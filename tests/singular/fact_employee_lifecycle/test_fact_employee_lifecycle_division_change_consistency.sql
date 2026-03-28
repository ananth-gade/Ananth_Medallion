-- Test: FACT_EMPLOYEE_LIFECYCLE - IS_DIVISION_CHANGE flag consistency
-- Validates IS_DIVISION_CHANGE is true only when PREV and NEW division numbers differ

select *
from {{ ref('FACT_EMPLOYEE_LIFECYCLE') }}
where (
    -- Flag is true but divisions are the same
    (IS_DIVISION_CHANGE = true
     and PREV_DIVISION_NUMBER is not null
     and NEW_DIVISION_NUMBER is not null
     and PREV_DIVISION_NUMBER = NEW_DIVISION_NUMBER)
    or
    -- Flag is false but divisions are different
    (IS_DIVISION_CHANGE = false
     and PREV_DIVISION_NUMBER is not null
     and NEW_DIVISION_NUMBER is not null
     and PREV_DIVISION_NUMBER != NEW_DIVISION_NUMBER)
)
