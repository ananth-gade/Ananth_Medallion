-- Test: FACT_EMPLOYEE_LIFECYCLE - IS_PROMOTION and IS_LATERAL must be mutually exclusive
-- Validates that a role change event cannot be flagged as both a promotion and a lateral move

select *
from {{ ref('FACT_EMPLOYEE_LIFECYCLE') }}
where IS_PROMOTION = true
  and IS_LATERAL = true
