-- Test: DIM_EMPLOYEE - Each EMPLOYEE_EIN must have exactly one current record
-- Validates SCD2 rule: only one version per employee should have IS_CURRENT = true

select
    EMPLOYEE_EIN,
    sum(case when IS_CURRENT = true and IS_DELETED = false then 1 else 0 end) as current_count
from {{ ref('DIM_EMPLOYEE') }}
group by EMPLOYEE_EIN
having sum(case when IS_CURRENT = true and IS_DELETED = false then 1 else 0 end) <> 1
