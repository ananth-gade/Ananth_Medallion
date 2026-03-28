-- Test: FACT_EMPLOYEE_LIFECYCLE - Referential integrity with DIM_EMPLOYEE
-- Validates every EMPLOYEE_SK in the fact table exists in DIM_EMPLOYEE

select fact.*
from {{ ref('FACT_EMPLOYEE_LIFECYCLE') }} fact
left join {{ ref('DIM_EMPLOYEE') }} dim
    on fact.EMPLOYEE_SK = dim.EMPLOYEE_SK
where dim.EMPLOYEE_SK is null
  and fact.EMPLOYEE_SK is not null
