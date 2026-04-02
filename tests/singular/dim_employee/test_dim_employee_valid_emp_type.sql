-- Test: DIM_EMPLOYEE - EMPLOYEE_TYPE must contain only valid values
-- Validates employee type code (F=Full-Time, P=Part-Time, C=Contract, T=Temporary)

select *
from {{ ref('DIM_EMPLOYEE') }}
where EMPLOYEE_TYPE is not null
  and EMPLOYEE_TYPE not in ('F', 'P', 'C', 'T')
