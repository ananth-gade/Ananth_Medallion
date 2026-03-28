-- Test: DIM_EMPLOYEE - EMP_TYP must contain only valid values
-- Validates employee type code (F=Full-Time, P=Part-Time, C=Contract, T=Temporary)

select *
from {{ ref('DIM_EMPLOYEE') }}
where EMP_TYP is not null
  and EMP_TYP not in ('F', 'P', 'C', 'T')
