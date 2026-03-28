-- Test: DIM_EMPLOYEE - Referential integrity with CDM EMPLOYEE
-- Validates every DIM_EMPLOYEE record can trace back to a CDM EMPLOYEE record

select dim.*
from {{ ref('DIM_EMPLOYEE') }} dim
left join {{ ref('EMPLOYEE') }} cdm
    on dim.EMPLOYEE_EIN = cdm.EMPLOYEE_EIN
where cdm.EMPLOYEE_EIN is null
  and dim.EMPLOYEE_EIN is not null
  and dim.IS_DELETED = false
