-- Test: DIM_EMPLOYEE - EMPLOYEE_STATUS must contain only valid values
-- Validates employment status contains expected codes

select *
from {{ ref('DIM_EMPLOYEE') }}
where EMPLOYEE_STATUS is not null
  and UPPER(TRIM(EMPLOYEE_STATUS)) not in ('A', 'T', 'L', 'R', 'ACTIV', 'TERM', 'LOA', 'RETIR')
