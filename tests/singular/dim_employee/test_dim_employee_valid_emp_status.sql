-- Test: DIM_EMPLOYEE - EMP_STATUS must contain only valid values
-- Validates employment status contains expected codes

select *
from {{ ref('DIM_EMPLOYEE') }}
where EMP_STATUS is not null
  and UPPER(TRIM(EMP_STATUS)) not in ('A', 'T', 'L', 'R', 'ACTIV', 'TERM', 'LOA', 'RETIR')
