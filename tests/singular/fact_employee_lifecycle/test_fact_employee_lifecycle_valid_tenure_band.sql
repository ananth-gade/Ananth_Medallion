-- Test: FACT_EMPLOYEE_LIFECYCLE - TENURE_BAND must contain only valid values
-- Validates the derived tenure band classification contains only expected cohorts

select *
from {{ ref('FACT_EMPLOYEE_LIFECYCLE') }}
where TENURE_BAND is not null
  and TENURE_BAND not in ('<1 year', '1-3 years', '3-5 years', '5+ years')
