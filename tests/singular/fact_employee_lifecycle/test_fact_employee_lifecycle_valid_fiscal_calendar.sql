-- Test: FACT_EMPLOYEE_LIFECYCLE - Fiscal calendar fields must be valid ranges
-- Validates EVENT_FISCAL_YEAR, EVENT_QUARTER, and EVENT_MONTH are within expected ranges

select *
from {{ ref('FACT_EMPLOYEE_LIFECYCLE') }}
where EVENT_FISCAL_YEAR is not null
  and (
    EVENT_FISCAL_YEAR < 1900 or EVENT_FISCAL_YEAR > 2100
    or EVENT_QUARTER not between 1 and 4
    or EVENT_MONTH not between 1 and 12
  )
