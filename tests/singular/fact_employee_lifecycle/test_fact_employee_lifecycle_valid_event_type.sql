-- Test: FACT_EMPLOYEE_LIFECYCLE - EVENT_TYPE must contain only valid values
-- Validates the event classification contains only expected lifecycle event types

select *
from {{ ref('FACT_EMPLOYEE_LIFECYCLE') }}
where EVENT_TYPE not in ('HIRE', 'TERMINATION', 'LOA', 'RETURN', 'ROLE_CHANGE', 'ORG_TRANSFER', 'PROFILE_UPDATE')
