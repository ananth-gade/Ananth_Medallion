{% test terminated_status_consistency(model, status_column, termination_date_column) %}

select *
from {{ model }}
where (
    -- Terminated employees should have a termination date
    ({{ status_column }} = 'TERMINATED' and {{ termination_date_column }} is null)
    or
    -- Active employees should not have a termination date in the past
    ({{ status_column }} = 'ACTIVE' and {{ termination_date_column }} is not null and {{ termination_date_column }} <= current_date())
)

{% endtest %}