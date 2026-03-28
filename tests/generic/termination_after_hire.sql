{% test termination_after_hire(model, hire_date_column, termination_date_column) %}

select *
from {{ model }}
where {{ hire_date_column }} is not null
  and {{ termination_date_column }} is not null
  and {{ termination_date_column }} <= {{ hire_date_column }}

{% endtest %}