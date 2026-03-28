{% test no_self_supervision(model, employee_column, supervisor_column) %}

select *
from {{ model }}
where {{ employee_column }} is not null
  and {{ supervisor_column }} is not null
  and {{ employee_column }} = {{ supervisor_column }}

{% endtest %}