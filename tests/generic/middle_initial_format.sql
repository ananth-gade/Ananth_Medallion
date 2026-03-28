{% test middle_initial_format(model, column_name) %}

select *
from {{ model }}
where {{ column_name }} is not null
  and {{ column_name }} not rlike '^[A-Za-z]$'

{% endtest %}