{% test ein_format(model, column_name) %}

select *
from {{ model }}
where {{ column_name }} is not null
  and {{ column_name }} not rlike '^[A-Za-z0-9]{1,15}$'

{% endtest %}