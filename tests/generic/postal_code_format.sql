{% test postal_code_format(model, column_name) %}

select *
from {{ model }}
where {{ column_name }} is not null
  and {{ column_name }} not rlike '^[0-9]{5}(-[0-9]{4})?$|^[A-Za-z][0-9][A-Za-z] ?[0-9][A-Za-z][0-9]$'

{% endtest %}