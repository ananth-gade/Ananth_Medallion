{% test phone_format(model, column_name) %}

select *
from {{ model }}
where {{ column_name }} is not null
  and {{ column_name }} not rlike '^[0-9]{3}-?[0-9]{3}-?[0-9]{4}$|^\\([0-9]{3}\\)[0-9]{3}-?[0-9]{4}$|^[0-9]{10}$'

{% endtest %}