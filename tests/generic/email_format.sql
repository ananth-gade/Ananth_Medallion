{% test email_format(model, column_name) %}

select *
from {{ model }}
where {{ column_name }} is not null
  and {{ column_name }} not rlike '^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$'

{% endtest %}