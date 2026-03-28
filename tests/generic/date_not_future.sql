{% test date_not_future(model, column_name) %}

select *
from {{ model }}
where {{ column_name }} is not null
  and {{ column_name }} > current_date()

{% endtest %}