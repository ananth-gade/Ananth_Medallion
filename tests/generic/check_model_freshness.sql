{% test check_model_freshness(model, column_name, value) %}
    with validation as (
        select max({{ column_name }})::DATE as last_load from {{ model }}
    ),

    validation_error as (
      select last_load from validation where last_load < dateadd(day, -{{ value }}, current_date())
    )

    select * from validation_error
{% endtest %}