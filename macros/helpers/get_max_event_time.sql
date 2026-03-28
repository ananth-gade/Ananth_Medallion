# macros/get_max_event_time.sql
{% macro get_max_event_time(date_field, not_minus3) %}
{% if execute and is_incremental() and env_var('ENABLE_MAX_EVENT_TIME_MACRO', '1') == '1' %}
{% if not_minus3 %}
    {% set query %}
    SELECT max({{date_field}}) FROM {{ this }};
    {% endset %}
{% else %}
    {% set query %}
    SELECT max({{date_field}}) - interval '3 minutes' FROM {{ this }};
    {% endset %}
{% endif %}
{% set max_event_time = run_query(query).columns[0][0] %}
{% if max_event_time is none %}
    {% set max_event_time = '19000101000000000'%}
{% endif %}
{% do return(max_event_time) %}
{% endif %}
{% endmacro %}