{% macro trim_string(Input_Field) %}
LTRIM(RTRIM(REGEXP_REPLACE({{ Input_Field }},' +',' '))) 
{% endmacro %}