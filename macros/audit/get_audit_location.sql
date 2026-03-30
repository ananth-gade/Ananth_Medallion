{% macro get_audit_database() %}
    {{ return('AUDIT') }}
{% endmacro %}

{% macro get_audit_schema() %}
    {{ return('DATA_TRANSFORMATION') }}
{% endmacro %}
