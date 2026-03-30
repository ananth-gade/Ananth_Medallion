{% macro show_databases() %}
  {% set results = run_query("SHOW DATABASES") %}
  {% if execute %}
    {% for row in results %}
      {{ log(row[1], info=True) }}
    {% endfor %}
  {% endif %}
{% endmacro %}

{% macro show_schemas_in_db(db_name) %}
  {% set results = run_query("SHOW SCHEMAS IN DATABASE " ~ db_name) %}
  {% if execute %}
    {% for row in results %}
      {{ log(db_name ~ "." ~ row[1], info=True) }}
    {% endfor %}
  {% endif %}
{% endmacro %}
