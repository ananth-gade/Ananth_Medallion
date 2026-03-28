{% macro is_incremental() %}
{#-- do not run introspective queries in parsing #}
{% if not execute %}
{{ return(False) }}
{% else %}
{% if model.config.load_historical %}
     {% do log("Loading Historical - Incremental", info=True) %}
    {% set relation = 'relation_exists' %}
    {{ return(True) }}
{% else %}

    {% set relation = adapter.get_relation(this.database, this.schema, this.table) %}
    {{ return(relation is not none
    and relation.type == 'table'
    and 'incremental' in model.config.materialized
    and not should_full_refresh()) }}
    {% endif %}
{% endif %}
{% endmacro %}