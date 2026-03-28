{% macro dbt_snowflake_validate_get_incremental_strategy(config) %}
  {#-- Find and validate the incremental strategy #}
  {%- set strategy = config.get("incremental_strategy", default="merge") -%}
  {% do log("GETTTING STRATEGY================", info=True) %}
  {% do log(strategy, info=True) %}
  {% set invalid_strategy_msg -%}
    Invalid incremental strategy provided: {{ strategy }}
    Expected one of: 'merge', 'delete+insert', 'scd+merge'
  {%- endset %}
  {% if strategy not in ['merge', 'delete+insert','scd+merge'] %}
    {% do exceptions.raise_compiler_error(invalid_strategy_msg) %}
  {% endif %}
  {% do return(strategy) %}
{% endmacro %}
{% macro dbt_snowflake_get_incremental_sql(strategy, tmp_relation, target_relation, unique_key, dest_columns, timestamp_column,type6_columns) %}
  {% if strategy == 'merge' %}
    {% do return(get_merge_sql(target_relation, tmp_relation, unique_key, dest_columns)) %}
  {% elif strategy == 'delete+insert' %}
    {% do return(get_delete_insert_merge_sql(target_relation, tmp_relation, unique_key, dest_columns)) %}
  {% elif strategy == 'scd+merge' %}
    {% do log("SCD MERGE================", info=True) %}
    {% do return(get_scd_merge_sql(target_relation, tmp_relation, unique_key, dest_columns, none, timestamp_column,type6_columns))  %}
  {% else %}
    {% do exceptions.raise_compiler_error('invalid strategy: ' ~ strategy) %}
  {% endif %}
{% endmacro %}
{% materialization scd_incremental, adapter='snowflake' -%}
  {% do log("SCD INCREMENTAL================", info=True) %}
  {% set original_query_tag = set_query_tag() %}

  {%- set load_historical = config.get('load_historical') -%}
  {%- set unique_key = config.get('unique_key') -%}
  {%- set timestamp_column = config.get('timestamp_column') -%}
  {%- set full_refresh_mode = (should_full_refresh()) -%}
  {%- set type6_columns = config.get('type6_columns', default=[]) -%}

  {% do log(full_refresh_mode, info=True) %}

 -- setup
  {{ run_hooks(pre_hooks, inside_transaction=False) }}
  -- `BEGIN` happens here:
  {{ run_hooks(pre_hooks, inside_transaction=True) }}

  {% set target_relation = this %}

  {% set existing_relation = load_relation(this) %}
  {% do log(this, info=True) %}
  {% do log("Load Historical:", info=True) %}
  {% do log(load_historical, info=True) %}
  {% do log("Existing Relation:", info=True) %}
  {% do log(existing_relation, info=True) %}
  {% set tmp_relation = make_temp_relation(this) %}
  {#-- Validate early so we don't run SQL if the strategy is invalid --#}
  {% set strategy = dbt_snowflake_validate_get_incremental_strategy(config) -%}




  {% if existing_relation is none and load_historical == 'False'%}
    {% set build_sql = create_table_as(False, target_relation, sql) %}
  {% elif existing_relation.is_view %}
    {#-- Can't overwrite a view with a table - we must drop --#}
    {{ log("Dropping relation " ~ target_relation ~ " because it is a view and this model is a table.") }}
    {% do adapter.drop_relation(existing_relation) %}
    {% set build_sql = create_table_as(False, target_relation, sql) %}
  {% elif full_refresh_mode %}
    {% do log("Full Refresh Mode", info=True) %}
    {% set build_sql = create_table_as(False, target_relation, sql) %}
  {% else %}
    {% do log("About to create table", info=True) %}
    {% do run_query(create_table_as(True, tmp_relation, sql)) %}
    {% do adapter.expand_target_column_types(
           from_relation=tmp_relation,
           to_relation=target_relation) %}
    {% set dest_columns = adapter.get_columns_in_relation(target_relation) %}
    {% set build_sql = dbt_snowflake_get_incremental_sql(strategy, tmp_relation, target_relation, unique_key, dest_columns, timestamp_column,type6_columns) %}
  {% endif %}
  {%- call statement('main') -%}
    {{ build_sql }}
  {%- endcall -%}
  {{ run_hooks(post_hooks, inside_transaction=True) }}
  -- `COMMIT` happens here
  {{ adapter.commit() }}
  {{ run_hooks(post_hooks, inside_transaction=False) }}
  {% set target_relation = target_relation.incorporate(type='table') %}
  {% do persist_docs(target_relation, model) %}
  {% do unset_query_tag(original_query_tag) %}
  {{ return({'relations': [target_relation]}) }}
{%- endmaterialization %}
