{% macro get_scd_merge_sql(target, source, unique_key, dest_columns, timestamp_column, predicates=none, type6_columns=none) -%}
    {{ adapter.dispatch('get_scd_merge_sql')(target, source, unique_key, dest_columns, timestamp_column, predicates, type6_columns) }}
{%- endmacro %}

{% macro default__get_scd_merge_sql(target, source, unique_key, dest_columns, predicates,  timestamp_column, type6_columns=none) -%}
    -- Type 6: Find which type6 columns have changed (source vs. target where IS_CURRENT=1) using a single query
    {% set changed_type6_columns = [] %}
    {% if type6_columns and type6_columns|length > 0 %}
        {% set compare_sql %}
            select
            {%- for col in type6_columns %}
                max(case when COALESCE(src.{{ col }},'') <> COALESCE(tgt.{{ col }},'') then 1 else 0 end) as {{ col }}_changed{% if not loop.last %}, {% endif %}
            {%- endfor %}
            from {{ source }} src
            join {{ target }} tgt
              on src.{{ unique_key }} = tgt.{{ unique_key }}
            where tgt.IS_CURRENT = 1
        {% endset %}
        {% set result = run_query(compare_sql) %}
        {% if result %}
            {% for col in type6_columns %}
                {% if result.columns[loop.index0].values()[0]|int > 0 %}
                    {% do changed_type6_columns.append(col) %}
                {% endif %}
            {% endfor %}
        {% endif %}
        -- changed_type6_columns: {{ changed_type6_columns | join(', ') }}
    {% endif %}
    {%- set predicates = [] if predicates is none else [] + predicates -%}
    {%- set dest_cols_csv = get_quoted_csv(dest_columns | map(attribute="name")) -%}
    {%- set update_columns = config.get('merge_update_columns', default = dest_columns | map(attribute="quoted") | list) -%}
    {%- set sql_header = config.get('sql_header', none) -%}

    {% if unique_key %}
        {% set unique_key_match %}
            DBT_INTERNAL_SOURCE.{{ unique_key }} = DBT_INTERNAL_DEST.{{ unique_key }}
        {% endset %}
        {% do predicates.append(unique_key_match) %}
    {% else %}
        {% do predicates.append('FALSE') %}
    {% endif %}
    {{ sql_header if sql_header is not none }}
    merge into {{ target }} as DBT_INTERNAL_DEST
        using  {{ source }} as DBT_INTERNAL_SOURCE
        on {{ predicates | join(' and ') }} --> Key = key (source - surrogate key on ODS)
        AND DBT_INTERNAL_DEST.DISC_DT = to_date('9999-12-31') --> to current record in the CORE
        AND to_date(to_varchar(substring(DBT_INTERNAL_SOURCE.{{ timestamp_column }},1,8)),'yyyymmdd') <= to_date(DBT_INTERNAL_DEST.EFF_DT) --> To determine intra-day or not
    {% if unique_key %}
    WHEN MATCHED
    AND (to_date(to_varchar(substring(DBT_INTERNAL_SOURCE.{{ timestamp_column }},1,8)),'yyyymmdd') = to_date(DBT_INTERNAL_DEST.EFF_DT) --> is this intra-day ?
    OR TO_NUMBER(TO_VARCHAR(CURRENT_TIMESTAMP, 'YYYYMMDD')) = substring(DBT_INTERNAL_DEST.INS_BATCH_ID,1,8))
    AND DBT_INTERNAL_DEST.HASH_SCD2 <> DBT_INTERNAL_SOURCE.HASH_SCD2 --> new values in the fields camparsion
    THEN UPDATE SET
        {% for column_name in update_columns -%}           
                {% do log(column_name, info=True) %}
                {{ column_name }} = DBT_INTERNAL_SOURCE.{{ column_name }}
                {%- if not loop.last %}, {%- endif %}
        {%- endfor %}


    when not matched AND DBT_INTERNAL_SOURCE.{{ unique_key }} NOT IN 
    (SELECT {{ unique_key }} FROM {{target}} DBT_INTERNAL_DEST where DBT_INTERNAL_DEST.{{ unique_key }} =  DBT_INTERNAL_SOURCE.{{ unique_key }})
    then insert
        ({{ dest_cols_csv }})
    values
        ({{ dest_cols_csv | replace('"EFF_DT"', "'1900-01-01'") }})

    when not matched AND DBT_INTERNAL_SOURCE.HASH_SCD2 NOT IN 
    (SELECT HASH_SCD2 FROM {{target}} DBT_INTERNAL_DEST 
    WHERE DBT_INTERNAL_DEST.{{ unique_key }}  = DBT_INTERNAL_SOURCE.{{ unique_key }}
    and DBT_INTERNAL_DEST.DISC_DT = to_date('9999-12-31'))
    then insert
        ({{ dest_cols_csv }})
    values
        ({{ dest_cols_csv }})

    {% endif %};



    -- Type 2: Close out current record
    UPDATE {{ target }} DBT_INTERNAL_DEST
    SET
    "DISC_DT" = CURRENT_DATE - 1,
    "IS_CURRENT" = FALSE,
    "UPD_BATCH_ID" = TO_NUMBER(TO_VARCHAR(CURRENT_TIMESTAMP, 'YYYYMMDDHH24MISSFF3'))
    FROM (
        SELECT
            HASH_SCD2,
            {{ unique_key }} 
        FROM
            {{ source }}
    ) DBT_INTERNAL_SOURCE
    where DBT_INTERNAL_DEST.{{ unique_key }}  = DBT_INTERNAL_SOURCE.{{ unique_key }}  
      and DBT_INTERNAL_DEST.HASH_SCD2 <> DBT_INTERNAL_SOURCE.HASH_SCD2 
      and DBT_INTERNAL_DEST.DISC_DT = to_date('9999-12-31');

    -- Type 1: In-place overwrite of current records where only SCD1 columns changed
    -- Activates when HASH_SCD2 matches but HASH_SCD1 differs
    {%- set dest_col_names = dest_columns | map(attribute='name') | list -%}
    {%- set has_hash_scd1 = 'HASH_SCD1' in dest_col_names -%}
    {% if has_hash_scd1 %}
    UPDATE {{ target }} DBT_INTERNAL_DEST
    SET
        {% for column_name in update_columns -%}
            {{ column_name }} = DBT_INTERNAL_SOURCE.{{ column_name }}
            {%- if not loop.last %}, {%- endif %}
        {%- endfor %}
    FROM (
        SELECT * FROM {{ source }}
    ) DBT_INTERNAL_SOURCE
    WHERE DBT_INTERNAL_DEST.{{ unique_key }} = DBT_INTERNAL_SOURCE.{{ unique_key }}
      AND DBT_INTERNAL_DEST.DISC_DT = to_date('9999-12-31')
      AND DBT_INTERNAL_DEST."HASH_SCD2" = DBT_INTERNAL_SOURCE."HASH_SCD2"
      AND DBT_INTERNAL_DEST."HASH_SCD1" <> DBT_INTERNAL_SOURCE."HASH_SCD1";
    {% endif %}

    -- Type 6: Update only changed type6 columns for all records in target
    {% if changed_type6_columns and changed_type6_columns|length > 0 %}
        UPDATE {{ target }} hist
        SET
        {%- for col in changed_type6_columns %}
            {{ col }} = curr.{{ col }}{% if not loop.last %}, {% endif %}
        {%- endfor %}
        FROM (
            SELECT {{ unique_key }},
            {%- for col in changed_type6_columns %}
                {{ col }}{% if not loop.last %}, {% endif %}
            {%- endfor %}
            FROM {{ target }}
            WHERE DISC_DT = to_date('9999-12-31')
        ) curr
        WHERE hist.{{ unique_key }} = curr.{{ unique_key }}
          AND hist.DISC_DT <> to_date('9999-12-31');
    {% endif %}





{% endmacro %}

{% macro default__get_merge_sql(target, source, unique_key, dest_columns, predicates) -%}
    {%- set predicates = [] if predicates is none else [] + predicates -%}
    {%- set dest_cols_csv = get_quoted_csv(dest_columns | map(attribute="name")) -%}
    {%- set update_columns = config.get('merge_update_columns', default = dest_columns | map(attribute="quoted") | list) -%}
    {%- set sql_header = config.get('sql_header', none) -%}
    {%- set no_update_columns = config.get('merge_no_update_columns', default = []) -%}
    {%- set merge_condition = config.get('merge_condition', none) -%}
    {%- set dest_column_names_upper = dest_columns | map(attribute='name') | map('upper') | list -%}
    {%- set has_operation_type = 'OPERATION_TYPE' in dest_column_names_upper -%}


    {% if unique_key %}
        {% set unique_key_match %}
            DBT_INTERNAL_SOURCE.{{ unique_key }} = DBT_INTERNAL_DEST.{{ unique_key }}
        {% endset %}
        {% do predicates.append(unique_key_match) %}
    {% else %}
        {% do predicates.append('FALSE') %}
    {% endif %}

    {{ sql_header if sql_header is not none }}

    merge into {{ target }} as DBT_INTERNAL_DEST
        using {{ source }} as DBT_INTERNAL_SOURCE
        on {{ predicates | join(' and ') }}

    {% if unique_key %}
    when matched 
    {%- if merge_condition %} 
        and {{ merge_condition }}
    {%- endif %} 
    then update set
        {% if config.get('merge_update_columns') %}
            {%- for column_name in update_columns -%}
                {{ column_name }} = DBT_INTERNAL_SOURCE.{{ column_name }}{%- if not loop.last or has_operation_type %}, {% endif %}
            {%- endfor %}
            {% if has_operation_type %}
                "OPERATION_TYPE" = CASE WHEN DBT_INTERNAL_SOURCE.OPERATION_TYPE = 'DELETE' THEN 'DELETE' ELSE 'UPDATE' END
            {% endif %}
        {% else %}
            {%- set updatable_columns = [] -%}
            {%- for column in dest_columns -%}
                {%- if column.name not in no_update_columns and column.name != 'OPERATION_TYPE' -%}
                    {%- do updatable_columns.append(column) -%}
                {%- endif -%}
            {%- endfor -%}
            {%- for column in updatable_columns -%}
                {{ adapter.quote(column.name) }} = DBT_INTERNAL_SOURCE.{{ adapter.quote(column.name) }}{%- if not loop.last or has_operation_type %}, {% endif %}
            {%- endfor %}
            {% if has_operation_type %}
                "OPERATION_TYPE" = CASE WHEN DBT_INTERNAL_SOURCE.OPERATION_TYPE = 'DELETE' THEN 'DELETE' ELSE 'UPDATE' END
            {% endif %}
        {% endif %}
    {% endif %}

    when not matched then insert
        ({{ dest_cols_csv }})
    values
        ({{ dest_cols_csv }})

{% endmacro %}