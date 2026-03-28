{% macro purge_staging_duplicates_by_hash(database_name, schema_name, table_name, exclude_columns) %}
    -- exclude_columns is mandatory; allow comma-separated string
    {% if exclude_columns is string %}
        {% set exclude_columns = exclude_columns.split(',') %}
    {% endif %}

    -- Resolve relation using explicit database, schema, and table
    {% set target_relation = adapter.get_relation(
        database=database_name,
        schema=schema_name,
        identifier=table_name
    ) %}

    -- Get all columns from the table
    {% set all_columns = adapter.get_columns_in_relation(target_relation) %}

    -- List to store columns for deduplication
    {% set dedup_columns = [] %}

    -- Add columns that are not in exclude_columns
    {% for col in all_columns %}
        {% if col.name not in exclude_columns %}
            {% if col.name | lower == 'default' %}
                {% do dedup_columns.append('"DEFAULT"') %}  -- Escape reserved keyword
            {% else %}
                {% do dedup_columns.append(col.name) %}
            {% endif %}
        {% endif %}
    {% endfor %}

    -- Function to create hash key
    {% set default_null_value = 'NULL' %}
    {% set processed_columns = [] %}

    -- Process each column for null-safe concatenation
    {% for col in dedup_columns %}
        {% do processed_columns.append("coalesce(cast(" ~ col ~ " as " ~ dbt.type_string() ~ "), '" ~ default_null_value ~ "')") %}
    {% endfor %}

    {% set columns_hash_key = dbt.hash(dbt.concat(processed_columns)) %}

    {{ log("Deleting duplicates for " ~ table_name ~ "...") }}

    -- Perform deletion of duplicates using hash key
    delete from {{ target_relation }}
    where ({{ columns_hash_key }}{% for col in exclude_columns %}, {{ col }}{% endfor %}) in (
        select hash_key{% for col in exclude_columns %}, {{ col }}{% endfor %}
        from (
            select {{ columns_hash_key }} as hash_key{% for col in exclude_columns %}, {{ col }}{% endfor %},
                   rank() over (
                       partition by {{ columns_hash_key }} 
                       order by {% for col in exclude_columns %}{{ col }} desc{% if not loop.last %}, {% endif %}{% endfor %}
                   ) as rank
            from {{ target_relation }}
        ) t
        where t.rank > 1
    );
{% endmacro %}