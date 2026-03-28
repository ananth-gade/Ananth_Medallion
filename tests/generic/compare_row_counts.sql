{% test compare_row_counts(stage_table, target_table,stage_pk_column, ods_pk_column, timestamp_column,column_name, model) %}
    
    WITH deduped_source AS (
        SELECT
            {{ stage_pk_column | join(', ') }},
            ROW_NUMBER() OVER (PARTITION BY  {{ stage_pk_column | join(', ') }} ORDER BY {{timestamp_column}} DESC) AS rn
        FROM {{ stage_table }}
    ),

    source_count AS (
        SELECT COUNT(*) AS source_count
        FROM deduped_source
        WHERE rn = 1
    ),
    
    target_count AS (
        SELECT COUNT(*) AS target_count
        FROM {{ target_table }}
    ),

    count_diff AS (
        SELECT
            CASE WHEN source_count != target_count THEN 1 ELSE 0 END AS final_count
        FROM source_count
        LEFT JOIN target_count ON 1=1
    )
    
    SELECT SUM(final_count) AS count
    FROM count_diff

{% endtest %}
