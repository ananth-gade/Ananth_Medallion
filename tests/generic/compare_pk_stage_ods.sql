{% test compare_pk_stage_ods(stage_table, target_table, stage_pk_column, ods_pk_column, column_name, model) %}
    
    with distinct_a as (
        select 
        {{ stage_pk_column | join(', ') }} as stage_pk_column
        from {{ stage_table }}
        group by  {{ stage_pk_column | join(', ') }} 
    ),
    distinct_b as (
        select
        {{ ods_pk_column | join(', ') }} as ods_pk_column
        from {{ target_table }}
        group by  {{ ods_pk_column | join(', ') }} 
    ),

    a_not_in_b as (
        select
        stage_pk_column
        from distinct_a
        where stage_pk_column not in (select ods_pk_column from distinct_b)
    )
    
    select
        count(*) as count
    from a_not_in_b

{% endtest %}
