{% macro get_last_query_id() %}
    {% if execute%}
        {% set query %}
                with 
            query_history as (
            select
                *,
                regexp_substr(query_text, '/\\*\\s({"app":\\s"dbt".*})\\s\\*/', 
1, 1, 'ie') as _dbt_json_meta,
                try_parse_json(_dbt_json_meta) as dbt_metadata
            from table(information_schema.query_history())
            where 
            database_name = '{{ this.database }}' and
            query_tag <>'' and 
            query_type not in ('COMMIT','ALTER_SESSION','SELECT','INSERT','DROP')
            and query_text not like '%dbt_audit_log%'
            order by start_time desc 
            )
            , final as (
            select  start_time, end_time,
            query_id, query_text, query_type, query_tag,
            dbt_metadata:invocation_id::string as invocation_id
            from query_history 
            where _dbt_json_meta is not null
            )
            select  a.query_id
            from final a 
            limit 1   
        {% endset %}
        {% set query_id = run_query(query).columns[0][0] %}
        {% do return(query_id) %}
    {% endif %}
{% endmacro %}
