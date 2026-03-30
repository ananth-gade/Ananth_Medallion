{# ============================================================================
   Macro: capture_run_results
   Description: Captures dbt run results and inserts them into an audit table
   Target: AUDIT.DATA_TRANSFORMATION.dbt_run_results
   
   Features:
   - Captures execution metadata (timing, status, query_id, etc.)
   - Optionally extracts DML counts via RESULT_SCAN
   - Batches inserts (100 rows per batch) to avoid query size limits
   
   Configuration:
   - scan_result_for_dml_counts: Enable/disable RESULT_SCAN (default: true)
   - dml_supported_materializations: List of materializations for DML scanning
============================================================================ #}

{% macro capture_run_results() %}
    {% if execute %}
        
        {# ==================== Skip if audit is disabled via vars ==================== #}
        {% if var('skip_audit', false) %}
            {{ log("Skipping capture_run_results (skip_audit=true)", info=true) }}
            {{ return('') }}
        {% endif %}

        {# ==================== Configuration ==================== #}
        {% set audit_database = 'AUDIT' %}
        {% set audit_schema = 'DATA_TRANSFORMATION' %}
        {% set target_table = audit_database ~ '.' ~ audit_schema ~ '.dbt_run_results' %}
        {% set all_rows = [] %}
        
        {# ==================== Process Each Result ==================== #}
        {% for result in results %}
            {% if result.node %}
                {% set node = result.node %}
                {% set materialization = (node.config.get('materialized', '') | replace("'", "''")) if node.config else '' %}
                
                {# ==================== Extract Timing Information ==================== #}
                {% set timing_ns = namespace(
                    execute_started_at='NULL',
                    execute_completed_at='NULL',
                    compile_started_at='NULL',
                    compile_completed_at='NULL'
                ) %}
                
                {% if result.timing %}
                    {% for timing in result.timing %}
                        {% if timing.name == 'execute' %}
                            {% set timing_ns.execute_started_at = "TO_TIMESTAMP_NTZ('" ~ (timing.started_at | string | replace("'", "''")) ~ "')" if timing.started_at else 'NULL' %}
                            {% set timing_ns.execute_completed_at = "TO_TIMESTAMP_NTZ('" ~ (timing.completed_at | string | replace("'", "''")) ~ "')" if timing.completed_at else 'NULL' %}
                        {% elif timing.name == 'compile' %}
                            {% set timing_ns.compile_started_at = "TO_TIMESTAMP_NTZ('" ~ (timing.started_at | string | replace("'", "''")) ~ "')" if timing.started_at else 'NULL' %}
                            {% set timing_ns.compile_completed_at = "TO_TIMESTAMP_NTZ('" ~ (timing.completed_at | string | replace("'", "''")) ~ "')" if timing.completed_at else 'NULL' %}
                        {% endif %}
                    {% endfor %}
                {% endif %}
                
                {# ==================== Generate IDs and Initialize DML Variables ==================== #}
                {% set model_execution_id = (invocation_id ~ '.' ~ node.unique_id) | replace("'", "''") %}
                {% set query_id = none %}
                {% set rows_affected = 'NULL' %}
                {% set dml_counts = namespace(rows_inserted='NULL', rows_updated='NULL', rows_deleted='NULL') %}
                
                {# ==================== Extract query_id from Adapter Response (works for both success and failure) ==================== #}
                {# Try to extract query_id from adapter_response regardless of execution status #}
                {% if result.adapter_response %}
                    {% if result.adapter_response is mapping %}
                        {% set raw_query_id = result.adapter_response.get('query_id', none) %}
                        {% set query_id = (raw_query_id | replace("'", "''")) if raw_query_id else none %}
                        {% if query_id %}
                            {% do log("Captured query_id=" ~ query_id ~ " for " ~ node.name ~ " (status=" ~ result.status ~ ")", info=True) %}
                        {% else %}
                            {% do log("No query_id in adapter_response for " ~ node.name ~ " (status=" ~ result.status ~ ")", info=True) %}
                        {% endif %}
                    {% else %}
                        {% do log("adapter_response is not a mapping for " ~ node.name ~ " (status=" ~ result.status ~ ")", info=True) %}
                    {% endif %}
                {% else %}
                    {% do log("No adapter_response available for " ~ node.name ~ " (status=" ~ result.status ~ ", likely compile-time error)", info=True) %}
                {% endif %}
                
                {# ==================== Extract Adapter Response Metadata ==================== #}
                {% if result.adapter_response and result.adapter_response is mapping %}
                    {% set raw_rows_affected = result.adapter_response.get('rows_affected', none) %}
                    {% set rows_affected = raw_rows_affected if raw_rows_affected is not none else 'NULL' %}
                    
                    {# ==================== RESULT_SCAN for Detailed DML Counts ==================== #}
                    {# Performance Note: RESULT_SCAN executes 2 additional queries per eligible model.
                       To disable: Set scan_result_for_dml_counts: false in dbt_project.yml or model config #}
                    
                    {% set resource_type_upper = (node.resource_type | string | upper) if node.resource_type else '' %}
                    {% set materialization_upper = (materialization | string | upper) if materialization else '' %}
                    {% set status_upper = (result.status | string | upper) if result.status else '' %}
                    {% set scan_dml_enabled = node.config.get('scan_result_for_dml_counts', var('scan_result_for_dml_counts', true)) %}
                    {% set is_dml_eligible = (resource_type_upper in ['MODEL', 'SNAPSHOT'] and status_upper == 'SUCCESS') %}
                    {% set dml_supported_materializations = var('dml_supported_materializations', ['TABLE', 'INCREMENTAL', 'DYNAMIC_TABLE']) %}
                    {% set dml_supported_materializations_upper = dml_supported_materializations | map('upper') | list %}
                    {% set materialization_supports_dml = materialization_upper in dml_supported_materializations_upper %}
                    {% set should_attempt_result_scan = scan_dml_enabled and query_id and is_dml_eligible and materialization_supports_dml %}
                    
                    {% if should_attempt_result_scan %}
                        {# Query RESULT_SCAN directly to get DML data with error handling #}
                        {% set get_scan_data_sql %}
                            SELECT *
                            FROM TABLE(RESULT_SCAN('{{ query_id }}'))
                            LIMIT 1
                        {% endset %}

                        {% set dml_results = none %}
                        
                        {# Execute with error handling - failures should not break audit capture #}
                        {% if execute %}
                            {% set _tmp_results = run_query(get_scan_data_sql) %}
                            {# Validate structure before using RESULT_SCAN output #}
                            {% if _tmp_results is not none and _tmp_results.rows is defined and _tmp_results.columns is defined and _tmp_results.rows | length > 0 %}
                                {% set dml_results = _tmp_results %}
                            {% else %}
                                {% do log("RESULT_SCAN did not return usable data for query_id=" ~ query_id ~ "; skipping DML detail capture.", info=True) %}
                                {% set dml_results = none %}
                            {% endif %}
                        {% endif %}
                        
                        {# Extract DML values by finding column positions #}
                        {% if dml_results is not none and dml_results.rows is defined and dml_results.rows | length > 0 and dml_results.columns is defined %}
                            {% set first_row = dml_results.rows[0] %}
                            {% set columns = dml_results.columns %}
                            
                            {# Validate we have both rows and columns before accessing #}
                            {% if first_row is not none and columns | length > 0 %}
                                {# Check what type of result we have - use namespace for proper scoping #}
                                {% set col_flags = namespace(has_status=false, has_dml=false) %}
                                
                                {% for col in columns %}
                                    {% set col_lower = col.name | lower %}
                                    {% if col_lower == 'status' %}
                                        {% set col_flags.has_status = true %}
                                    {% endif %}
                                    {% if col_lower in ['number of rows inserted', 'number of rows updated', 'number of rows deleted'] %}
                                        {% set col_flags.has_dml = true %}
                                    {% endif %}
                                {% endfor %}
                                
                                {% if col_flags.has_dml %}
                                    {# Extract DML counts from RESULT_SCAN #}
                                    {% for i in range(columns | length) %}
                                        {% set col_name = columns[i].name | lower %}
                                        {% if col_name == 'number of rows inserted' and first_row[i] is not none %}
                                            {% set dml_counts.rows_inserted = first_row[i] %}
                                        {% elif col_name == 'number of rows updated' and first_row[i] is not none %}
                                            {% set dml_counts.rows_updated = first_row[i] %}
                                        {% elif col_name == 'number of rows deleted' and first_row[i] is not none %}
                                            {% set dml_counts.rows_deleted = first_row[i] %}
                                        {% endif %}
                                    {% endfor %}
                                {% elif col_flags.has_status %}
                                    {# Check if this is a table creation #}
                                    {% set status_message = first_row[0] | string | lower %}
                                    {% if 'successfully created' in status_message %}
                                        {# Query table for row count #}
                                        {% set relation = adapter.get_relation(database=node.database, schema=node.schema, identifier=node.name) %}
                                        {% if relation %}
                                            {% set count_query %}
                                                SELECT COUNT(*) as row_count FROM {{ relation }}
                                            {% endset %}
                                            {% set count_result = run_query(count_query) %}
                                            {% if count_result and count_result.rows and count_result.rows | length > 0 %}
                                                {% set dml_counts.rows_inserted = count_result.rows[0][0] %}
                                            {% endif %}
                                        {% endif %}
                                    {% endif %}
                                {% endif %}
                            {% endif %}
                        {% endif %}
                    {% endif %}
                {% endif %}
                
                {# ==================== Prepare Field Values ==================== #}
                {# Escape compiled_code (no truncation - column supports up to 16MB) #}
                {% set compiled_code = '' %}
                {% if node.compiled_code %}
                    {% set compiled_code = node.compiled_code | replace("'", "''") %}
                {% endif %}
                
                {# Escape string fields to prevent SQL injection #}
                {% set invocation_id_escaped = invocation_id | replace("'", "''") if invocation_id else '' %}
                {% set name_escaped = node.name | replace("'", "''") if node.name else '' %}
                {% set unique_id_escaped = node.unique_id | replace("'", "''") if node.unique_id else '' %}
                {% set resource_type_escaped = node.resource_type | replace("'", "''") if node.resource_type else '' %}
                {% set status_escaped = result.status | lower | replace("'", "''") if result.status else '' %}
                {% set thread_id_escaped = result.thread_id | replace("'", "''") if result.thread_id else '' %}
                {% set message = result.message | replace("'", "''") if result.message else '' %}
                
                {# Convert adapter_response to JSON string (no truncation - column supports up to 16MB) #}
                {% set adapter_response_json = '{}' %}
                {% if result.adapter_response %}
                    {% set adapter_response_json = tojson(result.adapter_response) | replace("'", "''") %}
                {% endif %}
                
                {# Extract additional metadata #}
                {% set failures = result.failures if result.failures is not none else 'NULL' %}
                {% set generated_at = modules.datetime.datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.%f') | replace("'", "''") %}
                
                {# ==================== Build INSERT Row ==================== #}
                
                {%- set row_sql -%}
(
'{{ model_execution_id }}',
'{{ unique_id_escaped }}',
'{{ invocation_id_escaped }}',
TO_TIMESTAMP_NTZ('{{ generated_at }}'),
CURRENT_TIMESTAMP()::TIMESTAMP_NTZ,
'{{ name_escaped }}',
'{{ message }}',
'{{ status_escaped }}',
'{{ resource_type_escaped }}',
{{ result.execution_time if result.execution_time else 'NULL' }},
{{ timing_ns.execute_started_at }},
{{ timing_ns.execute_completed_at }},
{{ timing_ns.compile_started_at }},
{{ timing_ns.compile_completed_at }},
{{ rows_affected }},
{{ dml_counts.rows_inserted }},
{{ dml_counts.rows_updated }},
{{ dml_counts.rows_deleted }},
{{ 'TRUE' if flags.FULL_REFRESH else 'FALSE' }},
'{{ compiled_code }}',
{{ failures }},
{{ "'" ~ query_id ~ "'" if query_id else 'NULL' }},
'{{ thread_id_escaped }}',
'{{ materialization }}',
'{{ adapter_response_json }}'
)
                {%- endset %}
                
                {% do all_rows.append(row_sql) %}
            {% endif %}
        {% endfor %}
        
        {# ==================== Execute Batched INSERT Statements ==================== #}
        {% if all_rows | length > 0 %}
            {% do log("Capturing " ~ all_rows | length ~ " run results to " ~ target_table, info=True) %}
            
            {# Delete records older than 60 days for data retention #}
            {% set cleanup_sql -%}
                DELETE FROM {{ target_table }}
                WHERE created_at < DATEADD(day, -60, CURRENT_TIMESTAMP());
            {%- endset %}
            
            {% do log("Deleting audit records older than 60 days from " ~ target_table, info=True) %}
            
            {# Split into batches to avoid query size limits (100 rows per batch) #}
            {% set batch_size = 100 %}
            {% set total_rows = all_rows | length %}
            {% set num_batches = ((total_rows + batch_size - 1) / batch_size) | int %}
            
            {% set all_inserts = [] %}
            {% do all_inserts.append(cleanup_sql) %}
            {% for batch_num in range(num_batches) %}
                {% set start_idx = batch_num * batch_size %}
                {% set end_idx = [start_idx + batch_size, total_rows] | min %}
                {% set batch_values = all_rows[start_idx:end_idx] %}
                
                {% set insert_sql -%}
INSERT INTO {{ target_table }} (
    model_execution_id,
    unique_id,
    invocation_id,
    generated_at,
    created_at,
    name,
    message,
    status,
    resource_type,
    execution_time,
    execute_started_at,
    execute_completed_at,
    compile_started_at,
    compile_completed_at,
    rows_affected,
    rows_inserted,
    rows_updated,
    rows_deleted,
    full_refresh,
    compiled_code,
    failures,
    query_id,
    thread_id,
    materialization,
    adapter_response
)
VALUES
{{ batch_values | join(',\n') }};
                {%- endset %}
                
                {% do all_inserts.append(insert_sql) %}
            {% endfor %}
            
            {{ return(all_inserts | join('\n')) }}
        {% else %}
            {% do log("No run results to capture", info=True) %}
            {{ return('') }}
        {% endif %}
        
    {% else %}
        {{ return('') }}
    {% endif %}
{% endmacro %}