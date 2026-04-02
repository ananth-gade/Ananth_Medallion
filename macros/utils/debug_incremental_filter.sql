-- Debug incremental filter
-- Run: dbt run-operation debug_incremental_filter

{% macro debug_incremental_filter() %}

    -- 1. Max CDM_UPDATE_TIMESTAMP in DIM target
    {% set dim_max %}
        select max(CDM_UPDATE_TIMESTAMP) from CORE.SHARED_DIMS.DIM_EMPLOYEE
    {% endset %}
    {% set r1 = run_query(dim_max) %}
    {{ log("=== DIM max CDM_UPDATE_TIMESTAMP: " ~ r1.columns[0][0], info=True) }}

    -- 2. CDM_UPDATE_TIMESTAMP of our test records in source
    {% set src_ts %}
        select EMPLOYEE_EIN, CDM_UPDATE_TIMESTAMP
        from CDM.WORKFORCE.EMPLOYEE
        where EMPLOYEE_EIN in ('100071494', '719074411')
    {% endset %}
    {% set r2 = run_query(src_ts) %}
    {% for row in r2.rows %}
        {{ log("=== SOURCE EIN " ~ row[0] ~ " CDM_UPDATE_TIMESTAMP: " ~ row[1], info=True) }}
    {% endfor %}

    -- 3. Would the filter include them?
    {% set filter_test %}
        select EMPLOYEE_EIN, CDM_UPDATE_TIMESTAMP
        from CDM.WORKFORCE.EMPLOYEE
        where CDM_UPDATE_TIMESTAMP >= (
            select max(CDM_UPDATE_TIMESTAMP) - interval '3 minutes'
            from CORE.SHARED_DIMS.DIM_EMPLOYEE
        )
    {% endset %}
    {% set r3 = run_query(filter_test) %}
    {{ log("=== Records passing incremental filter: " ~ r3.rows | length, info=True) }}
    {% for row in r3.rows %}
        {{ log("    EIN=" ~ row[0] ~ " | TS=" ~ row[1], info=True) }}
    {% endfor %}

{% endmacro %}
