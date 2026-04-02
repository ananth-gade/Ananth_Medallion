-- Verify initial load and pick test employees
-- Run: dbt run-operation verify_dim_employee

{% macro verify_dim_employee() %}

    {% set count_query %}
        select count(*) as cnt from CORE.SHARED_DIMS.DIM_EMPLOYEE
    {% endset %}
    {% set results = run_query(count_query) %}
    {{ log("=== TOTAL ROWS: " ~ results.columns[0][0], info=True) }}

    {% set hash_query %}
        select
            count(case when HASH_SCD2 is not null then 1 end) as scd2_cnt,
            count(case when HASH_SCD1 is not null then 1 end) as scd1_cnt,
            count(case when HASH_FULL is not null then 1 end) as full_cnt,
            count(case when DBT_HASH is not null then 1 end) as dbt_hash_cnt,
            count(case when IS_CURRENT = true then 1 end) as current_cnt
        from CORE.SHARED_DIMS.DIM_EMPLOYEE
    {% endset %}
    {% set r = run_query(hash_query) %}
    {{ log("=== HASH_SCD2 populated: " ~ r.columns[0][0], info=True) }}
    {{ log("=== HASH_SCD1 populated: " ~ r.columns[1][0], info=True) }}
    {{ log("=== HASH_FULL populated: " ~ r.columns[2][0], info=True) }}
    {{ log("=== DBT_HASH populated:  " ~ r.columns[3][0], info=True) }}
    {{ log("=== IS_CURRENT=true:     " ~ r.columns[4][0], info=True) }}

    {% set sample_query %}
        select EMPLOYEE_EIN, LAST_NAME, FIRST_NAME, JOB_CODE, ADDRESS_LINE_1, CITY, STATE
        from CORE.SHARED_DIMS.DIM_EMPLOYEE
        where IS_CURRENT = true and JOB_CODE is not null and ADDRESS_LINE_1 is not null
        limit 3
    {% endset %}
    {% set s = run_query(sample_query) %}
    {{ log("=== SAMPLE EMPLOYEES FOR TESTING:", info=True) }}
    {% for row in s.rows %}
        {{ log("    EIN=" ~ row[0] ~ " | " ~ row[2] ~ " " ~ row[1] ~ " | JOB=" ~ row[3] ~ " | ADDR=" ~ row[4] ~ ", " ~ row[5] ~ " " ~ row[6], info=True) }}
    {% endfor %}

{% endmacro %}
