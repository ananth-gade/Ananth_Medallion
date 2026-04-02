-- Validate SCD2 and SCD1 capture results
-- Run: dbt run-operation validate_scd_test

{% macro validate_scd_test() %}

    {{ log("", info=True) }}
    {{ log("========================================", info=True) }}
    {{ log("  SCD TYPE 2 TEST: EIN 100071494", info=True) }}
    {{ log("  Changed JOB_CODE -> TEST_SCD2", info=True) }}
    {{ log("  Expected: 2 rows (old expired + new current)", info=True) }}
    {{ log("========================================", info=True) }}

    {% set scd2_query %}
        select EMPLOYEE_EIN, JOB_CODE, JOB_DESCRIPTION, EFF_DT, DISC_DT, IS_CURRENT, HASH_SCD2, INS_BATCH_ID
        from CORE.SHARED_DIMS.DIM_EMPLOYEE
        where EMPLOYEE_EIN = '100071494'
        order by EFF_DT, IS_CURRENT
    {% endset %}
    {% set r2 = run_query(scd2_query) %}
    {{ log("  Row count: " ~ r2.rows | length, info=True) }}
    {% for row in r2.rows %}
        {{ log("  ROW " ~ loop.index ~ ": JOB=" ~ row[1] ~ " | DESC=" ~ row[2] ~ " | EFF=" ~ row[3] ~ " | DISC=" ~ row[4] ~ " | CURRENT=" ~ row[5] ~ " | HASH=" ~ row[6][:12] ~ "...", info=True) }}
    {% endfor %}

    {{ log("", info=True) }}
    {{ log("========================================", info=True) }}
    {{ log("  SCD TYPE 1 TEST: EIN 719074411", info=True) }}
    {{ log("  Changed ADDRESS -> 999 SCD1 TEST STREET", info=True) }}
    {{ log("  Expected: 1 row (in-place overwrite)", info=True) }}
    {{ log("========================================", info=True) }}

    {% set scd1_query %}
        select EMPLOYEE_EIN, ADDRESS_LINE_1, CITY, STATE, EFF_DT, DISC_DT, IS_CURRENT, HASH_SCD1, INS_BATCH_ID
        from CORE.SHARED_DIMS.DIM_EMPLOYEE
        where EMPLOYEE_EIN = '719074411'
        order by EFF_DT, IS_CURRENT
    {% endset %}
    {% set r1 = run_query(scd1_query) %}
    {{ log("  Row count: " ~ r1.rows | length, info=True) }}
    {% for row in r1.rows %}
        {{ log("  ROW " ~ loop.index ~ ": ADDR=" ~ row[1] ~ " | CITY=" ~ row[2] ~ " | ST=" ~ row[3] ~ " | EFF=" ~ row[4] ~ " | DISC=" ~ row[5] ~ " | CURRENT=" ~ row[6] ~ " | HASH=" ~ row[7][:12] ~ "...", info=True) }}
    {% endfor %}

    {{ log("", info=True) }}
    {{ log("========================================", info=True) }}
    {{ log("  OVERALL COUNTS AFTER INCREMENTAL", info=True) }}
    {{ log("========================================", info=True) }}

    {% set count_query %}
        select
            count(*) as total,
            count(case when IS_CURRENT = true then 1 end) as current_cnt,
            count(case when IS_CURRENT = false then 1 end) as historical_cnt
        from CORE.SHARED_DIMS.DIM_EMPLOYEE
    {% endset %}
    {% set c = run_query(count_query) %}
    {{ log("  Total rows:      " ~ c.columns[0][0], info=True) }}
    {{ log("  Current rows:    " ~ c.columns[1][0], info=True) }}
    {{ log("  Historical rows: " ~ c.columns[2][0], info=True) }}

{% endmacro %}
