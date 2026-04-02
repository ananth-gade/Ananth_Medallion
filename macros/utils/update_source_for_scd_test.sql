-- Update source EMPLOYEE records to test SCD Type 2 and Type 1 changes
-- Run: dbt run-operation update_source_for_scd_test
-- Source: CDM.WORKFORCE.EMPLOYEE | Target: CORE.SHARED_DIMS.DIM_EMPLOYEE

{% macro update_source_for_scd_test() %}

    -- SCD2 test: Change JOB_CODE on EIN 100071494 (triggers new historical row)
    {% set scd2_update %}
        update CDM.WORKFORCE.EMPLOYEE
        set JOB_CODE = 'TEST_SCD2',
            JOB_DESCRIPTION = 'SCD2 TEST JOB CHANGE',
            CDM_UPDATE_TIMESTAMP = current_timestamp()
        where EMPLOYEE_EIN = '100071494'
    {% endset %}
    {% do run_query(scd2_update) %}
    {{ log("=== SCD2 TEST: Updated JOB_CODE to 'TEST_SCD2' for EIN 100071494", info=True) }}

    -- SCD1 test: Change ADDRESS_LINE_1 on EIN 719074411 (triggers in-place overwrite)
    {% set scd1_update %}
        update CDM.WORKFORCE.EMPLOYEE
        set ADDRESS_LINE_1 = '999 SCD1 TEST STREET',
            CITY = 'TESTVILLE',
            CDM_UPDATE_TIMESTAMP = current_timestamp()
        where EMPLOYEE_EIN = '719074411'
    {% endset %}
    {% do run_query(scd1_update) %}
    {{ log("=== SCD1 TEST: Updated ADDRESS to '999 SCD1 TEST STREET, TESTVILLE' for EIN 719074411", info=True) }}

    -- Verify source changes
    {% set verify %}
        select EMPLOYEE_EIN, JOB_CODE, ADDRESS_LINE_1, CITY, CDM_UPDATE_TIMESTAMP
        from CDM.WORKFORCE.EMPLOYEE
        where EMPLOYEE_EIN in ('100071494', '719074411')
    {% endset %}
    {% set v = run_query(verify) %}
    {{ log("=== SOURCE VERIFICATION:", info=True) }}
    {% for row in v.rows %}
        {{ log("    EIN=" ~ row[0] ~ " | JOB=" ~ row[1] ~ " | ADDR=" ~ row[2] ~ ", " ~ row[3] ~ " | UPDATED=" ~ row[4], info=True) }}
    {% endfor %}

{% endmacro %}
