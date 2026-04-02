-- Reset source test records to original values before full refresh
-- Run: dbt run-operation reset_test_source

{% macro reset_test_source() %}

    -- Reset EIN 100071494 to original values
    {% set reset_100071494 %}
        update CDM.WORKFORCE.EMPLOYEE
        set JOB_CODE = 'DYX8',
            JOB_DESCRIPTION = 'Driver - CDL (B)',
            CDM_UPDATE_TIMESTAMP = current_timestamp()
        where EMPLOYEE_EIN = '100071494'
    {% endset %}
    {% do run_query(reset_100071494) %}

    -- Reset EIN 719074411 to original values
    {% set reset_719074411 %}
        update CDM.WORKFORCE.EMPLOYEE
        set ADDRESS_LINE_1 = '3950 Mississippi Ave',
            CITY = 'Cahokia',
            CDM_UPDATE_TIMESTAMP = current_timestamp()
        where EMPLOYEE_EIN = '719074411'
    {% endset %}
    {% do run_query(reset_719074411) %}

    {{ log("=== Source records reset to original values", info=True) }}

    -- Verify
    {% set verify %}
        select EMPLOYEE_EIN, JOB_CODE, JOB_DESCRIPTION, ADDRESS_LINE_1, CITY
        from CDM.WORKFORCE.EMPLOYEE
        where EMPLOYEE_EIN in ('100071494', '719074411')
    {% endset %}
    {% set v = run_query(verify) %}
    {% for row in v.rows %}
        {{ log("    EIN=" ~ row[0] ~ " | JOB=" ~ row[1] ~ " | DESC=" ~ row[2] ~ " | ADDR=" ~ row[3] ~ ", " ~ row[4], info=True) }}
    {% endfor %}

{% endmacro %}
