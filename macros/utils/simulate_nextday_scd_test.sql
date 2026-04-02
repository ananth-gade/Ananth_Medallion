-- Simulate next-day SCD2 test by backdating EFF_DT on target rows
-- Run: dbt run-operation simulate_nextday_scd_test

{% macro simulate_nextday_scd_test() %}

    -- Step 1: Reset the test records back to original values in source
    {% set reset_src %}
        update CDM.WORKFORCE.EMPLOYEE
        set JOB_CODE = 'DYX8',
            JOB_DESCRIPTION = 'Driver - CDL (B)',
            ADDRESS_LINE_1 = '3950 Mississippi Ave',
            CITY = 'Cahokia',
            CDM_UPDATE_TIMESTAMP = current_timestamp()
        where EMPLOYEE_EIN in ('100071494', '719074411')
    {% endset %}
    {% do run_query(reset_src) %}

    -- Step 2: Backdate the EFF_DT and INS_BATCH_ID in DIM to simulate "loaded yesterday"
    {% set backdate %}
        update CORE.SHARED_DIMS.DIM_EMPLOYEE
        set EFF_DT = '2026-04-01'::date,
            INS_BATCH_ID = 20260401120000
        where EMPLOYEE_EIN in ('100071494', '719074411')
    {% endset %}
    {% do run_query(backdate) %}
    {{ log("=== Step 1+2: Reset source & backdated DIM EFF_DT to 2026-04-01 for test EINs", info=True) }}

    -- Step 3: Now re-apply the SCD2 change (JOB_CODE) and SCD1 change (ADDRESS)
    {% set scd2_change %}
        update CDM.WORKFORCE.EMPLOYEE
        set JOB_CODE = 'TEST_SCD2',
            JOB_DESCRIPTION = 'SCD2 TEST JOB CHANGE',
            CDM_UPDATE_TIMESTAMP = current_timestamp()
        where EMPLOYEE_EIN = '100071494'
    {% endset %}
    {% do run_query(scd2_change) %}

    {% set scd1_change %}
        update CDM.WORKFORCE.EMPLOYEE
        set ADDRESS_LINE_1 = '999 SCD1 TEST STREET',
            CITY = 'TESTVILLE',
            CDM_UPDATE_TIMESTAMP = current_timestamp()
        where EMPLOYEE_EIN = '719074411'
    {% endset %}
    {% do run_query(scd1_change) %}
    {{ log("=== Step 3: Applied SCD2 (JOB_CODE) and SCD1 (ADDRESS) changes to source", info=True) }}

    -- Step 4: Verify setup
    {% set verify_dim %}
        select EMPLOYEE_EIN, JOB_CODE, ADDRESS_LINE_1, EFF_DT, INS_BATCH_ID
        from CORE.SHARED_DIMS.DIM_EMPLOYEE
        where EMPLOYEE_EIN in ('100071494', '719074411')
    {% endset %}
    {% set d = run_query(verify_dim) %}
    {{ log("=== DIM state (should show yesterday's EFF_DT with OLD values):", info=True) }}
    {% for row in d.rows %}
        {{ log("    DIM: EIN=" ~ row[0] ~ " | JOB=" ~ row[1] ~ " | ADDR=" ~ row[2] ~ " | EFF=" ~ row[3] ~ " | BATCH=" ~ row[4], info=True) }}
    {% endfor %}

    {% set verify_src %}
        select EMPLOYEE_EIN, JOB_CODE, ADDRESS_LINE_1, CDM_UPDATE_TIMESTAMP
        from CDM.WORKFORCE.EMPLOYEE
        where EMPLOYEE_EIN in ('100071494', '719074411')
    {% endset %}
    {% set s = run_query(verify_src) %}
    {{ log("=== SOURCE state (should show NEW values):", info=True) }}
    {% for row in s.rows %}
        {{ log("    SRC: EIN=" ~ row[0] ~ " | JOB=" ~ row[1] ~ " | ADDR=" ~ row[2] ~ " | UPDATED=" ~ row[3], info=True) }}
    {% endfor %}

    {{ log("", info=True) }}
    {{ log("=== Ready! Now run: dbt run --select DIM_EMPLOYEE", info=True) }}
    {{ log("=== SCD2 should create 2 rows for EIN 100071494 (old expired + new current)", info=True) }}
    {{ log("=== SCD1 should update in-place for EIN 719074411 (1 row, new address)", info=True) }}

{% endmacro %}
