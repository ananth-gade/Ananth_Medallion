-- Proper next-day SCD test setup
-- Run: dbt run-operation setup_nextday_test

{% macro setup_nextday_test() %}

    -- Step 1: Reset DIM records to original values AND backdate to yesterday
    {% set reset_dim_scd2 %}
        update CORE.SHARED_DIMS.DIM_EMPLOYEE
        set JOB_CODE = 'DYX8',
            JOB_DESCRIPTION = 'Driver - CDL (B)',
            EFF_DT = '2026-04-01'::date,
            INS_BATCH_ID = 20260401120000
        where EMPLOYEE_EIN = '100071494'
    {% endset %}
    {% do run_query(reset_dim_scd2) %}

    {% set reset_dim_scd1 %}
        update CORE.SHARED_DIMS.DIM_EMPLOYEE
        set ADDRESS_LINE_1 = '3950 Mississippi Ave',
            CITY = 'Cahokia',
            EFF_DT = '2026-04-01'::date,
            INS_BATCH_ID = 20260401120000
        where EMPLOYEE_EIN = '719074411'
    {% endset %}
    {% do run_query(reset_dim_scd1) %}
    {{ log("=== Step 1: Reset DIM records to original values + backdated EFF_DT to 2026-04-01", info=True) }}

    -- Step 2: Also need to recalculate the DBT_HASH in DIM to match old values
    -- The scd+merge engine compares DBT_HASH. Since we reset the DIM values,
    -- the hash in DIM is stale. We need the merge to see a hash MISMATCH.
    -- The model will recompute hashes from source, so we just need source to have new values.

    -- Step 3: Set source to have CHANGED values (SCD2: new JOB_CODE, SCD1: new ADDRESS)
    {% set change_src_scd2 %}
        update CDM.WORKFORCE.EMPLOYEE
        set JOB_CODE = 'NEWSCD2',
            JOB_DESCRIPTION = 'NEW SCD2 JOB TITLE',
            CDM_UPDATE_TIMESTAMP = current_timestamp()
        where EMPLOYEE_EIN = '100071494'
    {% endset %}
    {% do run_query(change_src_scd2) %}

    {% set change_src_scd1 %}
        update CDM.WORKFORCE.EMPLOYEE
        set ADDRESS_LINE_1 = '777 TYPE1 BLVD',
            CITY = 'NEWCITY',
            CDM_UPDATE_TIMESTAMP = current_timestamp()
        where EMPLOYEE_EIN = '719074411'
    {% endset %}
    {% do run_query(change_src_scd1) %}
    {{ log("=== Step 2: Source updated with new values (SCD2: JOB=NEWSCD2, SCD1: ADDR=777 TYPE1 BLVD)", info=True) }}

    -- Verify
    {% set v_dim %}
        select EMPLOYEE_EIN, JOB_CODE, ADDRESS_LINE_1, EFF_DT, DBT_HASH
        from CORE.SHARED_DIMS.DIM_EMPLOYEE
        where EMPLOYEE_EIN in ('100071494', '719074411')
    {% endset %}
    {% set d = run_query(v_dim) %}
    {{ log("=== DIM (old values, yesterday's EFF_DT):", info=True) }}
    {% for row in d.rows %}
        {{ log("    DIM: EIN=" ~ row[0] ~ " | JOB=" ~ row[1] ~ " | ADDR=" ~ row[2] ~ " | EFF=" ~ row[3] ~ " | HASH=" ~ row[4][:12] ~ "...", info=True) }}
    {% endfor %}

    {% set v_src %}
        select EMPLOYEE_EIN, JOB_CODE, ADDRESS_LINE_1, CDM_UPDATE_TIMESTAMP
        from CDM.WORKFORCE.EMPLOYEE
        where EMPLOYEE_EIN in ('100071494', '719074411')
    {% endset %}
    {% set s = run_query(v_src) %}
    {{ log("=== SOURCE (new values, fresh timestamp):", info=True) }}
    {% for row in s.rows %}
        {{ log("    SRC: EIN=" ~ row[0] ~ " | JOB=" ~ row[1] ~ " | ADDR=" ~ row[2] ~ " | TS=" ~ row[3], info=True) }}
    {% endfor %}

    {{ log("", info=True) }}
    {{ log("=== Now run: dbt run --select DIM_EMPLOYEE", info=True) }}

{% endmacro %}
