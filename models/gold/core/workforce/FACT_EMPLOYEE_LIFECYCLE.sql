-- CORE FACT_EMPLOYEE_LIFECYCLE Model - Transactional Fact
-- Source: CORE.DIM_EMPLOYEE (Dimension Layer - SCD Type 2)
-- Target: CORE.FACT_EMPLOYEE_LIFECYCLE (Fact Layer)
-- Pattern: Full-refresh table detecting lifecycle events from SCD2 dimension versions
-- Grain: One row per lifecycle event per employee
-- Events: HIRE, TERMINATION, LOA, RETURN, ROLE_CHANGE, ORG_TRANSFER, PROFILE_UPDATE


{{
    config(
        materialized='table',
        tags=["gold","workday", "workforce", "core-facts", "data-observe"]
    )
}}
-- TEMP COMMENT FOR OBSEVEABILITY POLICIES
with CTE_dim_employee as (
    -- Full SCD2 history for all employees
    select dim.*
    from {{ ref('DIM_EMPLOYEE') }} dim
    where dim.IS_DELETED = false
),
CTE_audits as (
    select
        TO_NUMBER(TO_VARCHAR(CURRENT_TIMESTAMP, 'YYYYMMDDHH24MISSFF3')) as batch_id,
        CAST(CURRENT_TIMESTAMP AS TIMESTAMP_NTZ) as batch_timestamp
),
CTE_versioned as (
    -- Compare each SCD2 version to the previous version to detect lifecycle events
    select
        dim.*,
        LAG(dim.JOB_CODE)          over (partition by dim.EMPLOYEE_EIN order by dim.EFF_DT) as PREV_JOB_CODE,
        LAG(dim.DIVISION_NUMBER)   over (partition by dim.EMPLOYEE_EIN order by dim.EFF_DT) as PREV_DIVISION_NUMBER,
        LAG(dim.SUPERVISOR_EIN)    over (partition by dim.EMPLOYEE_EIN order by dim.EFF_DT) as PREV_SUPERVISOR_EIN,
        LAG(dim.EMPLOYEE_STATUS)   over (partition by dim.EMPLOYEE_EIN order by dim.EFF_DT) as PREV_EMP_STATUS,
        LAG(dim.EFF_DT)        over (partition by dim.EMPLOYEE_EIN order by dim.EFF_DT) as PREV_EVENT_DATE,
        ROW_NUMBER()           over (partition by dim.EMPLOYEE_EIN order by dim.EFF_DT) as VERSION_NUM
    from CTE_dim_employee dim
),
CTE_events as (
    select
        -- Surrogate Key
        {{ generate_surrogate_key(['EMPLOYEE_EIN', 'EFF_DT']) }} as LIFECYCLE_SK,

        -- Foreign Key to DIM_EMPLOYEE
        cast(EMPLOYEE_SK as varchar(32)) as EMPLOYEE_SK,

        -- Degenerate Dimension
        cast(EMPLOYEE_EIN as varchar(15)) as EMPLOYEE_EIN,

        -- Event Date & Type Detection
        cast(EFF_DT as date) as EVENT_DATE,
        case
            when VERSION_NUM = 1                                                then 'HIRE'
            when UPPER(TRIM(EMPLOYEE_STATUS)) in ('TERM', 'T') 
                 and (PREV_EMP_STATUS is null or UPPER(TRIM(PREV_EMP_STATUS)) not in ('TERM', 'T'))
                                                                                then 'TERMINATION'
            when UPPER(TRIM(EMPLOYEE_STATUS)) = 'LOA' 
                 and (PREV_EMP_STATUS is null or UPPER(TRIM(PREV_EMP_STATUS)) != 'LOA')
                                                                                then 'LOA'
            when UPPER(TRIM(PREV_EMP_STATUS)) = 'LOA' 
                 and UPPER(TRIM(EMPLOYEE_STATUS)) not in ('LOA', 'TERM', 'T')
                                                                                then 'RETURN'
            when DIVISION_NUMBER != PREV_DIVISION_NUMBER                        then 'ORG_TRANSFER'
            when JOB_CODE != PREV_JOB_CODE                                      then 'ROLE_CHANGE'
            else 'PROFILE_UPDATE'
        end as EVENT_TYPE,

        -- Fiscal Calendar Derived Fields
        cast(YEAR(EFF_DT) as integer) as EVENT_FISCAL_YEAR,
        cast(QUARTER(EFF_DT) as integer) as EVENT_QUARTER,
        cast(MONTH(EFF_DT) as integer) as EVENT_MONTH,

        -- Tenure Measures (calculated from hire date to event date)
        cast(DATEDIFF('day', HIRE_DATE, EFF_DT) as integer) as TENURE_DAYS,
        cast(ROUND(DATEDIFF('day', HIRE_DATE, EFF_DT) / 30.44, 1) as number(10,1)) as TENURE_MONTHS,
        cast(ROUND(DATEDIFF('day', HIRE_DATE, EFF_DT) / 365.25, 2) as number(10,2)) as TENURE_YEARS,
        case
            when DATEDIFF('day', HIRE_DATE, EFF_DT) < 365   then '<1 year'
            when DATEDIFF('day', HIRE_DATE, EFF_DT) < 1095  then '1-3 years'
            when DATEDIFF('day', HIRE_DATE, EFF_DT) < 1825  then '3-5 years'
            else '5+ years'
        end as TENURE_BAND,

        -- Days Since Last Event
        cast(DATEDIFF('day', PREV_EVENT_DATE, EFF_DT) as integer) as DAYS_SINCE_LAST_EVENT,

        -- Termination Flags & Details
        case
            when UPPER(EMPLOYEE_STATUS) in ('TERM', 'T')
                 and (PREV_EMP_STATUS is null or UPPER(PREV_EMP_STATUS) not in ('TERM', 'T'))
            then true
            else false
        end as IS_TERMINATION,
        cast(null as boolean) as IS_VOLUNTARY_TERM,
        cast(null as varchar(20)) as TERM_REASON_CODE,
        cast(null as varchar(100)) as TERM_REASON_DESC,

        -- Role Change Tracking
        cast(PREV_JOB_CODE as varchar(9)) as PREV_JOB_CODE,
        cast(JOB_CODE as varchar(9)) as NEW_JOB_CODE,
        case
            when JOB_CODE is not null and PREV_JOB_CODE is not null 
                 and JOB_CODE != PREV_JOB_CODE and JOB_CODE > PREV_JOB_CODE
            then true
            else false
        end as IS_PROMOTION,
        case
            when JOB_CODE is not null and PREV_JOB_CODE is not null
                 and JOB_CODE != PREV_JOB_CODE and JOB_CODE <= PREV_JOB_CODE
            then true
            else false
        end as IS_LATERAL,

        -- Org Transfer Tracking
        cast(PREV_DIVISION_NUMBER as integer) as PREV_DIVISION_NUMBER,
        cast(DIVISION_NUMBER as integer) as NEW_DIVISION_NUMBER,
        case
            when DIVISION_NUMBER is not null and PREV_DIVISION_NUMBER is not null
                 and DIVISION_NUMBER != PREV_DIVISION_NUMBER
            then true
            else false
        end as IS_DIVISION_CHANGE,

        -- Supervisor Change Tracking
        cast(PREV_SUPERVISOR_EIN as varchar(15)) as PREV_SUPERVISOR_EIN,
        cast(SUPERVISOR_EIN as varchar(15)) as NEW_SUPERVISOR_EIN,

        -- Data Quality
        cast(
            case
                when EMPLOYEE_EIN is not null and EFF_DT is not null and EMPLOYEE_SK is not null
                    then 100.00
                else 0.00
            end as decimal(5,2)
        ) as DQ_SCORE,
        cast(
            case
                when EMPLOYEE_EIN is not null and EFF_DT is not null and EMPLOYEE_SK is not null
                    then 'PASSED'
                else 'FAILED'
            end as varchar(20)
        ) as DQ_STATUS,

        -- Audit Fields
        cast(CTE_audits.batch_id as number(18,0))  as INS_BATCH_ID,
        cast(CTE_audits.batch_id as number(18,0))  as UPD_BATCH_ID,
        cast(SOURCE_SYSTEM as varchar(20))          as SOURCE_SYSTEM,
        cast('DIM_EMPLOYEE' as varchar(50))         as PRIMARY_DATA_SOURCE,
        cast(CTE_audits.batch_timestamp as timestamp_ntz(9)) as CDM_INSERT_TIMESTAMP,
        cast(CTE_audits.batch_timestamp as timestamp_ntz(9)) as CDM_UPDATE_TIMESTAMP,

        -- Hash Columns
        {{ generate_hash_change([
            'EMPLOYEE_EIN', 'EFF_DT', 'JOB_CODE', 'DIVISION_NUMBER',
            'SUPERVISOR_EIN', 'EMPLOYEE_STATUS'
        ]) }} as HASH_CHANGE,
        {{ generate_hash_full([
            'EMPLOYEE_EIN', 'EFF_DT', 'EMPLOYEE_SK', 'JOB_CODE',
            'DIVISION_NUMBER', 'SUPERVISOR_EIN', 'EMPLOYEE_STATUS',
            'HIRE_DATE'
        ]) }} as HASH_FULL,

        -- Operation Type
        cast('INSERT' as varchar(10))               as OPERATION_TYPE

    from CTE_versioned
    cross join CTE_audits
)
select * from CTE_events