{% test check_job_cd_change(model, column_name, value) %}
WITH ranked_data AS (
    SELECT
        EMPLOYEE_EIN, 
        EFF_DT,
        DISC_DT,
        JOB_CD,
        LAG(JOB_CD) OVER (PARTITION BY EMPLOYEE_EIN ORDER BY EFF_DT) AS PREV_JOB_CD
    FROM {{ model }}
),

validation AS (
    SELECT
        EMPLOYEE_EIN,
        EFF_DT,
        DISC_DT,
        JOB_CD,
        PREV_JOB_CD
    FROM ranked_data
    WHERE PREV_JOB_CD IS NOT NULL
      AND JOB_CD <> PREV_JOB_CD -- Check if JOB_CD has changed
)

SELECT
    COUNT(*) AS validation_errors
FROM validation;
{% endtest %}
