{{
    config(
        materialized='incremental',
        unique_key='ODS_IFP_CUPCUA_PK',
        merge_no_update_columns = ['ODS_INS_BATCH_ID','ODS_INSERT_TIMESTAMP','SRC_CREATE_DTM'],
        merge_condition="DBT_INTERNAL_SOURCE.HASH_CHANGE != DBT_INTERNAL_DEST.HASH_CHANGE",
        tags=["ods","infopro","scheduled-every30min","data-observe"],
	pre_hook=" {% if is_incremental() %}
                EXECUTE IMMEDIATE $$
                 BEGIN
                 IF (EXISTS(SELECT * FROM {{ this.database }}.information_schema.tables WHERE table_schema = UPPER('{{ this.schema }}') AND table_name = UPPER('{{ this.identifier }}'))) THEN
                 UPDATE {{ this }}
                 SET SRC_ACTION_CD='D',
                     SRC_DEL_IND='Y',
                     SRC_LAST_UPDATE_DTM=CURRENT_TIMESTAMP,
                     ODS_UPD_BATCH_ID=TO_NUMBER(TO_VARCHAR(CURRENT_TIMESTAMP,'YYYYMMDDHH24MISSFF3')),
                     ODS_UPDATE_TIMESTAMP=CURRENT_TIMESTAMP,
                     OPERATION_TYPE='DELETE'
                 WHERE ODS_IFP_CUPCUA_PK IN (
                     SELECT {{ generate_surrogate_key([trim_string('DTL__BI_DIVISION'),trim_string('DTL__BI_ACCOUNT_NUMBER')]) }}
                     FROM {{ source('STAGING','STG_IFP_CUPCUA') }}
                     WHERE PRIMARY_KEY_CHANGE='Y'
                     AND to_timestamp_tz(SUBSTRING(DTL__CAPXTIMESTAMP,1,17),'YYYYMMDDHHMISSFF') >= '{{ get_max_event_time('SRC_LAST_UPDATE_DTM') }}'
                 );
                 COMMIT;
                 END IF;
                END;
                $$;
                {% endif %}"
        )
}}

WITH CTE_BATCH_ID AS (
    SELECT TO_NUMBER(TO_VARCHAR(CURRENT_TIMESTAMP, 'YYYYMMDDHH24MISSFF3')) AS BATCH_ID,
    CAST(CURRENT_TIMESTAMP AS TIMESTAMP_NTZ)  AS BATCH_TIMESTAMP
)

{% if is_incremental() %}

--> Grab all the records newer than last update
,CTE_SOURCE_RECORDS AS (
        SELECT * FROM {{ source('STAGING','STG_IFP_CUPCUA') }}
        where
        -- this filter will only be applied on an incremental run
        to_timestamp_tz(SUBSTRING(TRIM(DTL__CAPXTIMESTAMP),1,17),'YYYYMMDDHHMISSFF')  >= '{{ get_max_event_time('SRC_LAST_UPDATE_DTM') }}'
)

,CTE_DEDUP as (
  select *, ROW_NUMBER() OVER(PARTITION BY DIVISION,ACCOUNT_NUMBER ORDER BY DTL__CAPXTIMESTAMP DESC) AS ROW_NUM
  from CTE_SOURCE_RECORDS
  qualify ROW_NUM=1
)

,CTE_PRIMARY_KEY_CHANGE as (
select CTE_SOURCE_RECORDS.DTL__BI_DIVISION, CTE_SOURCE_RECORDS.DTL__BI_ACCOUNT_NUMBER from 
CTE_SOURCE_RECORDS LEFT JOIN CTE_DEDUP 
 ON 1=1
 where CTE_SOURCE_RECORDS.PRIMARY_KEY_CHANGE  = 'Y'
AND (TO_NUMBER(CTE_SOURCE_RECORDS.PK_LZ_IFP_CUPCUA_ID) >=  TO_NUMBER(CTE_DEDUP.PK_LZ_IFP_CUPCUA_ID) OR TO_NUMBER(CTE_DEDUP.PK_LZ_IFP_CUPCUA_ID) IS NULL)
)

,CTE_FINAL as (
select * from CTE_DEDUP where CONCAT_WS(',',DIVISION,ACCOUNT_NUMBER) in (
select CONCAT_WS(',',DIVISION,ACCOUNT_NUMBER) from CTE_DEDUP
except 
select CONCAT_WS(',',DTL__BI_DIVISION,DTL__BI_ACCOUNT_NUMBER) from CTE_PRIMARY_KEY_CHANGE)
)

,--> Current Delta data capture from STG table
CTE_TRANSFORMED AS (
SELECT
        {{ generate_surrogate_key([trim_string('DIVISION'),trim_string('ACCOUNT_NUMBER')])}} as ODS_IFP_CUPCUA_PK ,
        CAST(CONCAT({{ trim_string('DIVISION')}},'-',{{ trim_string('ACCOUNT_NUMBER')}}) AS VARCHAR(50)) AS ODS_IFP_CUPCUA_COMPOSITE_KEY,
        STG_IFP_CUPCUA_PK,
        {{ trim_string('DTL__CAPXUSER') }} AS SRC_CDC_LIB,
        {{ trim_string('DIVISION')}} AS UACONO,
        {{ trim_string('ACCOUNT_NUMBER')}} AS UAACCT,
        CAST(CUPCUAID AS NUMBER(10,0)) AS UGCUPCUAID,
        {{ trim_string('NATIONAL_ACCOUNT_ID')}} AS UANANO,
        CAST(ACCOUNT_GROUP_ID AS NUMBER(10,0)) AS UAACGRPID,
        {{ trim_string('CORPORATE_SHORT_AR_NAME')}} AS UAARNAM,
        CAST(CUSTOMER_ID AS NUMBER(10,0)) AS UAPARID,
        {{ trim_string('NA_BILLING')}} AS UANABIL,
        {{ trim_string('NA_FAP')}} AS UANAFAP,
        {{ trim_string('NA_REPORTING')}} AS UANARPT,
        {{ trim_string('PRICING')}} AS UAPRICE,
        {{ trim_string('SALES')}} AS UASALES,
        {{ trim_string('MPD')}} AS UAMPD,
        {{ trim_string('CORPFAP')}} AS UACOFAP,
        {{ trim_string('NA_PROCUREMENT')}} AS UANAPRO,
        {{ trim_string('REPORT_FIELD_1')}} AS UARPF1,
        {{ trim_string('REPORT_FIELD_2')}} AS UARPF2,
        {{ trim_string('REPORT_FIELD_3')}} AS UARPF3,
        DTL__BI_DATE_FIELD_1 AS UADATE1_ORIG,
        DATE_FIELD_1 AS UADATE1,
        DTL__BI_DATE_FIELD_2 AS UADATE2_ORIG,
        DATE_FIELD_2 AS UADATE2,
        DTL__BI_DATE_FIELD_3 AS UADATE3_ORIG,
        DATE_FIELD_3 AS UADATE3,
        DTL__BI_DATE_FIELD_4 AS UADATE4_ORIG,
        DATE_FIELD_4 AS UADATE4,
        DTL__BI_DATE_FIELD_5 AS UADATE5_ORIG,
        DATE_FIELD_5 AS UADATE5,
        {{ trim_string('FLAG_1')}} AS UAFLAG1,
        {{ trim_string('FLAG_2')}} AS UAFLAG2,
        {{ trim_string('FLAG_3')}} AS UAFLAG3,
        {{ trim_string('FLAG_4')}} AS UAFLAG4,
        {{ trim_string('FLAG_5')}} AS UAFLAG5,
        {{ trim_string('ADDED_BY_USER')}} AS UAADDBY,
        {{ trim_string('ADDED_BY_PROGRAM')}} AS UAADDPGM,
        ADDED_TIME_STAMP AS UAADDTIM,
        {{ trim_string('CHANGED_BY_USER')}} AS UACHGBY,
        {{ trim_string('CHANGED_BY_PROGRAM')}} AS UACHGPGM,
        CHANGED_TIME_STAMP AS UACHGTIM,
        CAST(CONCAT({{ trim_string('DIVISION') }},'-',{{ trim_string('ACCOUNT_NUMBER') }}) AS VARCHAR(50)) AS ACCOUNT_COMPOSITE_KEY,
        SOURCE_SYSTEM,
        CAST('STG_IFP_CUPCUA' AS VARCHAR(200)) AS SOURCE_TABLE,
        to_timestamp_ntz(to_timestamp_tz(SUBSTRING(TRIM(DTL__CAPXTIMESTAMP),1,17),'YYYYMMDDHHMISSFF')) AS SRC_CREATE_DTM,
        to_timestamp_ntz(to_timestamp_tz(SUBSTRING(TRIM(DTL__CAPXTIMESTAMP),1,17),'YYYYMMDDHHMISSFF')) AS SRC_LAST_UPDATE_DTM,
        TRIM(DTL__CAPXACTION) AS SRC_ACTION_CD,
        CASE WHEN TRIM(DTL__CAPXACTION) = 'D' THEN 'Y' ELSE 'N' END AS SRC_DEL_IND,
        CAST(BATCH_ID AS BIGINT) AS ODS_INS_BATCH_ID,
        CAST(BATCH_ID AS BIGINT) AS ODS_UPD_BATCH_ID,
        BATCH_TIMESTAMP AS ODS_INSERT_TIMESTAMP,
        BATCH_TIMESTAMP AS ODS_UPDATE_TIMESTAMP,
        CAST(STG_INSERT_TIMESTAMP AS TIMESTAMP_NTZ) AS STG_INSERT_TIMESTAMP,
        CASE WHEN SRC_DEL_IND = 'Y' THEN 'DELETE' ELSE 'INSERT' END AS OPERATION_TYPE, --overwrite operation type as UPDATE/DELETE in merge operation
        CAST(NULL AS VARCHAR(20)) AS DQ_STATUS,
        CAST(NULL AS VARCHAR(500)) AS DQ_ERROR_CODE,
        CAST(NULL AS VARCHAR(2000)) AS DQ_ERROR_MESSAGE
from CTE_FINAL
CROSS JOIN CTE_BATCH_ID
)

{% else %}

,CTE_HISTORICAL AS (
SELECT
        {{ generate_surrogate_key([trim_string('UACONO'),trim_string('UAACCT')]) }} AS ODS_IFP_CUPCUA_PK,
        CAST(CONCAT({{ trim_string('UACONO') }},'-',{{ trim_string('UAACCT') }}) AS VARCHAR(50)) AS ODS_IFP_CUPCUA_COMPOSITE_KEY,
        CAST(NULL AS NUMBER) AS STG_IFP_CUPCUA_PK,
        {{ trim_string('SYS_CDC_LIB') }} AS SRC_CDC_LIB,
        {{ trim_string('UACONO') }} AS UACONO,
        {{ trim_string('UAACCT') }} AS UAACCT,
        CAST(UGCUPCUAID AS NUMBER(10,0)) AS UGCUPCUAID,
        {{ trim_string('UANANO') }} AS UANANO,
        CAST(UAACGRPID AS NUMBER(10,0)) AS UAACGRPID,
        {{ trim_string('UAARNAM') }} AS UAARNAM,
        CAST(UAPARID AS NUMBER(10,0)) AS UAPARID,
        {{ trim_string('UANABIL') }} AS UANABIL,
        {{ trim_string('UANAFAP') }} AS UANAFAP,
        {{ trim_string('UANARPT') }} AS UANARPT,
        {{ trim_string('UAPRICE') }} AS UAPRICE,
        {{ trim_string('UASALES') }} AS UASALES,
        {{ trim_string('UAMPD') }} AS UAMPD,
        {{ trim_string('UACOFAP') }} AS UACOFAP,
        {{ trim_string('UANAPRO') }} AS UANAPRO,
        {{ trim_string('UARPF1') }} AS UARPF1,
        {{ trim_string('UARPF2') }} AS UARPF2,
        {{ trim_string('UARPF3') }} AS UARPF3,
        CAST(UADATE1_ORIG AS TIMESTAMP_NTZ) AS UADATE1_ORIG,
        CAST(UADATE1 AS TIMESTAMP_NTZ) AS UADATE1,
        CAST(UADATE2_ORIG AS TIMESTAMP_NTZ) AS UADATE2_ORIG,
        CAST(UADATE2 AS TIMESTAMP_NTZ) AS UADATE2,
        CAST(UADATE3_ORIG AS TIMESTAMP_NTZ) AS UADATE3_ORIG,
        CAST(UADATE3 AS TIMESTAMP_NTZ) AS UADATE3,
        CAST(UADATE4_ORIG AS TIMESTAMP_NTZ) AS UADATE4_ORIG,
        CAST(UADATE4 AS TIMESTAMP_NTZ) AS UADATE4,
        CAST(UADATE5_ORIG AS TIMESTAMP_NTZ) AS UADATE5_ORIG,
        CAST(UADATE5 AS TIMESTAMP_NTZ) AS UADATE5,
        {{ trim_string('UAFLAG1') }} AS UAFLAG1,
        {{ trim_string('UAFLAG2') }} AS UAFLAG2,
        {{ trim_string('UAFLAG3') }} AS UAFLAG3,
        {{ trim_string('UAFLAG4') }} AS UAFLAG4,
        {{ trim_string('UAFLAG5') }} AS UAFLAG5,
        {{ trim_string('UAADDBY') }} AS UAADDBY,
        {{ trim_string('UAADDPGM') }} AS UAADDPGM,
        CAST(UAADDTIM AS TIMESTAMP_NTZ) AS UAADDTIM,
        {{ trim_string('UACHGBY') }} AS UACHGBY,
        {{ trim_string('UACHGPGM') }} AS UACHGPGM,
        CAST(UACHGTIM AS TIMESTAMP_NTZ) AS UACHGTIM,
        CAST(CONCAT({{ trim_string('UACONO') }},'-',{{ trim_string('UAACCT') }}) AS VARCHAR(50)) AS ACCOUNT_COMPOSITE_KEY,
        CAST('Infopro' AS VARCHAR(50)) AS SOURCE_SYSTEM,
        CAST('ODS_IFP_CUPCUA_HIST' AS VARCHAR(200)) AS SOURCE_TABLE,
        CAST(SYS_CDC_DTM AS TIMESTAMP_NTZ) AS SRC_CREATE_DTM,
        CAST(SYS_CDC_DTM AS TIMESTAMP_NTZ) AS SRC_LAST_UPDATE_DTM,
        CAST(SYS_ACTION_CD AS VARCHAR(1)) AS SRC_ACTION_CD,
        CAST(SYS_DEL_IND AS VARCHAR(1)) AS SRC_DEL_IND,
        CAST(BATCH_ID AS BIGINT) AS ODS_INS_BATCH_ID,
        CAST(BATCH_ID AS BIGINT) AS ODS_UPD_BATCH_ID,
        BATCH_TIMESTAMP AS ODS_INSERT_TIMESTAMP,
        BATCH_TIMESTAMP AS ODS_UPDATE_TIMESTAMP,
        CAST(STG_INSERT_TIMESTAMP AS TIMESTAMP_NTZ) AS STG_INSERT_TIMESTAMP,
        CASE WHEN SYS_DEL_IND = 'Y' THEN 'DELETE' ELSE 'INSERT' END AS OPERATION_TYPE,
        CAST(NULL AS VARCHAR(20)) AS DQ_STATUS,
        CAST(NULL AS VARCHAR(500)) AS DQ_ERROR_CODE,
        CAST(NULL AS VARCHAR(2000)) AS DQ_ERROR_MESSAGE
FROM INFOPRO.HISTORY.ODS_IFP_CUPCUA_HIST
CROSS JOIN CTE_BATCH_ID
)

{% endif %}

SELECT 
        *,
        {{ generate_hash_change( relation = this) }} AS HASH_CHANGE,
        {{ generate_hash_full( relation = this) }} AS HASH_FULL
FROM {% if is_incremental() %}CTE_TRANSFORMED{% else %}CTE_HISTORICAL{% endif %}





