{{
    config(
        materialized='incremental',
        unique_key='ODS_IFP_CUPCST01_PK',
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
                 WHERE ODS_IFP_CUPCST01_PK IN (
                     SELECT {{ generate_surrogate_key([trim_string('DTL__BI_CST_COMP'),trim_string('DTL__BI_CST_ACCT'),trim_string('DTL__BI_CST_CNTR')]) }}
                     FROM {{ source('STAGING','STG_IFP_CUPCST01') }}
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

--Grab all the records newer than last update
WITH CTE_BATCH_ID AS (
    SELECT TO_NUMBER(TO_VARCHAR(CURRENT_TIMESTAMP, 'YYYYMMDDHH24MISSFF3')) AS BATCH_ID,
    CAST(CURRENT_TIMESTAMP AS TIMESTAMP_NTZ)  AS BATCH_TIMESTAMP
)

{% if is_incremental() %}
,CTE_SOURCE_RECORDS AS (
        SELECT * FROM {{ source('STAGING','STG_IFP_CUPCST01') }}
        where
        -- this filter will only be applied on an incremental run
        to_timestamp_tz(SUBSTRING(TRIM(DTL__CAPXTIMESTAMP),1,17),'YYYYMMDDHHMISSFF')  >= '{{ get_max_event_time('SRC_LAST_UPDATE_DTM') }}'
)

,CTE_DEDUP as (
  select *, ROW_NUMBER() OVER(PARTITION BY CST_COMP,CST_ACCT,CST_CNTR ORDER BY DTL__CAPXTIMESTAMP DESC) AS ROW_NUM
  from CTE_SOURCE_RECORDS
  qualify ROW_NUM=1
)

,CTE_TEMP as (
select CTE_SOURCE_RECORDS.DTL__BI_CST_COMP, CTE_SOURCE_RECORDS.DTL__BI_CST_ACCT, CTE_SOURCE_RECORDS.DTL__BI_CST_CNTR from 
CTE_SOURCE_RECORDS LEFT JOIN CTE_DEDUP 
 ON CTE_SOURCE_RECORDS.DTL__BI_CST_COMP=CTE_DEDUP.CST_COMP
 AND CTE_SOURCE_RECORDS.DTL__BI_CST_ACCT=CTE_DEDUP.CST_ACCT
 AND CTE_SOURCE_RECORDS.DTL__BI_CST_CNTR=CTE_DEDUP.CST_CNTR
 where CTE_SOURCE_RECORDS.PRIMARY_KEY_CHANGE  = 'Y'
AND (TO_NUMBER(CTE_SOURCE_RECORDS.PK_LZ_IFP_CUPCST01_ID) >=  TO_NUMBER(CTE_DEDUP.PK_LZ_IFP_CUPCST01_ID) OR TO_NUMBER(CTE_DEDUP.PK_LZ_IFP_CUPCST01_ID) IS NULL)
)

,CTE_FINAL as (
select * from CTE_DEDUP where CONCAT_WS(',',CST_COMP,CST_ACCT,CST_CNTR) in (
select CONCAT_WS(',',CST_COMP,CST_ACCT,CST_CNTR) from CTE_DEDUP
except 
select CONCAT_WS(',',DTL__BI_CST_COMP,DTL__BI_CST_ACCT,DTL__BI_CST_CNTR) from CTE_TEMP)
)

,-- Current Delta data capture from STG table
CTE_TRANSFORMED AS (
SELECT
        {{ generate_surrogate_key([trim_string('CST_COMP'),trim_string('CST_ACCT'),trim_string('CST_CNTR')])}} as ODS_IFP_CUPCST01_PK ,
        CAST(CONCAT({{ trim_string('CST_COMP')}},'-',{{ trim_string('CST_ACCT')}},'-',{{ trim_string('CST_CNTR')}}) AS VARCHAR(50)) AS ODS_IFP_CUPCST01_COMPOSITE_KEY,
        STG_IFP_CUPCST01_PK,
        {{ trim_string('DTL__CAPXUSER') }} AS SRC_CDC_LIB,
        {{ trim_string('CST_COMP')}} AS CST_COMP,
        {{ trim_string('CST_ACCT')}} AS CST_ACCT,
        {{ trim_string('CST_CNTR')}} AS CST_CNTR,
        {{ trim_string('CST_LINK')}} AS CST_LINK,
        {{ trim_string('CST_NAM1')}} AS CST_NAM1,
        {{ trim_string('CST_TTL1')}} AS CST_TTL1,
        {{ trim_string('CST_ROLE1')}} AS CST_ROLE1,
        CAST(CST_TAC1 AS NUMBER(3,0)) AS CST_TAC1,
        CAST(CST_TNB1 AS NUMBER(7,0)) AS CST_TNB1,
        CAST(CST_TXT1 AS NUMBER(6,0)) AS CST_TXT1,
        {{ trim_string('CST_TTP1')}} AS CST_TTP1,
        CAST(CST_TAC2 AS NUMBER(3,0)) AS CST_TAC2,
        CAST(CST_TNB2 AS NUMBER(7,0)) AS CST_TNB2,
        CAST(CST_TXT2 AS NUMBER(6,0)) AS CST_TXT2,
        {{ trim_string('CST_TTP2')}} AS CST_TTP2,
        CAST(CST_FXA1 AS NUMBER(3,0)) AS CST_FXA1,
        CAST(CST_FXN1 AS NUMBER(7,0)) AS CST_FXN1,
        {{ trim_string('CST_EMB1')}} AS CST_EMB1,
        {{ trim_string('CST_EMH1')}} AS CST_EMH1,
        {{ trim_string('CST_NAM2')}} AS CST_NAM2,
        UPPER({{ trim_string('CST_TTL2')}}) AS CST_TTL2,
        {{ trim_string('CST_ROLE2')}} AS CST_ROLE2,
        CAST(CST_TAC3 AS NUMBER(3,0)) AS CST_TAC3,
        CAST(CST_TNB3 AS NUMBER(7,0)) AS CST_TNB3,
        CAST(CST_TXT3 AS NUMBER(6,0)) AS CST_TXT3,
        {{ trim_string('CST_TTP3')}} AS CST_TTP3,
        CAST(CST_TAC4 AS NUMBER(3,0)) AS CST_TAC4,
        CAST(CST_TNB4 AS NUMBER(7,0)) AS CST_TNB4,
        CAST(CST_TXT4 AS NUMBER(6,0)) AS CST_TXT4,
        {{ trim_string('CST_TTP4')}} AS CST_TTP4,
        CAST(CST_FXA2 AS NUMBER(3,0)) AS CST_FXA2,
        CAST(CST_FXN2 AS NUMBER(7,0)) AS CST_FXN2,
        {{ trim_string('CST_EMB2')}} AS CST_EMB2,
        {{ trim_string('CST_EMH2')}} AS CST_EMH2,
        CST_XDT1 AS CST_XDT1_ORIG,
        {{ DateCheck_140_YYYYMMDD('CST_XDT1') }} AS CST_XDT1,
        CST_XDT2 AS CST_XDT2_ORIG,
        {{ DateCheck_140_YYYYMMDD('CST_XDT2') }} AS CST_XDT2,
        {{ trim_string('CST_FLG1')}} AS CST_FLG1,
        {{ trim_string('CST_FLG2')}} AS CST_FLG2,
        {{ trim_string('CST_FLD1')}} AS CST_FLD1,
        {{ trim_string('CST_FLD2')}} AS CST_FLD2,
        {{ trim_string('CST_USER')}} AS CST_USER,
        CAST(CST_UDAT AS NUMBER(10,0)) AS CST_UDAT,
        CAST(CST_UTIM AS NUMBER(6,0)) AS CST_UTIM,
        CAST(CONCAT({{ trim_string('CST_COMP')}},'-',{{ trim_string('CST_ACCT')}},'-',{{ trim_string('CST_CNTR')}}) AS VARCHAR(50)) AS SITE_COMPOSITE_KEY,
        CAST(SOURCE_SYSTEM AS VARCHAR(50)) AS SOURCE_SYSTEM,
        CAST('STG_IFP_CUPCST01' AS VARCHAR(200)) AS SOURCE_TABLE,
        to_timestamp_ntz(to_timestamp_tz(SUBSTRING(TRIM(DTL__CAPXTIMESTAMP),1,17),'YYYYMMDDHHMISSFF')) AS SRC_CREATE_DTM,
        to_timestamp_ntz(to_timestamp_tz(SUBSTRING(TRIM(DTL__CAPXTIMESTAMP),1,17),'YYYYMMDDHHMISSFF')) AS SRC_LAST_UPDATE_DTM,
        TRIM(DTL__CAPXACTION) AS SRC_ACTION_CD,
        CASE WHEN TRIM(DTL__CAPXACTION) = 'D' THEN 'Y' ELSE 'N' END AS SRC_DEL_IND,
        CAST(BATCH_ID AS BIGINT) AS ODS_INS_BATCH_ID,
        CAST(BATCH_ID AS BIGINT) AS ODS_UPD_BATCH_ID,
        CAST(BATCH_TIMESTAMP AS TIMESTAMP_NTZ) AS ODS_INSERT_TIMESTAMP,
        CAST(BATCH_TIMESTAMP AS TIMESTAMP_NTZ) AS ODS_UPDATE_TIMESTAMP,
        CAST(STG_INSERT_TIMESTAMP AS TIMESTAMP_NTZ) AS STG_INSERT_TIMESTAMP,
        CASE WHEN SRC_DEL_IND = 'Y' THEN 'DELETE' ELSE 'INSERT' END AS OPERATION_TYPE,
        CAST(NULL AS VARCHAR(20)) AS DQ_STATUS,
        CAST(NULL AS VARCHAR(50)) AS DQ_ERROR_CODE,
        CAST(NULL AS VARCHAR(500)) AS DQ_ERROR_MESSAGE
from CTE_FINAL
LEFT JOIN CTE_BATCH_ID ON 1=1
)

{% else %}
,CTE_HISTORICAL AS (
SELECT
        {{ generate_surrogate_key([trim_string('CST_COMP'),trim_string('CST_ACCT'),trim_string('CST_CNTR')]) }} as ODS_IFP_CUPCST01_PK,
        CAST(CONCAT({{ trim_string('CST_COMP') }},'-',{{ trim_string('CST_ACCT') }},'-',{{ trim_string('CST_CNTR') }}) AS VARCHAR(50)) AS ODS_IFP_CUPCST01_COMPOSITE_KEY,
        CAST(NULL AS NUMBER) AS STG_IFP_CUPCST01_PK,
        {{ trim_string('SYS_CDC_LIB') }} AS SRC_CDC_LIB,
        {{ trim_string('CST_COMP') }} AS CST_COMP,
        {{ trim_string('CST_ACCT') }} AS CST_ACCT,
        {{ trim_string('CST_CNTR') }} AS CST_CNTR,
        {{ trim_string('CST_LINK') }} AS CST_LINK,
        {{ trim_string('CST_NAM1') }} AS CST_NAM1,
        {{ trim_string('CST_TTL1') }} AS CST_TTL1,
        {{ trim_string('CST_ROLE1') }} AS CST_ROLE1,
        CAST(CST_TAC1 AS NUMBER(3,0)) AS CST_TAC1,
        CAST(CST_TNB1 AS NUMBER(7,0)) AS CST_TNB1,
        CAST(CST_TXT1 AS NUMBER(6,0)) AS CST_TXT1,
        {{ trim_string('CST_TTP1') }} AS CST_TTP1,
        CAST(CST_TAC2 AS NUMBER(3,0)) AS CST_TAC2,
        CAST(CST_TNB2 AS NUMBER(7,0)) AS CST_TNB2,
        CAST(CST_TXT2 AS NUMBER(6,0)) AS CST_TXT2,
        {{ trim_string('CST_TTP2') }} AS CST_TTP2,
        CAST(CST_FXA1 AS NUMBER(3,0)) AS CST_FXA1,
        CAST(CST_FXN1 AS NUMBER(7,0)) AS CST_FXN1,
        {{ trim_string('CST_EMB1') }} AS CST_EMB1,
        {{ trim_string('CST_EMH1') }} AS CST_EMH1,
        {{ trim_string('CST_NAM2') }} AS CST_NAM2,
        UPPER({{ trim_string('CST_TTL2') }}) AS CST_TTL2,
        {{ trim_string('CST_ROLE2') }} AS CST_ROLE2,
        CAST(CST_TAC3 AS NUMBER(3,0)) AS CST_TAC3,
        CAST(CST_TNB3 AS NUMBER(7,0)) AS CST_TNB3,
        CAST(CST_TXT3 AS NUMBER(6,0)) AS CST_TXT3,
        {{ trim_string('CST_TTP3') }} AS CST_TTP3,
        CAST(CST_TAC4 AS NUMBER(3,0)) AS CST_TAC4,
        CAST(CST_TNB4 AS NUMBER(7,0)) AS CST_TNB4,
        CAST(CST_TXT4 AS NUMBER(6,0)) AS CST_TXT4,
        {{ trim_string('CST_TTP4') }} AS CST_TTP4,
        CAST(CST_FXA2 AS NUMBER(3,0)) AS CST_FXA2,
        CAST(CST_FXN2 AS NUMBER(7,0)) AS CST_FXN2,
        {{ trim_string('CST_EMB2') }} AS CST_EMB2,
        {{ trim_string('CST_EMH2') }} AS CST_EMH2,
        CAST(CST_XDT1_ORIG AS NUMBER(10,0)) AS CST_XDT1_ORIG,
        CAST(CST_XDT1 AS TIMESTAMP_NTZ(7)) AS CST_XDT1,
        CAST(CST_XDT2_ORIG AS NUMBER(10,0)) AS CST_XDT2_ORIG,
        CAST(CST_XDT2 AS TIMESTAMP_NTZ(7)) AS CST_XDT2,
        {{ trim_string('CST_FLG1') }} AS CST_FLG1,
        {{ trim_string('CST_FLG2') }} AS CST_FLG2,
        {{ trim_string('CST_FLD1') }} AS CST_FLD1,
        {{ trim_string('CST_FLD2') }} AS CST_FLD2,
        {{ trim_string('CST_USER') }} AS CST_USER,
        CAST(CST_UDAT AS NUMBER(10,0)) AS CST_UDAT,
        CAST(CST_UTIM AS NUMBER(6,0)) AS CST_UTIM,
        CAST(CONCAT({{ trim_string('CST_COMP') }},'-',{{ trim_string('CST_ACCT') }},'-',{{ trim_string('CST_CNTR') }}) AS VARCHAR(50)) AS SITE_COMPOSITE_KEY,
        CAST('Infopro' AS VARCHAR(50)) AS SOURCE_SYSTEM,
        CAST('ODS_IFP_CUPCST01_HIST' AS VARCHAR(200)) AS SOURCE_TABLE,
        CAST(SYS_CDC_DTM AS TIMESTAMP_NTZ) AS SRC_CREATE_DTM,
        CAST(SYS_CDC_DTM AS TIMESTAMP_NTZ) AS SRC_LAST_UPDATE_DTM,
        CAST(SYS_ACTION_CD AS VARCHAR(1)) AS SRC_ACTION_CD,
        CAST(SYS_DEL_IND AS VARCHAR(1)) AS SRC_DEL_IND,
        CAST(BATCH_ID AS BIGINT) AS ODS_INS_BATCH_ID,
        CAST(BATCH_ID AS BIGINT) AS ODS_UPD_BATCH_ID,
        CAST(BATCH_TIMESTAMP AS TIMESTAMP_NTZ) AS ODS_INSERT_TIMESTAMP,
        CAST(BATCH_TIMESTAMP AS TIMESTAMP_NTZ) AS ODS_UPDATE_TIMESTAMP,
        CAST(STG_INSERT_TIMESTAMP AS TIMESTAMP_NTZ) AS STG_INSERT_TIMESTAMP,
        CASE WHEN SYS_DEL_IND = 'Y' THEN 'DELETE' ELSE 'INSERT' END AS OPERATION_TYPE,
        CAST(NULL AS VARCHAR(20)) AS DQ_STATUS,
        CAST(NULL AS VARCHAR(50)) AS DQ_ERROR_CODE,
        CAST(NULL AS VARCHAR(500)) AS DQ_ERROR_MESSAGE
FROM INFOPRO.HISTORY.ODS_IFP_CUPCST01_HIST
CROSS JOIN CTE_BATCH_ID
)

{% endif %}

SELECT 
        *,
        {{ generate_hash_change( relation = this) }} AS HASH_CHANGE,
        {{ generate_hash_full( relation = this) }} AS HASH_FULL
FROM {% if is_incremental() %}CTE_TRANSFORMED{% else %}CTE_HISTORICAL{% endif %}





