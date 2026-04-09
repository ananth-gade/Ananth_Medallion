{{
    config(
        materialized='incremental',
        unique_key='ODS_IFP_ARPCU_PK',
        merge_no_update_columns = ['ODS_INS_BATCH_ID','ODS_INSERT_TIMESTAMP','SRC_CREATE_DTM'],
        merge_condition="DBT_INTERNAL_SOURCE.HASH_CHANGE != DBT_INTERNAL_DEST.HASH_CHANGE",
        tags=["ods","infopro","scheduled-every30min","data-observe"],
	pre_hook=
		" {% if is_incremental() %}
                EXECUTE IMMEDIATE $$
                 BEGIN
                 IF (EXISTS(SELECT  * FROM {{ this.database }}.information_schema.tables WHERE  table_schema = 'ODS' and  table_name = '{{ this.identifier }}')) THEN
                 
                 UPDATE {{ this }}
                 SET SRC_ACTION_CD='D',
                     SRC_DEL_IND='Y',
                     SRC_LAST_UPDATE_DTM=CURRENT_TIMESTAMP,
                     ODS_UPD_BATCH_ID=TO_NUMBER(TO_VARCHAR(CURRENT_TIMESTAMP,'YYYYMMDDHH24MISSFF3')),
                     ODS_UPDATE_TIMESTAMP=CURRENT_TIMESTAMP,
                     OPERATION_TYPE='DELETE'
                 WHERE ODS_IFP_ARPCU_PK IN (
                     SELECT {{ generate_surrogate_key([trim_string('DTL__BI_CUCO'),trim_string('DTL__BI_CUCUNO')]) }}
                     FROM {{ source('STAGING','STG_IFP_ARPCU') }}
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


-- depends_on: {{ ref('STG_IFP_ARPCU_ADDRESS') }}

--> Grab all the records newer than last update
WITH CTE_BATCH_ID AS (
    SELECT TO_NUMBER(TO_VARCHAR(CURRENT_TIMESTAMP, 'YYYYMMDDHH24MISSFF3')) AS BATCH_ID,
    CAST(CURRENT_TIMESTAMP AS TIMESTAMP_NTZ)  AS BATCH_TIMESTAMP
)

{% if is_incremental() %}
,CTE_SOURCE_RECORDS AS (
        SELECT * FROM {{ source('STAGING','STG_IFP_ARPCU') }}
        where
        -- this filter will only be applied on an incremental run
        to_timestamp_tz(SUBSTRING(TRIM(DTL__CAPXTIMESTAMP),1,17),'YYYYMMDDHHMISSFF')  >= '{{ get_max_event_time('SRC_LAST_UPDATE_DTM') }}'
)

,CTE_DEDUP as (
  select *, ROW_NUMBER() OVER(PARTITION BY CUCO,CUCUNO ORDER BY DTL__CAPXTIMESTAMP DESC) AS ROW_NUM
  from CTE_SOURCE_RECORDS
  qualify ROW_NUM=1
)

,CTE_TEMP as (
select CTE_SOURCE_RECORDS.DTL__BI_CUCO, CTE_SOURCE_RECORDS.DTL__BI_CUCUNO from 
CTE_SOURCE_RECORDS LEFT JOIN CTE_DEDUP 
 ON CTE_SOURCE_RECORDS.DTL__BI_CUCO=CTE_DEDUP.CUCO
 AND CTE_SOURCE_RECORDS.DTL__BI_CUCUNO=CTE_DEDUP.CUCUNO
 where CTE_SOURCE_RECORDS.PRIMARY_KEY_CHANGE  = 'Y'
AND (TO_NUMBER(CTE_SOURCE_RECORDS.PK_LZ_IFP_ARPCU_ID) >=  TO_NUMBER(CTE_DEDUP.PK_LZ_IFP_ARPCU_ID) OR TO_NUMBER(CTE_DEDUP.PK_LZ_IFP_ARPCU_ID) IS NULL)
)

,CTE_FINAL as (
select * from CTE_DEDUP where CONCAT_WS(',',CUCO,CUCUNO) in (
select CONCAT_WS(',',CUCO,CUCUNO) from CTE_DEDUP
except 
select CONCAT_WS(',',DTL__BI_CUCO,DTL__BI_CUCUNO) from CTE_TEMP)
)

,--> Current Delta data capture from STG table
CTE_TRANSFORMED AS (
SELECT
        {{ generate_surrogate_key([trim_string('CTE_FINAL.CUCO'),trim_string('CTE_FINAL.CUCUNO')])}} as ODS_IFP_ARPCU_PK ,
        CAST(CONCAT({{ trim_string('CTE_FINAL.CUCO')}},'-',{{ trim_string('CTE_FINAL.CUCUNO')}}) AS VARCHAR(50)) AS ODS_IFP_ARPCU_COMPOSITE_KEY,
        CTE_FINAL.STG_IFP_ARPCU_PK,
        {{ trim_string('CTE_FINAL.DTL__CAPXUSER') }} AS SRC_CDC_LIB,
        {{ trim_string('CTE_FINAL.CUCO')}} AS CUCO,
        {{ trim_string('CTE_FINAL.CUCUNO')}} AS CUCUNO,
        {{ trim_string('CUNANO')}} AS CUNANO,
        CAST(CULKNO AS NUMBER(10,0)) AS CULKNO,
        {{ trim_string('CUTXID')}} AS CUTXID,
        {{ trim_string('CUSLID')}} AS CUSLID,
        {{ trim_string('CUSMAN')}} AS CUSMAN,
        {{ trim_string('CUNAME')}} AS CUNAME,
        {{ trim_string('CUADR1')}} AS CUADR1,
        {{ trim_string('CUADR2')}} AS CUADR2,
        {{ trim_string('CUADR3')}} AS CUADR3,
        {{ trim_string('CUCITY')}} AS CUCITY,
        {{ trim_string('CUSTAT')}} AS CUSTAT,
        {{ trim_string('CUPOST')}} AS CUPOST,
        {{ trim_string('CUCTRY')}} AS CUCTRY,
        {{ trim_string('CUALIA')}} AS CUALIA,
        {{ trim_string('CUPCO')}} AS CUPCO,
        {{ trim_string('CUPCNO')}} AS CUPCNO,
        {{ trim_string('CUCATG')}} AS CUCATG,
        {{ trim_string('CUMACL')}} AS CUMACL,
        {{ trim_string('CUMICL')}} AS CUMICL,
        {{ trim_string('CUSNAM')}} AS CUSNAM,
        {{ trim_string('CUAPST')}} AS CUAPST,
        {{ trim_string('CUOTCC')}} AS CUOTCC,
        {{ trim_string('CUAICD')}} AS CUAICD,
        {{ trim_string('CUUFL1')}} AS CUUFL1,
        {{ trim_string('CUUFL2')}} AS CUUFL2,
        {{ trim_string('CUUFL3')}} AS CUUFL3,
        {{ trim_string('CUUFL4')}} AS CUUFL4,
        {{ trim_string('CUUFL5')}} AS CUUFL5,
        CAST(CUUAM1 AS NUMBER(13,2)) AS CUUAM1,
        CAST(CUUAM2 AS NUMBER(13,2)) AS CUUAM2,
        CAST(CUUDTH AS NUMBER(10,0)) AS CUUDTH,
        {{ trim_string('CUUDTE')}} AS CUUDTE,
        CAST(CUUDT8 AS NUMBER(10,0)) AS CUUDT8,
        {{ trim_string('CUARTC')}} AS CUARTC,
        {{ trim_string('CUFCPC')}} AS CUFCPC,
        {{ trim_string('CUCTPC')}} AS CUCTPC,
        {{ trim_string('CUDGPC')}} AS CUDGPC,
        {{ trim_string('CUFGPC')}} AS CUFGPC,
        {{ trim_string('CUANPC')}} AS CUANPC,
        {{ trim_string('CULPPC')}} AS CULPPC,
        {{ trim_string('CUPTPC')}} AS CUPTPC,
        {{ trim_string('CUSTPC')}} AS CUSTPC,
        {{ trim_string('CUDCPC')}} AS CUDCPC,
        {{ trim_string('CURMPC')}} AS CURMPC,
        {{ trim_string('CUAGC')}} AS CUAGC,
        {{ trim_string('CUOADC')}} AS CUOADC,
        {{ trim_string('CUEXHC')}} AS CUEXHC,
        {{ trim_string('CUEXST')}} AS CUEXST,
        {{ trim_string('CUEXDL')}} AS CUEXDL,
        {{ trim_string('CUEXFC')}} AS CUEXFC,
        {{ trim_string('CUEXLC')}} AS CUEXLC,
        {{ trim_string('CUEXTT')}} AS CUEXTT,
        {{ trim_string('CUAPPC')}} AS CUAPPC,
        {{ trim_string('CUACCC')}} AS CUACCC,
        {{ trim_string('CUACCR')}} AS CUACCR,
        {{ trim_string('CUACPC')}} AS CUACPC,
        {{ trim_string('CUTEL')}} AS CUTEL,
        {{ trim_string('CUFAX')}} AS CUFAX,
        {{ trim_string('CUSTDS')}} AS CUSTDS,
        {{ trim_string('CUSTOR')}} AS CUSTOR,
        CAST(
            CASE
                WHEN STG_ESRI.ESRI_PROCESSED_INDICATOR = 'Y' THEN STG_ESRI.ESRI_ADDRESS_LINE_1
                WHEN CTE_FINAL.CUADR1 IS NULL OR LTRIM(RTRIM(CTE_FINAL.CUADR1)) = '' THEN
                    CASE WHEN CTE_FINAL.CUADR2 IS NULL OR LTRIM(RTRIM(CTE_FINAL.CUADR2)) = '' THEN CTE_FINAL.CUADR3 ELSE CTE_FINAL.CUADR2 END
                ELSE CTE_FINAL.CUADR1
            END
            AS VARCHAR(128)
        ) AS CORRECTED_ADDRESS_LINE_1,
        CAST(
            CASE
                WHEN STG_ESRI.ESRI_PROCESSED_INDICATOR = 'Y' THEN STG_ESRI.ESRI_ADDRESS_LINE_2
                WHEN CTE_FINAL.CUADR1 IS NULL OR LTRIM(RTRIM(CTE_FINAL.CUADR1)) = '' THEN
                    CASE WHEN CTE_FINAL.CUADR2 IS NULL OR LTRIM(RTRIM(CTE_FINAL.CUADR2)) = '' THEN '' ELSE CTE_FINAL.CUADR3 END
                ELSE
                    CASE WHEN CTE_FINAL.CUADR2 IS NULL OR LTRIM(RTRIM(CTE_FINAL.CUADR2)) = '' THEN CTE_FINAL.CUADR3 ELSE CTE_FINAL.CUADR2 END
            END
            AS VARCHAR(128)
        ) AS CORRECTED_ADDRESS_LINE_2,
        CAST(
            CASE
                WHEN STG_ESRI.ESRI_PROCESSED_INDICATOR = 'Y' THEN STG_ESRI.ESRI_ADDRESS_LINE_3
                WHEN CTE_FINAL.CUADR1 IS NULL OR LTRIM(RTRIM(CTE_FINAL.CUADR1)) = '' THEN ''
                ELSE
                    CASE WHEN CTE_FINAL.CUADR2 IS NULL OR LTRIM(RTRIM(CTE_FINAL.CUADR2)) = '' THEN '' ELSE CTE_FINAL.CUADR3 END
            END
            AS VARCHAR(128)
        ) AS CORRECTED_ADDRESS_LINE_3,
        CAST(CASE WHEN STG_ESRI.ESRI_PROCESSED_INDICATOR = 'Y' THEN STG_ESRI.ESRI_CITY ELSE CTE_FINAL.CUCITY END AS VARCHAR(50)) AS CORRECTED_CITY,
        CAST(CASE WHEN STG_ESRI.ESRI_PROCESSED_INDICATOR = 'Y' THEN STG_ESRI.ESRI_STATE ELSE CTE_FINAL.CUSTAT END AS VARCHAR(50)) AS CORRECTED_STATE,
        CAST(UPPER(COALESCE(STG_ESRI.ESRI_COUNTY, '')) AS VARCHAR(50)) AS CORRECTED_COUNTY,
        CAST(
            CASE
                WHEN STG_ESRI.ESRI_PROCESSED_INDICATOR = 'Y' THEN STG_ESRI.ESRI_POSTAL_CODE
                WHEN CTE_FINAL.CUPOST IS NULL THEN ''
                WHEN REGEXP_LIKE(LTRIM(RTRIM(CTE_FINAL.CUPOST)), '(\\d{5}-\\d{4})') THEN SUBSTR(LTRIM(RTRIM(CTE_FINAL.CUPOST)), 1, 5)
                WHEN REGEXP_LIKE(LTRIM(RTRIM(CTE_FINAL.CUPOST)), '(\\d{9})') THEN SUBSTR(LTRIM(RTRIM(CTE_FINAL.CUPOST)), 1, 5)
                WHEN REGEXP_LIKE(LTRIM(RTRIM(CTE_FINAL.CUPOST)), '(\\d{5} \\d{4})') THEN SUBSTR(LTRIM(RTRIM(CTE_FINAL.CUPOST)), 1, 5)
                WHEN REGEXP_LIKE(LTRIM(RTRIM(CTE_FINAL.CUPOST)), '(\\d{5}-)') THEN SUBSTR(LTRIM(RTRIM(CTE_FINAL.CUPOST)), 1, 5)
                ELSE LTRIM(RTRIM(CTE_FINAL.CUPOST))
            END
            AS VARCHAR(10)
        ) AS CORRECTED_POSTAL_CODE,
        CAST(
            CASE
                WHEN STG_ESRI.ESRI_PROCESSED_INDICATOR = 'Y' THEN STG_ESRI.ESRI_POSTAL_SUFFIX
                WHEN CTE_FINAL.CUPOST IS NULL THEN ''
                WHEN REGEXP_LIKE(LTRIM(RTRIM(CTE_FINAL.CUPOST)), '(\\d{5}-\\d{4})') THEN SUBSTR(LTRIM(RTRIM(CTE_FINAL.CUPOST)), 7, 4)
                WHEN REGEXP_LIKE(LTRIM(RTRIM(CTE_FINAL.CUPOST)), '(\\d{9})') THEN SUBSTR(LTRIM(RTRIM(CTE_FINAL.CUPOST)), 6, 4)
                WHEN REGEXP_LIKE(LTRIM(RTRIM(CTE_FINAL.CUPOST)), '(\\d{5} \\d{4})') THEN SUBSTR(LTRIM(RTRIM(CTE_FINAL.CUPOST)), 7, 4)
                ELSE ''
            END
            AS VARCHAR(4)
        ) AS CORRECTED_POSTAL_SUFFIX,
        CAST(
            UPPER(
                CASE
                    WHEN STG_ESRI.ESRI_PROCESSED_INDICATOR = 'Y' THEN STG_ESRI.ESRI_COUNTRY_CODE
                    WHEN REGEXP_REPLACE(UPPER(LTRIM(RTRIM(CTE_FINAL.CUCTRY))), '[^A-Za-z ]', '') IN ('USA', 'US', 'UNITED STATES') THEN 'USA'
                    ELSE UPPER(LTRIM(RTRIM(CTE_FINAL.CUCTRY)))
                END
            )
            AS VARCHAR(5)
        ) AS CORRECTED_COUNTRY_CODE,
        CAST(
            CASE
                WHEN STG_ESRI.ESRI_PROCESSED_INDICATOR = 'Y' THEN STG_ESRI.ESRI_COUNTRY_NAME
                WHEN REGEXP_REPLACE(UPPER(LTRIM(RTRIM(CTE_FINAL.CUCTRY))), '[^A-Za-z ]', '') IN ('USA', 'US', 'UNITED STATES') THEN 'UNITED STATES OF AMERICA'
                ELSE UPPER(LTRIM(RTRIM(CTE_FINAL.CUCTRY)))
            END
            AS VARCHAR(50)
        ) AS CORRECTED_COUNTRY_NAME,
        CAST(
            CASE
                WHEN STG_ESRI.ESRI_PROCESSED_INDICATOR = 'Y' THEN STG_ESRI.ESRI_COMPLETEADDRESS_MD5
                ELSE MD5(
                    COALESCE(
                        CASE
                            WHEN STG_ESRI.ESRI_PROCESSED_INDICATOR = 'Y' THEN STG_ESRI.ESRI_ADDRESS_LINE_1
                            WHEN CTE_FINAL.CUADR1 IS NULL OR LTRIM(RTRIM(CTE_FINAL.CUADR1)) = '' THEN
                                CASE WHEN CTE_FINAL.CUADR2 IS NULL OR LTRIM(RTRIM(CTE_FINAL.CUADR2)) = '' THEN CTE_FINAL.CUADR3 ELSE CTE_FINAL.CUADR2 END
                            ELSE CTE_FINAL.CUADR1
                        END, ''
                    ) ||
                    COALESCE(
                        CASE
                            WHEN STG_ESRI.ESRI_PROCESSED_INDICATOR = 'Y' THEN STG_ESRI.ESRI_ADDRESS_LINE_2
                            WHEN CTE_FINAL.CUADR1 IS NULL OR LTRIM(RTRIM(CTE_FINAL.CUADR1)) = '' THEN
                                CASE WHEN CTE_FINAL.CUADR2 IS NULL OR LTRIM(RTRIM(CTE_FINAL.CUADR2)) = '' THEN '' ELSE CTE_FINAL.CUADR3 END
                            ELSE
                                CASE WHEN CTE_FINAL.CUADR2 IS NULL OR LTRIM(RTRIM(CTE_FINAL.CUADR2)) = '' THEN CTE_FINAL.CUADR3 ELSE CTE_FINAL.CUADR2 END
                        END, ''
                    ) ||
                    COALESCE(
                        CASE
                            WHEN STG_ESRI.ESRI_PROCESSED_INDICATOR = 'Y' THEN STG_ESRI.ESRI_ADDRESS_LINE_3
                            WHEN CTE_FINAL.CUADR1 IS NULL OR LTRIM(RTRIM(CTE_FINAL.CUADR1)) = '' THEN ''
                            ELSE CASE WHEN CTE_FINAL.CUADR2 IS NULL OR LTRIM(RTRIM(CTE_FINAL.CUADR2)) = '' THEN '' ELSE CTE_FINAL.CUADR3 END
                        END, ''
                    ) ||
                    COALESCE(CASE WHEN STG_ESRI.ESRI_PROCESSED_INDICATOR = 'Y' THEN STG_ESRI.ESRI_CITY ELSE CTE_FINAL.CUCITY END, '') ||
                    COALESCE(CASE WHEN STG_ESRI.ESRI_PROCESSED_INDICATOR = 'Y' THEN STG_ESRI.ESRI_STATE ELSE CTE_FINAL.CUSTAT END, '') ||
                    COALESCE(
                        CASE
                            WHEN STG_ESRI.ESRI_PROCESSED_INDICATOR = 'Y' THEN STG_ESRI.ESRI_POSTAL_CODE
                            WHEN CTE_FINAL.CUPOST IS NULL THEN ''
                            WHEN REGEXP_LIKE(LTRIM(RTRIM(CTE_FINAL.CUPOST)), '(\\d{5}-\\d{4})') THEN SUBSTR(LTRIM(RTRIM(CTE_FINAL.CUPOST)), 1, 5)
                            WHEN REGEXP_LIKE(LTRIM(RTRIM(CTE_FINAL.CUPOST)), '(\\d{9})') THEN SUBSTR(LTRIM(RTRIM(CTE_FINAL.CUPOST)), 1, 5)
                            WHEN REGEXP_LIKE(LTRIM(RTRIM(CTE_FINAL.CUPOST)), '(\\d{5} \\d{4})') THEN SUBSTR(LTRIM(RTRIM(CTE_FINAL.CUPOST)), 1, 5)
                            WHEN REGEXP_LIKE(LTRIM(RTRIM(CTE_FINAL.CUPOST)), '(\\d{5}-)') THEN SUBSTR(LTRIM(RTRIM(CTE_FINAL.CUPOST)), 1, 5)
                            ELSE LTRIM(RTRIM(CTE_FINAL.CUPOST))
                        END, ''
                    ) ||
                    COALESCE(
                        CASE
                            WHEN STG_ESRI.ESRI_PROCESSED_INDICATOR = 'Y' THEN STG_ESRI.ESRI_POSTAL_SUFFIX
                            WHEN CTE_FINAL.CUPOST IS NULL THEN ''
                            WHEN REGEXP_LIKE(LTRIM(RTRIM(CTE_FINAL.CUPOST)), '(\\d{5}-\\d{4})') THEN SUBSTR(LTRIM(RTRIM(CTE_FINAL.CUPOST)), 7, 4)
                            WHEN REGEXP_LIKE(LTRIM(RTRIM(CTE_FINAL.CUPOST)), '(\\d{9})') THEN SUBSTR(LTRIM(RTRIM(CTE_FINAL.CUPOST)), 6, 4)
                            WHEN REGEXP_LIKE(LTRIM(RTRIM(CTE_FINAL.CUPOST)), '(\\d{5} \\d{4})') THEN SUBSTR(LTRIM(RTRIM(CTE_FINAL.CUPOST)), 7, 4)
                            ELSE ''
                        END, ''
                    )
                )
            END
            AS VARCHAR(32)
        ) AS CORRECTED_COMPLETEADDRESS_MD5,
        CAST(CTE_FINAL.CUTEL AS VARCHAR(50)) AS CORRECTED_TELEPHONE_NUMBER,
        CAST(CTE_FINAL.CUFAX AS VARCHAR(50)) AS CORRECTED_FAX_NUMBER,
        CAST(CONCAT({{ trim_string('CTE_FINAL.CUCO')}} ,'-',{{ trim_string('CTE_FINAL.CUCUNO')}}) AS VARCHAR(50)) AS ACCOUNT_COMPOSITE_KEY,
        STG_ESRI.ESRI_COMPLETEADDRESS_MD5 AS ESRI_COMPLETEADDRESS_MD5,
        STG_ESRI.ESRI_ADDRESS_LINE_1 AS ESRI_ADDRESS_LINE_1,
        STG_ESRI.ESRI_ADDRESS_LINE_2 AS ESRI_ADDRESS_LINE_2,
        STG_ESRI.ESRI_ADDRESS_LINE_3 AS ESRI_ADDRESS_LINE_3,
        STG_ESRI.ESRI_CITY AS ESRI_CITY,
        STG_ESRI.ESRI_STATE AS ESRI_STATE,
        STG_ESRI.ESRI_POSTAL_CODE AS ESRI_POSTAL_CODE,
        STG_ESRI.ESRI_POSTAL_SUFFIX AS ESRI_POSTAL_SUFFIX,
        STG_ESRI.ESRI_COUNTRY_CODE AS ESRI_COUNTRY_CODE,
        STG_ESRI.ESRI_COUNTRY_NAME AS ESRI_COUNTRY_NAME,
        STG_ESRI.ESRI_LONGITUDE AS ESRI_LONGITUDE,
        STG_ESRI.ESRI_LATITUDE AS ESRI_LATITUDE,
        STG_ESRI.ESRI_PROCESSED_INDICATOR AS ESRI_PROCESSED_INDICATOR,
        STG_ESRI.ESRI_ERROR_INDICATOR AS ESRI_ERROR_INDICATOR,
        STG_ESRI.ESRI_MATCHCONFIDENCESCORE AS ESRI_MATCHCONFIDENCESCORE,
        STG_ESRI.ESRI_LOCATIONCONFIDENCESCORE AS ESRI_LOCATIONCONFIDENCESCORE,
        STG_ESRI.ESRI_LOCATIONCODE AS ESRI_LOCATIONCODE,
        STG_ESRI.ESRI_LOCATIONCODE_DESC AS ESRI_LOCATIONCODE_DESC,
        STG_ESRI.ESRI_COUNTY AS ESRI_COUNTY,
        STG_ESRI.ESRI_RESIDENTIAL_BUSINESS AS ESRI_RESIDENTIAL_BUSINESS,
        STG_ESRI.ESRI_APN AS ESRI_APN,
        STG_ESRI.ESRI_APN_USECODE AS ESRI_APN_USECODE,
        STG_ESRI.ESRI_APN_USE_DESC AS ESRI_APN_USE_DESC,
        STG_ESRI.ESRI_APN_UNIT_COUNT AS ESRI_APN_UNIT_COUNT,
        CAST(CTE_FINAL.SOURCE_SYSTEM AS VARCHAR(50)) AS SOURCE_SYSTEM,
        CAST('STG_IFP_ARPCU' AS VARCHAR(200)) AS SOURCE_TABLE,
        to_timestamp_ntz(to_timestamp_tz(SUBSTRING(TRIM(CTE_FINAL.DTL__CAPXTIMESTAMP),1,17),'YYYYMMDDHHMISSFF')) AS SRC_CREATE_DTM,
        to_timestamp_ntz(to_timestamp_tz(SUBSTRING(TRIM(CTE_FINAL.DTL__CAPXTIMESTAMP),1,17),'YYYYMMDDHHMISSFF')) AS SRC_LAST_UPDATE_DTM,
        TRIM(CTE_FINAL.DTL__CAPXACTION) AS SRC_ACTION_CD,
        CASE WHEN TRIM(CTE_FINAL.DTL__CAPXACTION) = 'D' THEN 'Y' ELSE 'N' END AS SRC_DEL_IND,
        CAST(BATCH_ID AS BIGINT) AS ODS_INS_BATCH_ID,
        CAST(BATCH_ID AS BIGINT) AS ODS_UPD_BATCH_ID,
        BATCH_TIMESTAMP AS ODS_INSERT_TIMESTAMP,
        BATCH_TIMESTAMP AS ODS_UPDATE_TIMESTAMP,
        CAST(CTE_FINAL.STG_INSERT_TIMESTAMP AS TIMESTAMP_NTZ) AS STG_INSERT_TIMESTAMP,
        CASE WHEN SRC_DEL_IND = 'Y' THEN 'DELETE' ELSE 'INSERT' END AS OPERATION_TYPE, --overwrite operation type as UPDATE/DELETE in merge operation
        CAST(NULL AS VARCHAR(20)) AS DQ_STATUS,
        CAST(NULL AS VARCHAR(50)) AS DQ_ERROR_CODE,
        CAST(NULL AS VARCHAR(500)) AS DQ_ERROR_MESSAGE
from CTE_FINAL
LEFT JOIN CTE_BATCH_ID ON 1=1
LEFT JOIN {{ ref('STG_IFP_ARPCU_ADDRESS') }} STG_ESRI
    ON TRIM(CTE_FINAL.CUCO) = STG_ESRI.CUCO
    AND TRIM(CTE_FINAL.CUCUNO) = STG_ESRI.CUCUNO
)

{% else %}
,CTE_HISTORICAL AS (
SELECT
        {{ generate_surrogate_key([trim_string('CUCO'),trim_string('CUCUNO')]) }} AS ODS_IFP_ARPCU_PK,
        CAST(CONCAT({{ trim_string('CUCO')}},'-',{{ trim_string('CUCUNO')}}) AS VARCHAR(50)) AS ODS_IFP_ARPCU_COMPOSITE_KEY,
        CAST(NULL AS NUMBER) AS STG_IFP_ARPCU_PK,
        {{ trim_string('SYS_CDC_LIB') }} AS SRC_CDC_LIB,
        {{ trim_string('CUCO')}} AS CUCO,
        {{ trim_string('CUCUNO')}} AS CUCUNO,
        {{ trim_string('CUNANO')}} AS CUNANO,
        CAST(CULKNO AS NUMBER(10,0)) AS CULKNO,
        {{ trim_string('CUTXID')}} AS CUTXID,
        {{ trim_string('CUSLID')}} AS CUSLID,
        {{ trim_string('CUSMAN')}} AS CUSMAN,
        {{ trim_string('CUNAME')}} AS CUNAME,
        {{ trim_string('CUADR1')}} AS CUADR1,
        {{ trim_string('CUADR2')}} AS CUADR2,
        {{ trim_string('CUADR3')}} AS CUADR3,
        {{ trim_string('CUCITY')}} AS CUCITY,
        {{ trim_string('CUSTAT')}} AS CUSTAT,
        {{ trim_string('CUPOST')}} AS CUPOST,
        {{ trim_string('CUCTRY')}} AS CUCTRY,
        {{ trim_string('CUALIA')}} AS CUALIA,
        {{ trim_string('CUPCO')}} AS CUPCO,
        {{ trim_string('CUPCNO')}} AS CUPCNO,
        {{ trim_string('CUCATG')}} AS CUCATG,
        {{ trim_string('CUMACL')}} AS CUMACL,
        {{ trim_string('CUMICL')}} AS CUMICL,
        {{ trim_string('CUSNAM')}} AS CUSNAM,
        {{ trim_string('CUAPST')}} AS CUAPST,
        {{ trim_string('CUOTCC')}} AS CUOTCC,
        {{ trim_string('CUAICD')}} AS CUAICD,
        {{ trim_string('CUUFL1')}} AS CUUFL1,
        {{ trim_string('CUUFL2')}} AS CUUFL2,
        {{ trim_string('CUUFL3')}} AS CUUFL3,
        {{ trim_string('CUUFL4')}} AS CUUFL4,
        {{ trim_string('CUUFL5')}} AS CUUFL5,
        CAST(CUUAM1 AS NUMBER(13,2)) AS CUUAM1,
        CAST(CUUAM2 AS NUMBER(13,2)) AS CUUAM2,
        CAST(CUUDTH AS NUMBER(10,0)) AS CUUDTH,
        {{ trim_string('CUUDTE')}} AS CUUDTE,
        CAST(CUUDT8 AS NUMBER(10,0)) AS CUUDT8,
        {{ trim_string('CUARTC')}} AS CUARTC,
        {{ trim_string('CUFCPC')}} AS CUFCPC,
        {{ trim_string('CUCTPC')}} AS CUCTPC,
        {{ trim_string('CUDGPC')}} AS CUDGPC,
        {{ trim_string('CUFGPC')}} AS CUFGPC,
        {{ trim_string('CUANPC')}} AS CUANPC,
        {{ trim_string('CULPPC')}} AS CULPPC,
        {{ trim_string('CUPTPC')}} AS CUPTPC,
        {{ trim_string('CUSTPC')}} AS CUSTPC,
        {{ trim_string('CUDCPC')}} AS CUDCPC,
        {{ trim_string('CURMPC')}} AS CURMPC,
        {{ trim_string('CUAGC')}} AS CUAGC,
        {{ trim_string('CUOADC')}} AS CUOADC,
        {{ trim_string('CUEXHC')}} AS CUEXHC,
        {{ trim_string('CUEXST')}} AS CUEXST,
        {{ trim_string('CUEXDL')}} AS CUEXDL,
        {{ trim_string('CUEXFC')}} AS CUEXFC,
        {{ trim_string('CUEXLC')}} AS CUEXLC,
        {{ trim_string('CUEXTT')}} AS CUEXTT,
        {{ trim_string('CUAPPC')}} AS CUAPPC,
        {{ trim_string('CUACCC')}} AS CUACCC,
        {{ trim_string('CUACCR')}} AS CUACCR,
        {{ trim_string('CUACPC')}} AS CUACPC,
        {{ trim_string('CUTEL')}} AS CUTEL,
        {{ trim_string('CUFAX')}} AS CUFAX,
        {{ trim_string('CUSTDS')}} AS CUSTDS,
        {{ trim_string('CUSTOR')}} AS CUSTOR,
        CAST(CORRECTED_ADDRESS_LINE_1 AS VARCHAR(128)) AS CORRECTED_ADDRESS_LINE_1,
        CAST(CORRECTED_ADDRESS_LINE_2 AS VARCHAR(128)) AS CORRECTED_ADDRESS_LINE_2,
        CAST(CORRECTED_ADDRESS_LINE_3 AS VARCHAR(128)) AS CORRECTED_ADDRESS_LINE_3,
        CAST(CORRECTED_CITY AS VARCHAR(50)) AS CORRECTED_CITY,
        CAST(CORRECTED_STATE AS VARCHAR(50)) AS CORRECTED_STATE,
        CAST(CORRECTED_COUNTY AS VARCHAR(50)) AS CORRECTED_COUNTY,
        CAST(CORRECTED_POSTAL_CODE AS VARCHAR(10)) AS CORRECTED_POSTAL_CODE,
        CAST(CORRECTED_POSTAL_SUFFIX AS VARCHAR(4)) AS CORRECTED_POSTAL_SUFFIX,
        CAST(CORRECTED_COUNTRY_CODE AS VARCHAR(5)) AS CORRECTED_COUNTRY_CODE,
        CAST(CORRECTED_COUNTRY_NAME AS VARCHAR(50)) AS CORRECTED_COUNTRY_NAME,
        CAST(CORRECTED_COMPLETEADDRESS_MD5 AS VARCHAR(32)) AS CORRECTED_COMPLETEADDRESS_MD5,
        CAST(CORRECTED_TELEPHONE_NUMBER AS VARCHAR(50)) AS CORRECTED_TELEPHONE_NUMBER,
        CAST(CORRECTED_FAX_NUMBER AS VARCHAR(50)) AS CORRECTED_FAX_NUMBER,
        CAST(CONCAT({{ trim_string('CUCO')}},'-',{{ trim_string('CUCUNO')}}) AS VARCHAR(50)) AS ACCOUNT_COMPOSITE_KEY,
        CAST(ESRI_COMPLETEADDRESS_MD5 AS VARCHAR(32)) AS ESRI_COMPLETEADDRESS_MD5,
        CAST(ESRI_ADDRESS_LINE_1 AS VARCHAR(128)) AS ESRI_ADDRESS_LINE_1,
        CAST(ESRI_ADDRESS_LINE_2 AS VARCHAR(128)) AS ESRI_ADDRESS_LINE_2,
        CAST(ESRI_ADDRESS_LINE_3 AS VARCHAR(128)) AS ESRI_ADDRESS_LINE_3,
        CAST(ESRI_CITY AS VARCHAR(50)) AS ESRI_CITY,
        CAST(ESRI_STATE AS VARCHAR(50)) AS ESRI_STATE,
        CAST(ESRI_POSTAL_CODE AS VARCHAR(10)) AS ESRI_POSTAL_CODE,
        CAST(ESRI_POSTAL_SUFFIX AS VARCHAR(4)) AS ESRI_POSTAL_SUFFIX,
        CAST(ESRI_COUNTRY_CODE AS VARCHAR(5)) AS ESRI_COUNTRY_CODE,
        CAST(ESRI_COUNTRY_NAME AS VARCHAR(50)) AS ESRI_COUNTRY_NAME,
        CAST(ESRI_LONGITUDE AS VARCHAR(30)) AS ESRI_LONGITUDE,
        CAST(ESRI_LATITUDE AS VARCHAR(30)) AS ESRI_LATITUDE,
        CAST(ESRI_PROCESSED_INDICATOR AS VARCHAR(1)) AS ESRI_PROCESSED_INDICATOR,
        CAST(ESRI_ERROR_INDICATOR AS VARCHAR(1)) AS ESRI_ERROR_INDICATOR,
        CAST(ESRI_MATCHCONFIDENCESCORE AS VARCHAR(10)) AS ESRI_MATCHCONFIDENCESCORE,
        CAST(ESRI_LOCATIONCONFIDENCESCORE AS VARCHAR(10)) AS ESRI_LOCATIONCONFIDENCESCORE,
        CAST(ESRI_LOCATIONCODE AS VARCHAR(10)) AS ESRI_LOCATIONCODE,
        CAST(ESRI_LOCATIONCODE_DESC AS VARCHAR(100)) AS ESRI_LOCATIONCODE_DESC,
        CAST(ESRI_COUNTY AS VARCHAR(128)) AS ESRI_COUNTY,
        CAST(ESRI_RESIDENTIAL_BUSINESS AS VARCHAR(100)) AS ESRI_RESIDENTIAL_BUSINESS,
        CAST(ESRI_APN AS VARCHAR(100)) AS ESRI_APN,
        CAST(ESRI_APN_USECODE AS VARCHAR(100)) AS ESRI_APN_USECODE,
        CAST(ESRI_APN_USE_DESC AS VARCHAR(100)) AS ESRI_APN_USE_DESC,
        CAST(ESRI_APN_UNIT_COUNT AS VARCHAR(100)) AS ESRI_APN_UNIT_COUNT,
        CAST('Infopro' AS VARCHAR(50)) AS SOURCE_SYSTEM,
        CAST('ODS_IFP_ARPCU_HIST' AS VARCHAR(200)) AS SOURCE_TABLE,
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
        CAST(NULL AS VARCHAR(50)) AS DQ_ERROR_CODE,
        CAST(NULL AS VARCHAR(500)) AS DQ_ERROR_MESSAGE
FROM INFOPRO.HISTORY.ODS_IFP_ARPCU_HIST
CROSS JOIN CTE_BATCH_ID
)

{% endif %}

SELECT
*,
{{ generate_hash_change( relation = this) }} AS HASH_CHANGE,
{{ generate_hash_full( relation = this) }} AS HASH_FULL
FROM {% if is_incremental() %}CTE_TRANSFORMED{% else %}CTE_HISTORICAL{% endif %}






