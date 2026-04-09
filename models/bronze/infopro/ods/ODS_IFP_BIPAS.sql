{{
        config(
                materialized='incremental',
                unique_key='ODS_IFP_BIPAS_PK',
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
                                 WHERE ODS_IFP_BIPAS_PK IN (
                                     SELECT {{ generate_surrogate_key([trim_string('DTL__BI_ASCOMP'),trim_string('DTL__BI_ASACCT'),trim_string('DTL__BI_ASSITE')]) }}
                                     FROM {{ source('STAGING','STG_IFP_BIPAS') }}
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
 
 
-- depends_on: {{ ref('STG_IFP_BIPAS_ADDRESS') }}
--> Grab all the records newer than last update
WITH CTE_BATCH_ID AS (
    SELECT TO_NUMBER(TO_VARCHAR(CURRENT_TIMESTAMP, 'YYYYMMDDHH24MISSFF3')) AS BATCH_ID,
    CAST(CURRENT_TIMESTAMP AS TIMESTAMP_NTZ)  AS BATCH_TIMESTAMP
)
 
{% if is_incremental() %}
,CTE_SOURCE_RECORDS AS (
        SELECT * FROM {{ source('STAGING','STG_IFP_BIPAS') }}
        where
        -- this filter will only be applied on an incremental run
        to_timestamp_tz(SUBSTRING(TRIM(DTL__CAPXTIMESTAMP),1,17),'YYYYMMDDHHMISSFF')  >= '{{ get_max_event_time('SRC_LAST_UPDATE_DTM') }}'
)
 
,CTE_DEDUP as (
  select *, ROW_NUMBER() OVER(PARTITION BY ASCOMP,ASACCT,ASSITE ORDER BY DTL__CAPXTIMESTAMP DESC) AS ROW_NUM
  from CTE_SOURCE_RECORDS
  qualify ROW_NUM=1
)
 
,CTE_TEMP as (
select CTE_SOURCE_RECORDS.DTL__BI_ASCOMP, CTE_SOURCE_RECORDS.DTL__BI_ASACCT, CTE_SOURCE_RECORDS.DTL__BI_ASSITE from
CTE_SOURCE_RECORDS LEFT JOIN CTE_DEDUP
 ON CTE_SOURCE_RECORDS.DTL__BI_ASCOMP=CTE_DEDUP.ASCOMP
 AND CTE_SOURCE_RECORDS.DTL__BI_ASACCT=CTE_DEDUP.ASACCT
 AND CTE_SOURCE_RECORDS.DTL__BI_ASSITE=CTE_DEDUP.ASSITE
 where CTE_SOURCE_RECORDS.PRIMARY_KEY_CHANGE  = 'Y'
AND (TO_NUMBER(CTE_SOURCE_RECORDS.PK_LZ_IFP_BIPAS_ID) >=  TO_NUMBER(CTE_DEDUP.PK_LZ_IFP_BIPAS_ID) OR TO_NUMBER(CTE_DEDUP.PK_LZ_IFP_BIPAS_ID) IS NULL)
)
 
,CTE_FINAL as (
select * from CTE_DEDUP where CONCAT_WS(',',ASCOMP,ASACCT,ASSITE) in (
select CONCAT_WS(',',ASCOMP,ASACCT,ASSITE) from CTE_DEDUP
except
select CONCAT_WS(',',DTL__BI_ASCOMP,DTL__BI_ASACCT,DTL__BI_ASSITE) from CTE_TEMP)
)
 
,--> Current Delta data capture from STG table
CTE_TRANSFORMED AS (
SELECT
        {{ generate_surrogate_key([trim_string('CTE_FINAL.ASCOMP'),trim_string('CTE_FINAL.ASACCT'),trim_string('CTE_FINAL.ASSITE')])}} as ODS_IFP_BIPAS_PK ,
        CAST(CONCAT({{ trim_string('CTE_FINAL.ASCOMP')}},'-',{{ trim_string('CTE_FINAL.ASACCT')}},'-',{{ trim_string('CTE_FINAL.ASSITE')}}) AS VARCHAR(50)) AS ODS_IFP_BIPAS_COMPOSITE_KEY,
        CTE_FINAL.STG_IFP_BIPAS_PK,
        {{ trim_string('CTE_FINAL.DTL__CAPXUSER') }} AS SRC_CDC_LIB,
        {{ trim_string('CTE_FINAL.ASCOMP')}} AS ASCOMP,
        {{ trim_string('CTE_FINAL.ASACCT')}} AS ASACCT,
        {{ trim_string('CTE_FINAL.ASSITE')}} AS ASSITE,
        {{ trim_string('CTE_FINAL.ASSNAM')}} AS ASSNAM,
        {{ trim_string('CTE_FINAL.ASADNO')}} AS ASADNO,
        {{ trim_string('CTE_FINAL.ASADNM')}} AS ASADNM,
        {{ trim_string('CTE_FINAL.ASSUIT')}} AS ASSUIT,
        {{ trim_string('CTE_FINAL.ASSTTY')}} AS ASSTTY,
        {{ trim_string('CTE_FINAL.ASSTDR')}} AS ASSTDR,
        {{ trim_string('CTE_FINAL.ASSADR')}} AS ASSADR,
        {{ trim_string('CTE_FINAL.ASADR2')}} AS ASADR2,
        {{ trim_string('CTE_FINAL.ASCITY')}} AS ASCITY,
        {{ trim_string('CTE_FINAL.ASSTPR')}} AS ASSTPR,
        {{ trim_string('CTE_FINAL.ASZPPC')}} AS ASZPPC,
        {{ trim_string('CTE_FINAL.ASCTRY')}} AS ASCTRY,
        CAST(CTE_FINAL.ASARCD AS NUMBER(5,0)) AS ASARCD,
        CAST(CTE_FINAL.ASPHNE AS NUMBER(10,0)) AS ASPHNE,
        CAST(CTE_FINAL.ASPEXT AS NUMBER(5,0)) AS ASPEXT,
        {{ trim_string('CTE_FINAL.ASCNAM')}} AS ASCNAM,
        {{ trim_string('CTE_FINAL.ASCTTL')}} AS ASCTTL,
        {{ trim_string('CTE_FINAL.ASANAM')}} AS ASANAM,
        {{ trim_string('CTE_FINAL.ASATTL')}} AS ASATTL,
        CAST(CTE_FINAL.ASTERR AS NUMBER(5,0)) AS ASTERR,
        {{ trim_string('CTE_FINAL.ASSLNO')}} AS ASSLNO,
        {{ trim_string('CTE_FINAL.ASCTRT')}} AS ASCTRT,
        CAST(CTE_FINAL.ASTERM AS NUMBER(5,0)) AS ASTERM,
        {{ trim_string('CTE_FINAL.ASCTST')}} AS ASCTST,
        CAST(CTE_FINAL.ASSTDT AS NUMBER(10,0)) AS ASSTDT_ORIG,
        {{ DateCheck_140_YYYYMMDD('CTE_FINAL.ASSTDT') }} AS ASSTDT,
        CAST(CTE_FINAL.ASONDT AS NUMBER(10,0)) AS ASONDT_ORIG,
        {{ DateCheck_140_YYYYMMDD('CTE_FINAL.ASONDT') }} AS ASONDT,
        CAST(CTE_FINAL.ASCLDT AS NUMBER(10,0)) AS ASCLDT_ORIG,
        {{ DateCheck_140_YYYYMMDD_HighDate('CTE_FINAL.ASCLDT') }} AS ASCLDT,
        CAST(CTE_FINAL.ASRVDT AS NUMBER(10,0)) AS ASRVDT_ORIG,
        {{ DateCheck_140_YYYYMMDD('CTE_FINAL.ASRVDT') }} AS ASRVDT,
        CAST(CTE_FINAL.ASEXDT AS NUMBER(10,0)) AS ASEXDT_ORIG,
        {{ DateCheck_140_YYYYMMDD_HighDate('CTE_FINAL.ASEXDT') }} AS ASEXDT,
        {{ trim_string('CTE_FINAL.ASSCLS')}} AS ASSCLS,
        {{ trim_string('CTE_FINAL.ASTXCD')}} AS ASTXCD,
        {{ trim_string('CTE_FINAL.ASTXEX')}} AS ASTXEX,
        {{ trim_string('CTE_FINAL.ASPO')}} AS ASPO,
        {{ trim_string('CTE_FINAL.ASSPFL')}} AS ASSPFL,
        {{ trim_string('CTE_FINAL.ASSHRD')}} AS ASSHRD,
        {{ trim_string('CTE_FINAL.ASCPI')}} AS ASCPI,
        {{ trim_string('CTE_FINAL.ASOLDC')}} AS ASOLDC,
        {{ trim_string('CTE_FINAL.ASUSER')}} AS ASUSER,
        CAST(CTE_FINAL.ASUDAT AS NUMBER(10,0)) AS ASUDAT,
        CAST(CTE_FINAL.ASUTIM AS NUMBER(10,0)) AS ASUTIM,
        CAST(NULL AS VARCHAR(12)) AS CCT,
        CAST(
            CASE
                WHEN STG_ESRI.esri_processed_indicator = 'Y' THEN STG_ESRI.esri_address_line_1
                WHEN CTE_FINAL.ASSADR IS NULL OR LTRIM(RTRIM(CTE_FINAL.ASSADR)) = '' THEN CASE WHEN CTE_FINAL.ASADR2 IS NULL OR LTRIM(RTRIM(CTE_FINAL.ASADR2)) = '' THEN '' ELSE CTE_FINAL.ASADR2 END
                ELSE CTE_FINAL.ASSADR
            END
            AS VARCHAR(128)
        ) AS CORRECTED_ADDRESS_LINE_1,
        CAST(
            CASE
                WHEN STG_ESRI.esri_processed_indicator = 'Y' THEN STG_ESRI.esri_address_line_2
                WHEN CTE_FINAL.ASSADR IS NULL OR LTRIM(RTRIM(CTE_FINAL.ASSADR)) = '' THEN CASE WHEN CTE_FINAL.ASADR2 IS NULL OR LTRIM(RTRIM(CTE_FINAL.ASADR2)) = '' THEN '' ELSE '' END
                ELSE CASE WHEN CTE_FINAL.ASADR2 IS NULL OR LTRIM(RTRIM(CTE_FINAL.ASADR2)) = '' THEN '' ELSE CTE_FINAL.ASADR2 END
            END
            AS VARCHAR(128)
        ) AS CORRECTED_ADDRESS_LINE_2,
        CAST(CASE WHEN STG_ESRI.esri_processed_indicator = 'Y' THEN STG_ESRI.esri_address_line_3 ELSE '' END AS VARCHAR(128)) AS CORRECTED_ADDRESS_LINE_3,
        CAST(CASE WHEN STG_ESRI.esri_processed_indicator = 'Y' THEN STG_ESRI.esri_city ELSE CTE_FINAL.ASCITY END AS VARCHAR(50)) AS CORRECTED_CITY,
        CAST(CASE WHEN STG_ESRI.esri_processed_indicator = 'Y' THEN STG_ESRI.esri_state ELSE CTE_FINAL.ASSTPR END AS VARCHAR(50)) AS CORRECTED_STATE,
        CAST(UPPER(COALESCE(STG_ESRI.esri_county, '')) AS VARCHAR(50)) AS CORRECTED_COUNTY,
        CAST(
        CASE
        WHEN STG_ESRI.esri_processed_indicator = 'Y' THEN STG_ESRI.esri_postal_code
        WHEN CTE_FINAL.ASZPPC IS NULL THEN ''
        WHEN REGEXP_LIKE(TRIM(CTE_FINAL.ASZPPC), '\\d{5}-\\d{4}') THEN SUBSTRING(TRIM(CTE_FINAL.ASZPPC), 1, 5)
        WHEN REGEXP_LIKE(TRIM(CTE_FINAL.ASZPPC), '\\d{9}') THEN SUBSTRING(TRIM(CTE_FINAL.ASZPPC), 1, 5)
        WHEN REGEXP_LIKE(TRIM(CTE_FINAL.ASZPPC), '\\d{5} \\d{4}') THEN SUBSTRING(TRIM(CTE_FINAL.ASZPPC), 1, 5)
        WHEN REGEXP_LIKE(TRIM(CTE_FINAL.ASZPPC), '\\d{5}-') THEN SUBSTRING(TRIM(CTE_FINAL.ASZPPC), 1, 5)
        ELSE TRIM(CTE_FINAL.ASZPPC)
        END
        AS VARCHAR(10) ) AS CORRECTED_POSTAL_CODE,
        CAST(
            CASE
                WHEN STG_ESRI.esri_processed_indicator = 'Y' THEN STG_ESRI.esri_postal_suffix
                WHEN CTE_FINAL.ASZPPC IS NULL THEN ''
                WHEN REGEXP_LIKE(LTRIM(RTRIM(CTE_FINAL.ASZPPC)), '(\d{5}-\d{4})') THEN SUBSTR(LTRIM(RTRIM(CTE_FINAL.ASZPPC)), 7, 4)
                WHEN REGEXP_LIKE(LTRIM(RTRIM(CTE_FINAL.ASZPPC)), '(\d{9})') THEN SUBSTR(LTRIM(RTRIM(CTE_FINAL.ASZPPC)), 6, 4)
                WHEN REGEXP_LIKE(LTRIM(RTRIM(CTE_FINAL.ASZPPC)), '(\d{5} \d{4})') THEN SUBSTR(LTRIM(RTRIM(CTE_FINAL.ASZPPC)), 7, 4)
                ELSE ''
            END
            AS VARCHAR(4)
        ) AS CORRECTED_POSTAL_SUFFIX,
        CAST(
            UPPER(
                CASE
                    WHEN STG_ESRI.esri_processed_indicator = 'Y' THEN STG_ESRI.esri_country_code
                    WHEN REGEXP_REPLACE(UPPER(LTRIM(RTRIM(CTE_FINAL.ASCTRY))), '[^A-Za-z ]', '') IN ('USA', 'US', 'UNITED STATES') THEN 'USA'
                    ELSE UPPER(LTRIM(RTRIM(CTE_FINAL.ASCTRY)))
                END
            )
            AS VARCHAR(5)
        ) AS CORRECTED_COUNTRY_CODE,
        CAST(
            CASE
                WHEN STG_ESRI.esri_processed_indicator = 'Y' THEN STG_ESRI.esri_country_name
                WHEN REGEXP_REPLACE(UPPER(LTRIM(RTRIM(CTE_FINAL.ASCTRY))), '[^A-Za-z ]', '') IN ('USA', 'US', 'UNITED STATES') THEN 'UNITED STATES OF AMERICA'
                ELSE UPPER(LTRIM(RTRIM(CTE_FINAL.ASCTRY)))
            END
            AS VARCHAR(50)
        ) AS CORRECTED_COUNTRY_NAME,
        CAST(
            CASE
                WHEN STG_ESRI.esri_processed_indicator = 'Y' THEN STG_ESRI.esri_complete_address_md5
                ELSE MD5(
                    COALESCE(
                        CASE
                            WHEN STG_ESRI.esri_processed_indicator = 'Y' THEN STG_ESRI.esri_address_line_1
                            WHEN CTE_FINAL.ASSADR IS NULL OR LTRIM(RTRIM(CTE_FINAL.ASSADR)) = '' THEN CASE WHEN CTE_FINAL.ASADR2 IS NULL OR LTRIM(RTRIM(CTE_FINAL.ASADR2)) = '' THEN '' ELSE CTE_FINAL.ASADR2 END
                            ELSE CTE_FINAL.ASSADR
                        END,
                        ''
                    ) ||
                    COALESCE(
                        CASE
                            WHEN STG_ESRI.esri_processed_indicator = 'Y' THEN STG_ESRI.esri_address_line_2
                            WHEN CTE_FINAL.ASSADR IS NULL OR LTRIM(RTRIM(CTE_FINAL.ASSADR)) = '' THEN CASE WHEN CTE_FINAL.ASADR2 IS NULL OR LTRIM(RTRIM(CTE_FINAL.ASADR2)) = '' THEN '' ELSE '' END
                            ELSE CASE WHEN CTE_FINAL.ASADR2 IS NULL OR LTRIM(RTRIM(CTE_FINAL.ASADR2)) = '' THEN '' ELSE CTE_FINAL.ASADR2 END
                        END,
                        ''
                    ) ||
                    COALESCE(CASE WHEN STG_ESRI.esri_processed_indicator = 'Y' THEN STG_ESRI.esri_address_line_3 ELSE '' END, '') ||
                    COALESCE(CASE WHEN STG_ESRI.esri_processed_indicator = 'Y' THEN STG_ESRI.esri_city ELSE CTE_FINAL.ASCITY END, '') ||
                    COALESCE(CASE WHEN STG_ESRI.esri_processed_indicator = 'Y' THEN STG_ESRI.esri_state ELSE CTE_FINAL.ASSTPR END, '') ||
                    COALESCE(
                        CASE
                            WHEN STG_ESRI.esri_processed_indicator = 'Y' THEN STG_ESRI.esri_postal_code
                            WHEN CTE_FINAL.ASZPPC IS NULL THEN ''
                            WHEN REGEXP_LIKE(LTRIM(RTRIM(CTE_FINAL.ASZPPC)), '(\d{5}-\d{4})') THEN SUBSTR(LTRIM(RTRIM(CTE_FINAL.ASZPPC)), 1, 5)
                            WHEN REGEXP_LIKE(LTRIM(RTRIM(CTE_FINAL.ASZPPC)), '(\d{9})') THEN SUBSTR(LTRIM(RTRIM(CTE_FINAL.ASZPPC)), 1, 5)
                            WHEN REGEXP_LIKE(LTRIM(RTRIM(CTE_FINAL.ASZPPC)), '(\d{5} \d{4})') THEN SUBSTR(LTRIM(RTRIM(CTE_FINAL.ASZPPC)), 1, 5)
                            WHEN REGEXP_LIKE(LTRIM(RTRIM(CTE_FINAL.ASZPPC)), '(\d{5}-)') THEN SUBSTR(LTRIM(RTRIM(CTE_FINAL.ASZPPC)), 1, 5)
                            ELSE LTRIM(RTRIM(CTE_FINAL.ASZPPC))
                        END,
                        ''
                    ) ||
                    COALESCE(
                        CASE
                            WHEN STG_ESRI.esri_processed_indicator = 'Y' THEN STG_ESRI.esri_postal_suffix
                            WHEN CTE_FINAL.ASZPPC IS NULL THEN ''
                            WHEN REGEXP_LIKE(LTRIM(RTRIM(CTE_FINAL.ASZPPC)), '(\d{5}-\d{4})') THEN SUBSTR(LTRIM(RTRIM(CTE_FINAL.ASZPPC)), 7, 4)
                            WHEN REGEXP_LIKE(LTRIM(RTRIM(CTE_FINAL.ASZPPC)), '(\d{9})') THEN SUBSTR(LTRIM(RTRIM(CTE_FINAL.ASZPPC)), 6, 4)
                            WHEN REGEXP_LIKE(LTRIM(RTRIM(CTE_FINAL.ASZPPC)), '(\d{5} \d{4})') THEN SUBSTR(LTRIM(RTRIM(CTE_FINAL.ASZPPC)), 7, 4)
                            ELSE ''
                        END,
                        ''
                    )
                )
            END
            AS VARCHAR(32)
        ) AS CORRECTED_COMPLETEADDRESS_MD5,
        CAST(TO_VARCHAR(CTE_FINAL.ASARCD) || TO_VARCHAR(CTE_FINAL.ASPHNE) AS VARCHAR(50)) AS CORRECTED_TELEPHONE_NUMBER,
        CAST(NULL AS VARCHAR(50)) AS CORRECTED_FAX_NUMBER,
        CAST(STG_ESRI.esri_latitude AS NUMBER(11,6)) AS CORRECTED_AD_LATITUDE,
        CAST(STG_ESRI.esri_longitude AS NUMBER(11,6)) AS CORRECTED_AD_LONGITUDE,
        CAST(CONCAT({{ trim_string('CTE_FINAL.ASCOMP') }},'-',{{ trim_string('CTE_FINAL.ASACCT') }},'-',{{ trim_string('CTE_FINAL.ASSITE') }}) AS VARCHAR(50)) AS SITE_COMPOSITE_KEY,
        CASE
            WHEN STG_ESRI.esri_latitude IS NOT NULL
                 AND STG_ESRI.esri_longitude IS NOT NULL
                 AND STG_ESRI.esri_latitude >= -90 AND STG_ESRI.esri_latitude <= 90
                 AND STG_ESRI.esri_longitude >= -180 AND STG_ESRI.esri_longitude <= 180
            THEN ST_MAKEPOINT(
                CAST(STG_ESRI.esri_longitude AS FLOAT),
                CAST(STG_ESRI.esri_latitude AS FLOAT)
            )
            ELSE NULL
        END AS CORRECTED_GEOCODE,
        STG_ESRI.esri_complete_address_md5 AS ESRI_COMPLETEADDRESS_MD5,
        STG_ESRI.esri_address_line_1 AS ESRI_ADDRESS_LINE_1,
        STG_ESRI.esri_address_line_2 AS ESRI_ADDRESS_LINE_2,
        STG_ESRI.esri_address_line_3 AS ESRI_ADDRESS_LINE_3,
        STG_ESRI.esri_city AS ESRI_CITY,
        STG_ESRI.esri_state AS ESRI_STATE,
        STG_ESRI.esri_postal_code AS ESRI_POSTAL_CODE,
        STG_ESRI.esri_postal_suffix AS ESRI_POSTAL_SUFFIX,
        STG_ESRI.esri_country_code AS ESRI_COUNTRY_CODE,
        STG_ESRI.esri_country_name AS ESRI_COUNTRY_NAME,
        STG_ESRI.esri_longitude AS ESRI_LONGITUDE,
        STG_ESRI.esri_latitude AS ESRI_LATITUDE,
        STG_ESRI.esri_processed_indicator AS ESRI_PROCESSED_INDICATOR,
        STG_ESRI.esri_error_indicator AS ESRI_ERROR_INDICATOR,
        STG_ESRI.esri_matchconfidencescore AS ESRI_MATCHCONFIDENCESCORE,
        STG_ESRI.esri_locationconfidencescore AS ESRI_LOCATIONCONFIDENCESCORE,
        STG_ESRI.esri_locationcode AS ESRI_LOCATIONCODE,
        STG_ESRI.esri_locationcode_desc AS ESRI_LOCATIONCODE_DESC,
        STG_ESRI.esri_county AS ESRI_COUNTY,
        STG_ESRI.esri_residential_business AS ESRI_RESIDENTIAL_BUSINESS,
        STG_ESRI.esri_apn AS ESRI_APN,
        STG_ESRI.esri_apn_usecode AS ESRI_APN_USECODE,
        STG_ESRI.esri_apn_use_desc AS ESRI_APN_USE_DESC,
        CAST(NULLIF(TRIM(STG_ESRI.esri_apn_unit_count), '') AS NUMBER(10,0)) AS ESRI_APN_UNIT_COUNT,
        CAST(CTE_FINAL.SOURCE_SYSTEM AS VARCHAR(50)) AS SOURCE_SYSTEM,
        CAST('STG_IFP_BIPAS' AS VARCHAR(200)) AS SOURCE_TABLE,
        to_timestamp_ntz(to_timestamp_tz(SUBSTRING(TRIM(CTE_FINAL.DTL__CAPXTIMESTAMP),1,17),'YYYYMMDDHHMISSFF')) AS SRC_CREATE_DTM,
        to_timestamp_ntz(to_timestamp_tz(SUBSTRING(TRIM(CTE_FINAL.DTL__CAPXTIMESTAMP),1,17),'YYYYMMDDHHMISSFF')) AS SRC_LAST_UPDATE_DTM,
        TRIM(CTE_FINAL.DTL__CAPXACTION) AS SRC_ACTION_CD,
        CASE WHEN TRIM(CTE_FINAL.DTL__CAPXACTION) = 'D' THEN 'Y' ELSE 'N' END AS SRC_DEL_IND,
        CAST(BATCH_ID AS BIGINT) AS ODS_INS_BATCH_ID,
        CAST(BATCH_ID AS BIGINT) AS ODS_UPD_BATCH_ID,
        CAST(BATCH_TIMESTAMP AS TIMESTAMP_NTZ) AS ODS_INSERT_TIMESTAMP,
        CAST(BATCH_TIMESTAMP AS TIMESTAMP_NTZ) AS ODS_UPDATE_TIMESTAMP,
        CAST(CTE_FINAL.STG_INSERT_TIMESTAMP AS TIMESTAMP_NTZ) AS STG_INSERT_TIMESTAMP,
        CASE WHEN SRC_DEL_IND = 'Y' THEN 'DELETE' ELSE 'INSERT' END AS OPERATION_TYPE,
        CAST(NULL AS VARCHAR(20)) AS DQ_STATUS,
        CAST(NULL AS VARCHAR(50)) AS DQ_ERROR_CODE,
        CAST(NULL AS VARCHAR(500)) AS DQ_ERROR_MESSAGE
from CTE_FINAL
LEFT JOIN CTE_BATCH_ID ON 1=1
LEFT JOIN {{ ref('STG_IFP_BIPAS_ADDRESS') }} STG_ESRI
    ON TRIM(CTE_FINAL.ASCOMP) = STG_ESRI.ASCOMP
    AND TRIM(CTE_FINAL.ASACCT) = STG_ESRI.ASACCT
    AND TRIM(CTE_FINAL.ASSITE) = STG_ESRI.ASSITE
)
 
{% else %}
,CTE_HISTORICAL AS (
SELECT
        {{ generate_surrogate_key([trim_string('ASCOMP'),trim_string('ASACCT'),trim_string('ASSITE')]) }} as ODS_IFP_BIPAS_PK,
        CAST(CONCAT({{ trim_string('ASCOMP') }},'-',{{ trim_string('ASACCT') }},'-',{{ trim_string('ASSITE') }}) AS VARCHAR(50)) AS ODS_IFP_BIPAS_COMPOSITE_KEY,
        CAST(NULL AS NUMBER) AS STG_IFP_BIPAS_PK,
        {{ trim_string('SYS_CDC_LIB') }} AS SRC_CDC_LIB,
        {{ trim_string('ASCOMP') }} AS ASCOMP,
        {{ trim_string('ASACCT') }} AS ASACCT,
        {{ trim_string('ASSITE') }} AS ASSITE,
        {{ trim_string('ASSNAM') }} AS ASSNAM,
        {{ trim_string('ASADNO') }} AS ASADNO,
        {{ trim_string('ASADNM') }} AS ASADNM,
        {{ trim_string('ASSUIT') }} AS ASSUIT,
        {{ trim_string('ASSTTY') }} AS ASSTTY,
        {{ trim_string('ASSTDR') }} AS ASSTDR,
        {{ trim_string('ASSADR') }} AS ASSADR,
        {{ trim_string('ASADR2') }} AS ASADR2,
        {{ trim_string('ASCITY') }} AS ASCITY,
        {{ trim_string('ASSTPR') }} AS ASSTPR,
        {{ trim_string('ASZPPC') }} AS ASZPPC,
        {{ trim_string('ASCTRY') }} AS ASCTRY,
        CAST(ASARCD AS NUMBER(5,0)) AS ASARCD,
        CAST(ASPHNE AS NUMBER(10,0)) AS ASPHNE,
        CAST(ASPEXT AS NUMBER(5,0)) AS ASPEXT,
        {{ trim_string('ASCNAM') }} AS ASCNAM,
        {{ trim_string('ASCTTL') }} AS ASCTTL,
        {{ trim_string('ASANAM') }} AS ASANAM,
        {{ trim_string('ASATTL') }} AS ASATTL,
        CAST(ASTERR AS NUMBER(5,0)) AS ASTERR,
        {{ trim_string('ASSLNO') }} AS ASSLNO,
        {{ trim_string('ASCTRT') }} AS ASCTRT,
        CAST(ASTERM AS NUMBER(5,0)) AS ASTERM,
        {{ trim_string('ASCTST') }} AS ASCTST,
        CAST(ASSTDT_ORIG AS NUMBER(10,0)) AS ASSTDT_ORIG,
        CAST(ASSTDT AS TIMESTAMP_NTZ(7)) AS ASSTDT,
        CAST(ASONDT_ORIG AS NUMBER(10,0)) AS ASONDT_ORIG,
        CAST(ASONDT AS TIMESTAMP_NTZ(7)) AS ASONDT,
        CAST(ASCLDT_ORIG AS NUMBER(10,0)) AS ASCLDT_ORIG,
        CAST(ASCLDT AS TIMESTAMP_NTZ(7)) AS ASCLDT,
        CAST(ASRVDT_ORIG AS NUMBER(10,0)) AS ASRVDT_ORIG,
        CAST(ASRVDT AS TIMESTAMP_NTZ(7)) AS ASRVDT,
        CAST(ASEXDT_ORIG AS NUMBER(10,0)) AS ASEXDT_ORIG,
        CAST(ASEXDT AS TIMESTAMP_NTZ(7)) AS ASEXDT,
        {{ trim_string('ASSCLS') }} AS ASSCLS,
        {{ trim_string('ASTXCD') }} AS ASTXCD,
        {{ trim_string('ASTXEX') }} AS ASTXEX,
        {{ trim_string('ASPO') }} AS ASPO,
        {{ trim_string('ASSPFL') }} AS ASSPFL,
        {{ trim_string('ASSHRD') }} AS ASSHRD,
        {{ trim_string('ASCPI') }} AS ASCPI,
        {{ trim_string('ASOLDC') }} AS ASOLDC,
        {{ trim_string('ASUSER') }} AS ASUSER,
        CAST(ASUDAT AS NUMBER(10,0)) AS ASUDAT,
        CAST(ASUTIM AS NUMBER(10,0)) AS ASUTIM,
        CAST(NULL AS VARCHAR(12)) AS CCT,
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
        CAST(NULL AS VARCHAR(50)) AS CORRECTED_FAX_NUMBER,
        CAST(CORRECTED_AD_LATITUDE AS NUMBER(11,6)) AS CORRECTED_AD_LATITUDE,
        CAST(CORRECTED_AD_LONGITUDE AS NUMBER(11,6)) AS CORRECTED_AD_LONGITUDE,
        CAST(CONCAT({{ trim_string('ASCOMP') }},'-',{{ trim_string('ASACCT') }},'-',{{ trim_string('ASSITE') }}) AS VARCHAR(50)) AS SITE_COMPOSITE_KEY,
        CASE
            WHEN CORRECTED_AD_LATITUDE IS NOT NULL
                 AND CORRECTED_AD_LONGITUDE IS NOT NULL
                 AND CORRECTED_AD_LATITUDE >= -90 AND CORRECTED_AD_LATITUDE <= 90
                 AND CORRECTED_AD_LONGITUDE >= -180 AND CORRECTED_AD_LONGITUDE <= 180
            THEN ST_MAKEPOINT(
                CAST(CORRECTED_AD_LONGITUDE AS FLOAT),
                CAST(CORRECTED_AD_LATITUDE AS FLOAT)
            )
            ELSE NULL
        END AS CORRECTED_GEOCODE,
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
        CAST(ESRI_LONGITUDE AS VARCHAR(50)) AS ESRI_LONGITUDE,
        CAST(ESRI_LATITUDE AS VARCHAR(50)) AS ESRI_LATITUDE,
        CAST(ESRI_PROCESSED_INDICATOR AS VARCHAR(1)) AS ESRI_PROCESSED_INDICATOR,
        CAST(ESRI_ERROR_INDICATOR AS VARCHAR(1)) AS ESRI_ERROR_INDICATOR,
        CAST(ESRI_MATCHCONFIDENCESCORE AS VARCHAR(50)) AS ESRI_MATCHCONFIDENCESCORE,
        CAST(ESRI_LOCATIONCONFIDENCESCORE AS VARCHAR(50)) AS ESRI_LOCATIONCONFIDENCESCORE,
        CAST(ESRI_LOCATIONCODE AS VARCHAR(50)) AS ESRI_LOCATIONCODE,
        CAST(ESRI_LOCATIONCODE_DESC AS VARCHAR(200)) AS ESRI_LOCATIONCODE_DESC,
        CAST(ESRI_COUNTY AS VARCHAR(50)) AS ESRI_COUNTY,
        CAST(ESRI_RESIDENTIAL_BUSINESS AS VARCHAR(50)) AS ESRI_RESIDENTIAL_BUSINESS,
        CAST(ESRI_APN AS VARCHAR(50)) AS ESRI_APN,
        CAST(ESRI_APN_USECODE AS VARCHAR(50)) AS ESRI_APN_USECODE,
        CAST(ESRI_APN_USE_DESC AS VARCHAR(200)) AS ESRI_APN_USE_DESC,
        CAST(ESRI_APN_UNIT_COUNT AS NUMBER(10,0)) AS ESRI_APN_UNIT_COUNT,
        CAST('Infopro' AS VARCHAR(50)) AS SOURCE_SYSTEM,
        CAST('ODS_IFP_BIPAS_HIST' AS VARCHAR(200)) AS SOURCE_TABLE,
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
FROM INFOPRO.HISTORY.ODS_IFP_BIPAS_HIST
CROSS JOIN CTE_BATCH_ID
)
 
{% endif %}
 
SELECT
*,
{{ generate_hash_change( relation = this) }} AS HASH_CHANGE,
{{ generate_hash_full( relation = this) }} AS HASH_FULL
FROM {% if is_incremental() %}CTE_TRANSFORMED{% else %}CTE_HISTORICAL{% endif %}