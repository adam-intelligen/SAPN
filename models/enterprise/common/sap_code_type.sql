
--WHERE language = 'E' -- English language removed, as excluded from raw



-- Foundational view to transform Code
WITH cte_qpct_latest_version as (
    SELECT *
    FROM {{ source('sap_erp', 'stg_qpct') }}
    -- WHERE language = 'E' -- English language removed, as excluded from raw
)
SELECT DISTINCT 
     CONCAT(qp.catalogtype,'-',qp.codegroup,'-',qp.codeid) AS type_key_code
    ,qp.codeid AS code
    ,qp.codetext AS code_name 
    ,NULL as code_desc
    ,CONCAT(catalogtype,'-',codegroup) AS type_group_key_code
    ,CAST(qp.versionnum AS BIGINT) AS code_version_number
    ,qp.validfromdate AS code_valid_from_date
    ,qp.statusofcodegrouporcoderecord AS code_status_code
    ,HASH(type_key_code) AS md_key_hash
    ,HASH(CONCAT(NVL(qp.codeid,'x'),NVL(code_name,'x'),NVL(code_desc,'x'),NVL(type_group_key_code,'x'),NVL(code_version_number,-1),nvl(code_valid_from_date,'1900-01-01'),NVL(code_status_code,'x'))) AS md_row_hash
FROM cte_qpct_latest_version qp