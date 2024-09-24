

-- Foundational view to transform Code Group
SELECT DISTINCT
    CONCAT(catalogtype,'-',codegroup) AS type_group_key_code
    ,codegroup AS code_group_code
    ,codetext AS code_group_name
    ,NULL AS code_group_desc
    ,statusofcodegrouporcoderecord AS code_group_status_code
    ,catalogtype AS catalog_code
    ,HASH(type_group_key_code) AS md_key_hash
    ,HASH(CONCAT(NVL(code_group_code,'x'),NVL(catalog_code,'x'),NVL(code_group_name,'x'),NVL(code_group_desc,'x'),NVL(code_group_status_code,'x'))) AS md_row_hash
FROM {{ source('sap_erp', 'stg_qpgt') }}