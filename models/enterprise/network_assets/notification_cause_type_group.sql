

    
--CAUSE TYPE
SELECT 
     type_group_key_code AS notification_cause_type_group_key_code
    ,code_group_code AS notification_cause_type_group_code
    ,code_group_name AS notification_cause_type_group_name
    ,code_group_desc AS notification_cause_type_group_desc
    ,code_group_status_code AS notification_cause_type_group_status_code
    ,catalog_code AS notification_cause_type_catalog_code
    ,md_key_hash
    ,md_row_hash
FROM {{ ref('sap_code_group') }} sap_code_group
WHERE EXISTS (
    SELECT 1
    FROM {{ ref('notification_cause_type') }} nct
    WHERE sap_code_group.type_group_key_code = nct.notification_cause_type_group_key_code
)