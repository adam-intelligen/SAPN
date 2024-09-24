

-- ACTIViTY TYPE
SELECT 
     type_group_key_code AS notification_activity_type_group_key_code
    ,code_group_code AS notification_activity_type_group_code
    ,code_group_name AS notification_activity_type_group_name
    ,code_group_desc AS notification_activity_type_group_desc
    ,code_group_status_code AS notification_activity_type_group_status_code
    ,catalog_code AS notification_activity_type_catalog_code
    ,md_key_hash
    ,md_row_hash
FROM {{ ref('sap_code_group') }} sap_code_group
WHERE EXISTS (
    SELECT 1
    FROM {{ ref('notification_activity_type') }} nat 
    WHERE sap_code_group.type_group_key_code = nat.notification_activity_type_group_key_code
)