

-- ACTIViTY  
    SELECT  type_key_code AS notification_activity_type_key_code
           ,code AS notification_activity_type_code
           ,code_name AS notification_activity_type_name
           ,code_desc AS notification_activity_type_desc
           ,type_group_key_code AS notification_activity_type_group_key_code
           ,code_version_number AS notification_activity_type_version_number
           ,code_valid_from_date AS notification_activity_type_valid_from_date
           ,code_status_code AS notification_activity_type_status_code
           ,md_key_hash
           ,md_row_hash
    FROM {{ ref('sap_code_type') }} sap_code_type
    WHERE EXISTS (
        SELECT 1
        FROM {{ source('sap_erp', 'stg_qmel') }} qmel
        WHERE sap_code_type.type_key_code = qmel.notificationcatalog||'-'||qmel.notificationcodegroup||'-'||qmel.notificationcodeid
    )