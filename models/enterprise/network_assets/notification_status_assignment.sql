

/********************************************************************************************************
vw_notification_status_assignment
*********************************************************************************************************/
 SELECT
    qmel.notification AS notification_no
   ,CONCAT('U','-',NVL(tj30t.statusprofile,''),'-',NVL(tj30t.userstatus,'')) AS notification_status_key_code
   ,DECODE(jest.statusisinactive,'X','N','Y') AS notification_status_assignment_active_ind
   ,HASH(CONCAT(NVL(notification_no,'x'), NVL(notification_status_key_code,'x'))) AS md_key_hash
   ,HASH(CONCAT(NVL(notification_no,'x'), NVL(notification_status_key_code,'x'),NVL(notification_status_assignment_active_ind,'x'))) AS md_row_hash
    FROM {{ source('sap_erp', 'stg_qmel') }} qmel 
    INNER JOIN {{ source('sap_erp', 'stg_jest') }} AS jest 
    ON qmel.objectnumber = jest.objectnumber
    INNER JOIN {{ source('sap_erp', 'stg_jsto') }} AS jsto 
    ON jest.objectnumber = jsto.objectnumber
    INNER JOIN {{ source('sap_erp', 'stg_tj30t') }} AS tj30t  
    ON jsto.statusprofile = tj30t.statusprofile
    AND jest.statuscode = tj30t.userstatus
    AND tj30t.language = 'E'
UNION ALL 
SELECT
    qmel.notification AS notification_no 
   ,CONCAT('S','-',NVL(tj02t.systemstatus,'')) AS notification_status_key_code
   ,DECODE(jest.statusisinactive,'X','N','Y') AS notification_status_assignment_active_ind
   ,HASH(CONCAT(NVL(notification_no,'x'), NVL(notification_status_key_code,'x'))) AS md_key_hash
   ,HASH(CONCAT(NVL(notification_no,'x'), NVL(notification_status_key_code,'x'),NVL(notification_status_assignment_active_ind,'x'))) AS md_row_hash
    FROM {{ source('sap_erp', 'stg_qmel') }} qmel 
    INNER JOIN {{ source('sap_erp', 'stg_jest') }} AS jest 
    ON qmel.objectnumber = jest.objectnumber
    INNER JOIN {{ source('sap_erp', 'stg_tj02t') }} AS tj02t
    ON jest.statuscode = tj02t.systemstatus
    AND tj02t.language = 'E'