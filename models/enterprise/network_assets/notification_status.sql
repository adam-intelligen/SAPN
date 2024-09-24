

/********************************************************************************************************
vw_notification_status
*********************************************************************************************************/
    /*User statuses*/
    WITH cte_notification_status_assignment AS
       (
        SELECT DISTINCT notification_status_key_code
        FROM {{ ref('notification_status_assignment') }}
        )
    SELECT
         CONCAT('U','-',NVL(tj30t.statusprofile,''),'-',NVL(tj30t.userstatus,'')) AS notification_status_key_code
        ,NVL(tj30t.statusprofile,'') AS notification_status_profile_code
        ,NVL(tj30t.userstatusshortname,'') AS notification_status_code
        ,NVL(tj30t.userstatusname,'') AS notification_status_name
        ,NULL AS notification_status_group  -- Need to replace this prior to use - set null until we have TJ30.STONR
        ,HASH(CONCAT(NVL(notification_status_key_code,'x'))) AS md_key_hash
        ,HASH(CONCAT(NVL(notification_status_key_code,'x'), NVL(notification_status_profile_code,'x'),NVL(notification_status_code,'x'),NVL(notification_status_name,'x'),NVL(notification_status_group ,'x'))) AS md_row_hash
    FROM cte_notification_status_assignment cte 
    INNER JOIN {{ source('sap_erp', 'stg_tj30t') }} tj30t 
        ON cte.notification_status_key_code  = CONCAT('U','-',NVL(tj30t.statusprofile,''),'-',NVL(tj30t.userstatus,'')) 
    WHERE tj30t.language = 'E'
    
    UNION ALL

    /*System statuses*/
    SELECT
         CONCAT('S','-',NVL(tj02t.systemstatus,'')) AS notification_status_key_code
        ,'' AS notification_status_profile_code
        ,NVL(tj02t.systemstatusshortname,'') AS notification_status_code
        ,NVL(tj02t.statusname,'') AS notification_status_name
        ,NULL AS notification_status_group -- Need to replace this prior to use - set null until we have TJ30.STONR*/
       ,HASH(CONCAT(NVL(notification_status_key_code,'x'))) AS md_key_hash
        ,HASH(CONCAT(NVL(notification_status_key_code,'x'), NVL(notification_status_profile_code,'x'),NVL(notification_status_code,'x'),NVL(notification_status_name,'x'),NVL(notification_status_group ,'x'))) AS md_row_hash
    FROM cte_notification_status_assignment cte 
    INNER JOIN {{ source('sap_erp', 'stg_tj02t') }} AS tj02t
        ON cte.notification_status_key_code = CONCAT('S','-',NVL(tj02t.systemstatus,'')) 
    WHERE tj02t.language = 'E'