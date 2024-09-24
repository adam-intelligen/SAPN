


    
/********************************************************************************************************
vw_functional_location_status_assignment
*********************************************************************************************************/
    /*User statuses*/
    WITH cte_functional_location_status_assignment AS
       (
        SELECT DISTINCT functional_location_status_key_code
        FROM {{ ref('functional_location_status_assignment') }}
        )
    SELECT
         CONCAT('U','-',NVL(tj30t.statusprofile,''),'-',NVL(tj30t.userstatus,'')) AS functional_location_status_key_code
        ,NVL(tj30t.statusprofile,'') AS functional_location_status_profile_code
        ,NVL(tj30t.userstatusshortname,'') AS functional_location_status_code
        ,NVL(tj30t.userstatusname,'') AS functional_location_status_name
        ,NULL AS functional_location_status_group  -- Need to replace this prior to use - set null until we have TJ30.STONR
        ,HASH(CONCAT(NVL(functional_location_status_key_code,'x'))) AS md_key_hash
        ,HASH(CONCAT(NVL(functional_location_status_key_code,'x'), NVL(functional_location_status_profile_code,'x'),NVL(functional_location_status_code,'x'),NVL(functional_location_status_name,'x'),NVL(functional_location_status_group ,'x'))) AS md_row_hash
    FROM cte_functional_location_status_assignment cte 
    INNER JOIN {{ source('sap_erp', 'stg_tj30t') }} tj30t 
        ON cte.functional_location_status_key_code  = CONCAT('U','-',NVL(tj30t.statusprofile,''),'-',NVL(tj30t.userstatus,'')) 
    WHERE tj30t.language = 'E'
UNION ALL
    /*System statuses*/
    SELECT
         CONCAT('S','-',NVL(tj02t.systemstatus,'')) AS functional_location_status_key_code
        ,'' AS functional_location_status_profile_code
        ,NVL(tj02t.systemstatusshortname,'') AS functional_location_status_code
        ,NVL(tj02t.statusname,'') AS functional_location_status_name
        ,NULL AS functional_location_status_group -- Need to replace this prior to use - set null until we have TJ30.STONR*/
       ,HASH(CONCAT(NVL(functional_location_status_key_code,'x'))) AS md_key_hash
        ,HASH(CONCAT(NVL(functional_location_status_key_code,'x'), NVL(functional_location_status_profile_code,'x'),NVL(functional_location_status_code,'x'),NVL(functional_location_status_name,'x'),NVL(functional_location_status_group ,'x'))) AS md_row_hash
    FROM cte_functional_location_status_assignment cte 
    INNER JOIN {{ source('sap_erp', 'stg_tj02t') }} AS tj02t
        ON cte.functional_location_status_key_code = CONCAT('S','-',NVL(tj02t.systemstatus,'')) 
    WHERE tj02t.language = 'E'