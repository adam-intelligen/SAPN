

/********************************************************************************************************
vw_functional_location_status_assignment
*********************************************************************************************************/
 SELECT
    iflot.functionallocation AS functional_location_id
   ,CONCAT('U','-',NVL(tj30t.statusprofile,''),'-',NVL(tj30t.userstatus,'')) AS functional_location_status_key_code
   ,DECODE(jest.statusisinactive,'X','N','Y') AS functional_location_status_assignment_active_ind
   ,HASH(CONCAT(NVL(functional_location_id,'x'), NVL(functional_location_status_key_code,'x'))) AS md_key_hash
   ,HASH(CONCAT(NVL(functional_location_id,'x'), NVL(functional_location_status_key_code,'x'),NVL(functional_location_status_assignment_active_ind,'x'))) AS md_row_hash
    FROM {{ source('sap_erp', 'stg_iflot') }} iflot
    INNER JOIN {{ source('sap_erp', 'stg_jest') }} AS jest 
    ON iflot.objectnumber = jest.objectnumber
    INNER JOIN {{ source('sap_erp', 'stg_jsto') }} AS jsto 
    ON jest.objectnumber = jsto.objectnumber
    INNER JOIN {{ source('sap_erp', 'stg_tj30t') }} AS tj30t  
    ON jsto.statusprofile = tj30t.statusprofile
    AND jest.statuscode = tj30t.userstatus
    AND tj30t.language = 'E'
UNION ALL 
SELECT
  iflot.functionallocation AS functional_location_id
   ,CONCAT('S','-',NVL(tj02t.systemstatus,'')) AS functional_location_status_key_code
   ,DECODE(jest.statusisinactive,'X','N','Y') AS functional_location_status_assignment_active_ind
   ,HASH(CONCAT(NVL(functional_location_id,'x'), NVL(functional_location_status_key_code,'x'))) AS md_key_hash
   ,HASH(CONCAT(NVL(functional_location_id,'x'), NVL(functional_location_status_key_code,'x'),NVL(functional_location_status_assignment_active_ind,'x'))) AS md_row_hash
    FROM {{ source('sap_erp', 'stg_iflot') }} iflot
    INNER JOIN {{ source('sap_erp', 'stg_jest') }} AS jest 
    ON iflot.objectnumber = jest.objectnumber
    INNER JOIN {{ source('sap_erp', 'stg_tj02t') }} AS tj02t
    ON jest.statuscode = tj02t.systemstatus
    AND tj02t.language = 'E'