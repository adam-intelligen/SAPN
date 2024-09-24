 


 /********************************************************************************************************
vw_work_centre
Columns remarked out not required as uncertain as to what they are
*********************************************************************************************************/
SELECT
     CONCAT(NVL(workcentertypecode,''),'-',NVL(workcenterinternalid,'')) AS work_centre_key_code
    ,workcentertypecode AS work_centre_object_type_code --SAP OBJTY
    ,workcenterinternalid AS work_centre_object_code --SAP CRHD.OBJID
    ,wrkctrhumrsceobjid AS work_centre_object_id --SAP HROID
    ,workcenter AS work_centre_code --SAP CRHD.ARBPL
    ,plant AS work_centre_plant_code --SAP WERKS
    ,NULL AS work_centre_name -- CRTX.KTEXT - to be populated when CRTX is added to Raw at a later date
    ,workcenterlocationgroup AS work_centre_location_group  --SAP ORTGR 
    --,capacityinternalid AS work_centre_capactiy_id --SAP KAPID
    --,workcentercategorycode AS work_centre_category_code -- SAP VERWE
    ,workcenteristobedeleted AS work_centre_is_deleted_flag --SAP LVORM
    --,changedongrnd AS grnd_last_changed_date  --SAP AEDAT_GRND
    --,workcenterlastchangedby AS grnd_last_changed_by_user_id  --SAP AENAM_GRND
    --,changedonvora AS vora_last_changed_date  --SAP AEDAT_VORA
    --,usernamevora AS vora_last_changed_by_user_id  --SAP AENAM_VORA
    --,changedonterm AS term_last_changed_date  --SAP AEDAT_VORA
    --,usernameterm AS term_last_changed_by_user_id  --SAP AENAM_VORA
    --,changedontech AS tech_last_changed_date  --SAP AEDAT_VORA
    --,usernametech AS tech_last_changed_by_user_id  --SAP AENAM_VORA
    ,validitystartdate AS work_centre_valid_from_date --SAP BEGDA
    ,validityenddate AS work_centre_valid_to_date --SAP ENDDA
    ,HASH(work_centre_key_code) AS md_key_hash
    ,HASH(CONCAT(NVL(work_centre_key_code,'x')||NVL(work_centre_object_type_code,'x'),NVL(work_centre_object_code,'x'),NVL(work_centre_object_id,'-1'),
          NVL(work_centre_code,'x'),NVL(work_centre_plant_code,'x'),NVL(plant,'x'),NVL(work_centre_name,'x'),NVL(work_centre_is_deleted_flag ,'x'),
          NVL(work_centre_valid_from_date,'1900-01-01'),NVL(work_centre_valid_to_date,'1900-01-01'))
    ) AS md_row_hash  
FROM {{ source('sap_erp', 'stg_crhd') }}