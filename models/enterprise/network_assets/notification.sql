

/********************************************************************************************************
vw_notification
*********************************************************************************************************/
    WITH cte_qmur_first_notification_item_latest_cause AS (
        SELECT * FROM {{ source('sap_erp', 'stg_qmur') }} 
        WHERE UPPER(NVL(isdeleted,'Z')) <> 'X' 
    ),   
    cte_qmfe_first_notification_item AS (
        SELECT * FROM {{ source('sap_erp', 'stg_qmfe') }}
    )
    SELECT
         qmel.notification as notification_no
        ,LTRIM(qmel.notification, '0') as notification_no_trimmed
        ,qmel.notificationtype as notification_type_code
        ,CASE WHEN qmel.omsnum IS NOT NULL THEN 'INC '||qmel.omsnum
           WHEN qmel.omsnum IS NULL AND qmel.notificationtext LIKE 'OMS Job #: INC%' THEN SUBSTR(qmel.notificationtext,11,14) 
           WHEN qmel.omsnum IS NULL AND qmel.notificationtext LIKE 'FIRE REPORT - OMS Job INC%' THEN SUBSTR(qmel.notificationtext,22,14) -- Fire
           WHEN qmel.notificationtype = 'IF' AND qmel.notificationcodeid = 'FIRE' AND nia.fire_oms_job_number <> '' THEN nia.fire_oms_job_number
           WHEN qmel.notificationtype = 'QS' AND nia.shock_oms_job_number <> '' THEN nia.shock_oms_job_number
           -- may need to link to characteristic to enrich this further
           ELSE '-1' 
         END AS notification_network_incident_no
        ,CAST(qmel.notificationreportingdate||' '||qmel.notificationcreationtime AS timestamp) as notification_start_datetime
        ,qmel.notificationreportingdate as notification_start_date
        ,qmel.notificationcreationtime as notification_start_time
        ,CAST(qmel.notificationcompletiondate||' '||qmel.completiontimeofnotif AS timestamp) as notification_end_datetime
        ,qmel.notificationcompletiondate as notification_end_date
        ,qmel.completiontimeofnotif as notification_end_time
        ,qmel.notificationtext as notification_name
        --,NULL AS notification_desc     -- #todo Is STRING_AGG(NLTXT.TXTLG...)
        ,iloa.functionallocation as functional_location_id
        ,iflot.uuid AS network_asset_uuid 
        ,qmel.addressnum as address_id
        ,qmel.notificationprioritytype||'-'||qmel.notificationpriority as notification_priority_key_code
        ,qmel.notificationcatalog||'-'||qmel.notificationcodegroup||'-'||qmel.notificationcodeid as notification_activity_type_key_code
        ,qmur.notificationcausecodecatalog||'-'||qmur.notificationcausecodegroup||'-'||qmur.notificationcausecode as notification_cause_type_key_code
        ,qmfe.defectcodecatalog||'-'||qmfe.defectcodegroup||'-'||qmfe.defectcode as notification_damage_type_key_code
        --,NULL AS impact_type_code     -- #todo not yet available
        ,qmfe.notifitmobjectpartcodectlg||'-'||qmfe.notifitmobjectpartcodegroup||'-'||qmfe.notifitmobjectpartcode as notification_object_part_type_key_code
        ,NVL(crhd.workcentertypecode,'')||'-'||NVL(crhd.workcenterinternalid,'') as work_centre_key_code
        ,CASE WHEN (qmel.notificationtype in ('FM','QS')) OR (qmel.notificationtype = 'IF' AND qmel.notificationcodeid = 'FIRE') THEN TRUE ELSE FALSE END AS notification_is_network_incident
        ,CASE WHEN qmel.notificationtype in ('DD','SD','FM','QS','VG','LO','RI') THEN TRUE ELSE FALSE END AS notification_is_network_defect
        ,CASE WHEN qmel.notificationtype = 'IF' AND qmel.notificationcodeid = 'FIRE' THEN TRUE ELSE FALSE END AS notification_is_fire_incident
        ,CASE WHEN qmel.notificationtype = 'QS' AND qmfe.defectcode = 'P178' THEN TRUE ELSE FALSE END AS notification_is_shock_incident
        ,qmel.createdbyuser as notification_created_by_user_id
        ,qmel.creationdate as notification_created_date
        ,qmel.lastchangedbyuser as notification_last_updated_by_user_id
        ,qmel.lastchangeddate as notification_last_updated_date
        ,HASH(notification_no) AS md_key_hash
        ,HASH(CONCAT(
             NVL(notification_no, 'x')
            ,NVL(notification_type_code, 'x')
            ,NVL(notification_network_incident_no, 'x')
            ,NVL(notification_start_datetime, '1900-01-01 00:01')
            ,NVL(notification_start_date, '1900-01-01')
            ,NVL(notification_start_time, '00:01')
            ,NVL(notification_end_datetime, '1900-01-01 00:01')
            ,NVL(notification_end_date, '1900-01-01')
            ,NVL(notification_end_time, '00:01')
            ,NVL(notification_name, 'x')
            ,NVL(functional_location_id, 'x')
            ,NVL(network_asset_uuid, 'x')
            ,NVL(address_id, 'x')
            ,NVL(notification_priority_key_code, 'x')
            ,NVL(notification_activity_type_key_code, 'x')
            ,NVL(notification_cause_type_key_code, 'x')
            ,NVL(notification_damage_type_key_code, 'x')
            ,NVL(notification_object_part_type_key_code, 'x')
            ,NVL(work_centre_key_code, 'x')
            ,NVL(notification_is_network_incident, FALSE)
            ,NVL(notification_is_network_defect, FALSE)
            ,NVL(notification_is_fire_incident, FALSE)
            ,NVL(notification_is_shock_incident, FALSE)
            ,NVL(notification_created_by_user_id, 'x')
            ,NVL(notification_created_date, '1900-01-01')
            ,NVL(notification_last_updated_by_user_id, 'x')
            ,NVL(notification_last_updated_date, '1900-01-01')
        )) as md_row_hash
    FROM {{ source('sap_erp', 'stg_qmel') }} qmel
    LEFT JOIN cte_qmur_first_notification_item_latest_cause qmur
        ON qmel.notification = qmur.notification
    LEFT JOIN cte_qmfe_first_notification_item qmfe
    	ON qmel.notification = qmfe.notification
    LEFT JOIN {{ ref('network_incident_attributes') }} nia
        ON qmel.notification = nia.notification
    LEFT JOIN {{ source('sap_erp', 'stg_qmih') }} qmih
        ON qmel.notification = qmih.notification
    LEFT JOIN {{ source('sap_erp', 'stg_iloa') }} iloa
        ON qmih.maintobjectlocacctassgmtnmbr = iloa.maintobjectlocacctassgmtnmbr
    LEFT JOIN {{ source('sap_erp', 'stg_crhd') }} crhd
        ON qmel.mainworkcenterinternalid = crhd.workcenterinternalid
    LEFT JOIN {{ source('sap_erp', 'stg_iflot') }} iflot
        ON iflot.functionallocation = iloa.functionallocation