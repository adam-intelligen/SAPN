

/********************************************************************************************************
vw_notification_task
*********************************************************************************************************/
    SELECT 
    qmel.notification as notification_no
    ,qmsm.notificationtask as notification_task_no
    ,qmsm.notificationtaskcatalog||'-'||qmsm.notificationtaskcodegroup||'-'||qmsm.notificationtaskcode AS notification_task_type_key_code
    ,qmsm.notificationtasktext as notification_task_desc
    ,qmsm.notiftaskplannedstartdate as notification_task_planned_start_date
    ,qmsm.notiftaskplannedstarttime as notification_task_planned_start_time
    ,CAST(qmsm.notiftaskplannedstartdate||' '||qmsm.notiftaskplannedstarttime AS timestamp) as notification_task_planned_start_datetime
    ,qmsm.notiftaskplannedenddate as notification_task_planned_end_date
    ,qmsm.notiftaskplannedendtime as notification_task_planned_end_time
    ,CAST(qmsm.notiftaskplannedenddate||' '||qmsm.notiftaskplannedendtime AS timestamp) as notification_task_planned_end_datetime
    ,qmsm.notiftaskcompletedbyuser as notification_task_completed_by_user_id
    ,qmsm.notiftaskcompletiondate as notification_task_completed_date
    ,qmsm.notiftaskcompletiontime as notification_task_completed_time
    ,CAST(qmsm.notiftaskcompletiondate||' '||qmsm.notiftaskcompletiontime AS timestamp) as notification_task_completed_datetime
    ,qmsm.createdbyuser as notification_task_created_by_user_id
    ,qmsm.creationdate as notification_task_created_date
    ,qmsm.lastchangedbyuser as notification_task_last_updated_by_user_id
    ,qmsm.lastchangeddate as notification_task_last_updated_date
    ,HASH(CONCAT(NVL(notification_no,'x'),NVL(notification_task_no, -1))) AS md_key_hash
    ,HASH(CONCAT(
            NVL(notification_no,'x'),
            NVL(notification_task_no, -1),
            NVL(notification_task_type_key_code, 'x'),
            NVL(notification_task_desc, 'x'),
            NVL(notification_task_planned_start_date, '1900-01-01'),
            NVL(notification_task_planned_start_time, '00:01'),
            NVL(notification_task_planned_start_datetime, '1900-01-01 00:01'),
            NVL(notification_task_planned_end_date, '1900-01-01'),
            NVL(notification_task_planned_end_time, '00:01'),
            NVL(notification_task_planned_end_datetime, '1900-01-01 00:01'),
            NVL(notification_task_completed_by_user_id, 'x'),
            NVL(notification_task_completed_date, '1900-01-01'),
            NVL(notification_task_completed_time, '00:01'),
            NVL(notification_task_completed_datetime, '1900-01-01 00:01'),
            NVL(notification_task_created_by_user_id, 'x'),
            NVL(notification_task_created_date, '1900-01-01'),
            NVL(notification_task_last_updated_by_user_id, 'x'),
            NVL(notification_task_last_updated_date, '1900-01-01')
     )) AS md_row_hash
FROM {{ source('sap_erp', 'stg_qmel') }} qmel
INNER JOIN {{ source('sap_erp', 'stg_qmsm') }} qmsm on qmsm.notification = qmel.notification