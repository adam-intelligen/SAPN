

/********************************************************************************************************
vw_priority
View that is built to pick up distince values for priority type and priority code from qmel

*********************************************************************************************************/

SELECT DISTINCT
 CONCAT(NVL(notificationprioritytype,''),'-',NVL(notificationpriority,'')) AS notification_priority_key_code
,notificationprioritytype AS notification_priority_type_code
,notificationpriority AS notification_priority_code
,NULL AS notification_priority_name --not yet available. Comes from the table SAP T356_T
,HASH(NVL(notification_priority_key_code,'x')) AS md_key_hash
,HASH(CONCAT(NVL(notification_priority_key_code,'x'),NVL(notification_priority_type_code,'x'),NVL(notification_priority_code,'x'),NVL(notification_priority_name,'x'))) AS md_row_hash
FROM {{ source('sap_erp', 'stg_qmel') }}
WHERE CONCAT(NVL(notificationprioritytype,'x'),NVL(notificationpriority,'x')) <> 'xx' 