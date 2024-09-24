

SELECT TO_CHAR(q.run_id,'UTF-8') as potential_network_incident_questionnaire_run_id -- pk
     ,q.RUN_DATETIME AS potential_network_incident_questionnaire_run_datetime
     ,LPAD(q.notification_number, 12, 0) AS notification_no
,HASH(NVL(potential_network_incident_questionnaire_run_id,'x')) AS md_key_hash            
,HASH(CONCAT(NVL(potential_network_incident_questionnaire_run_id,'x'),NVL(potential_network_incident_questionnaire_run_datetime,'1900-01-01 00:01'),NVL(notification_no,-1))) AS md_row_hash
FROM {{ source('archie', 'vw_questionnaire_run_latest_latest') }} q