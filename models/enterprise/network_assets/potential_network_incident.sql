

/********************************************************************************************************
vw_consequence_type

*********************************************************************************************************/

SELECT TO_CHAR(ev.event_id,'UTF-8') as potential_network_incident_id  -- pk
      ,TO_CHAR(q.run_id,'UTF-8') as potential_network_incident_questionnaire_run_id
      ,ev.event_name as potential_network_incident_type_code
      ,ev.likelihood as potential_network_incident_likelihood
      ,HASH(potential_network_incident_id) AS md_key_hash
      ,HASH(CONCAT(NVL(potential_network_incident_id,'x'),NVL(potential_network_incident_questionnaire_run_id,'x'),	NVL(potential_network_incident_type_code,'x'), NVL(potential_network_incident_likelihood,-1))) AS md_row_hash	 
FROM {{ source('archie', 'vw_questionnaire_run_latest_latest') }} q
INNER JOIN {{ source('archie', 'vw_analytics_value_event_latest') }} ev
ON q.run_id = ev.run_id