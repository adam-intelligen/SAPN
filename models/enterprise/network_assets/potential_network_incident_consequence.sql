


SELECT TO_CHAR(ev.event_id,'UTF-8') as potential_network_incident_id  -- pk
      ,con.consequence_name as potential_network_incident_consequence_type_code  -- pk
      ,con.consequence_magnitude AS potential_network_incident_consequence_magnitude
      ,con.consequence_likelihood as potential_network_incident_consequence_likelihood
      ,HASH(CONCAT( NVL(potential_network_incident_id,'x'),NVL(potential_network_incident_consequence_type_code,'x'))) AS md_key_hash
      ,HASH(CONCAT( NVL(potential_network_incident_id,'x'),	NVL(potential_network_incident_consequence_type_code,'x'),NVL(potential_network_incident_consequence_magnitude,-1),NVL(potential_network_incident_consequence_likelihood,-1))) AS md_row_hash	
FROM {{ source('archie', 'vw_questionnaire_run_latest_latest') }} q
INNER JOIN {{ source('archie', 'vw_analytics_value_event_latest') }} ev
ON q.run_id = ev.run_id
INNER JOIN {{ source('archie', 'vw_analytics_value_event_consequence_latest') }} con
ON ev.event_id = con.event_id