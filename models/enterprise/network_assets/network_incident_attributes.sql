 --to exclude the null records in the primary key


/********************************************************************************************************
vw_qmel_fire_attributes
View that filters Fire Notifications from qmwel

*********************************************************************************************************/
SELECT  qmel.notification
        -- Fire Attributes
       ,LISTAGG(CASE WHEN ausp.charcinternalid = 3037 THEN ausp.charcvalue ELSE NULL END,', ') AS if_sapn_infra_involved_in_fire
       ,LISTAGG(CASE WHEN ausp.charcinternalid = 3038 THEN ausp.charcvalue ELSE NULL END,', ') AS type_of_fire
       ,LISTAGG(CASE WHEN ausp.charcinternalid = 3039 THEN ausp.charcvalue ELSE NULL END,', ') AS size_of_fire
       ,LISTAGG(CASE WHEN ausp.charcinternalid = 3043 THEN ausp.charcvalue ELSE NULL END,', ') AS method_of_fire_extinguishing
       ,LISTAGG(CASE WHEN ausp.charcinternalid = 3044 THEN ausp.charcvalue ELSE NULL END,', ') AS conductor_down
       ,LISTAGG(CASE WHEN ausp.charcinternalid = 3045 THEN ausp.charcvalue ELSE NULL END,', ') AS contributing_cause_of_fire 
       ,LISTAGG(CASE WHEN ausp.charcinternalid = 3046 THEN ausp.charcvalue ELSE NULL END,', ') AS cause_of_fire 
--       ,LISTAGG(CASE WHEN ausp.charcinternalid = 3047 THEN ausp.charcvalue ELSE NULL END,', ') AS fire_oms_job_number       
       ,MAX(CASE WHEN ausp.charcinternalid = 3047 THEN ausp.charcvalue ELSE NULL END) AS fire_oms_job_number
       ,LISTAGG(CASE WHEN ausp.charcinternalid = 3048 THEN ausp.charcvalue ELSE NULL END,', ') AS voltage_affected_by_fire 
       ,LISTAGG(CASE WHEN ausp.charcinternalid = 3049 THEN ausp.charcvalue ELSE NULL END,', ') AS feeder_in_fire
       ,LISTAGG(CASE WHEN ausp.charcinternalid = 3053 THEN ausp.charcvalue ELSE NULL END,', ') AS injury_or_near_miss 
       ,LISTAGG(CASE WHEN ausp.charcinternalid = 3060 THEN ausp.charcvalue ELSE NULL END,', ') AS vegetation_involved
       ,LISTAGG(CASE WHEN ausp.charcinternalid = 3068 THEN ausp.charcvalue ELSE NULL END,', ') AS order_or_noti_number
       ,LISTAGG(CASE WHEN ausp.charcinternalid = 3074 THEN ausp.charcvalue ELSE NULL END,', ') AS immediate_action_taken
       ,LISTAGG(CASE WHEN ausp.charcinternalid = 3075 THEN ausp.charcvalue ELSE NULL END,', ') AS follow_up_preventative_action
       ,LISTAGG(CASE WHEN ausp.charcinternalid = 3085 THEN ausp.charcvalue ELSE NULL END,', ') AS inspection_cycle 
       ,LISTAGG(CASE WHEN ausp.charcinternalid = 3086 THEN ausp.charcvalue ELSE NULL END,', ') AS defect_outstanding_relevant_to_failure
       ,LISTAGG(CASE WHEN ausp.charcinternalid = 3087 THEN ausp.charcvalue ELSE NULL END,', ') AS defect_identifiable_before_failure 
       ,LISTAGG(CASE WHEN ausp.charcinternalid = 3088 THEN ausp.charcvalue ELSE NULL END,', ') AS reason_of_failure 
       ,LISTAGG(CASE WHEN ausp.charcinternalid = 3067 THEN ausp.charcfromnumericvalue ELSE NULL END,', ') AS last_inspection
       ,LISTAGG(CASE WHEN ausp.charcinternalid = 1665 THEN ausp.charcvalue ELSE NULL END,', ') AS latitude            
       ,LISTAGG(CASE WHEN ausp.charcinternalid = 1664 THEN ausp.charcvalue ELSE NULL END,', ') AS longitude            
       ,LISTAGG(CASE WHEN ausp.charcinternalid = 1976 THEN ausp.charcvalue ELSE NULL END,', ') AS bushfire_risk_area   
       ,LISTAGG(CASE WHEN ausp.charcinternalid = 1977 THEN ausp.charcvalue ELSE NULL END,', ') AS atmospheric_corrosion_zone  
        --             
       ,SUM(CASE WHEN ausp.charcinternalid = 3078 THEN ausp.charctonumericvalue ELSE 0 END) AS temperature
       ,SUM(CASE WHEN ausp.charcinternalid = 3079 THEN ausp.charctonumericvalue ELSE 0 END) AS grass_fire_danger_index
       ,SUM(CASE WHEN ausp.charcinternalid = 3080 THEN ausp.charctonumericvalue ELSE 0 END) AS wind_gust
       ,SUM(CASE WHEN ausp.charcinternalid = 3081 THEN ausp.charctonumericvalue ELSE 0 END) AS wind_direction_deg_from_north --#TODO This doesn't look right.  Why sum direction?
       -- Shock Attributes
       ,LISTAGG(CASE WHEN ausp.charcinternalid = 2935 THEN ausp.charcvalue ELSE NULL END,', ') AS responsibility  
       ,LISTAGG(CASE WHEN ausp.charcinternalid = 2939 THEN ausp.charcvalue ELSE NULL END,', ') AS body_contact_point_1  
       ,LISTAGG(CASE WHEN ausp.charcinternalid = 2940 THEN ausp.charcvalue ELSE NULL END,', ') AS body_contact_point_2   
       ,LISTAGG(CASE WHEN ausp.charcinternalid = 2936 THEN ausp.charcvalue ELSE NULL END,', ') AS contacted_surface_cause_shock  
       ,LISTAGG(CASE WHEN ausp.charcinternalid = 2919 THEN ausp.charcvalue ELSE NULL END,', ') AS distribution_system  
       ,LISTAGG(CASE WHEN ausp.charcinternalid = 2934 THEN ausp.charcvalue ELSE NULL END,', ') AS cause_location
   --    ,LISTAGG(CASE WHEN ausp.charcinternalid = 1988 THEN ausp.charcvalue ELSE NULL END,', ') AS shock_oms_job_number -- replaced with below
       ,MAX(CASE WHEN ausp.charcinternalid = 1988 THEN ausp.charcvalue ELSE NULL END) AS shock_oms_job_number
   --    ,LISTAGG(CASE WHEN ausp.atinn = 1988' THEN SUBSTRING_REGEXPR('J\d+' IN ausp.atwrt) ELSE NULL END AS job_number #TODO REGEXP syntax incorrect
       ,LISTAGG(CASE WHEN ausp.charcinternalid = 2944 THEN ausp.charcfromnumericvalue ELSE NULL END,', ') AS investigation_date  
      -- ,LISTAGG(CASE WHEN ausp.charcinternalid = 2910 THEN ausp.charcfromnumericvalue ELSE NULL END,', ') AS pre_rep_contact_vol  -- replaced with line tomorrow
       ,MAX(CASE WHEN ausp.charcinternalid = 2910 THEN ausp.charcfromnumericvalue ELSE NULL END) AS pre_rep_contact_vol  
       ,LISTAGG(CASE WHEN ausp.charcinternalid = 2915 THEN ausp.charcfromnumericvalue ELSE NULL END,', ') AS pre_rep_impedence  
       ,LISTAGG(CASE WHEN ausp.charcinternalid = 2913 THEN ausp.charcfromnumericvalue ELSE NULL END,', ') AS pre_rep_install_load_kw 
FROM {{ source('sap_erp', 'stg_qmel') }} qmel
INNER JOIN {{ ref('ausp_attributes') }} ausp
    ON CONCAT(qmel.notification, '0001') = ausp.clfnobjectid  -- this represents first item record without 
    AND ausp.classtype = '015'
GROUP BY qmel.notification