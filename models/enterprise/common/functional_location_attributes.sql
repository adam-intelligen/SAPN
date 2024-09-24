


/********************************************************************************************************
vw_functional_location_attributes
View that filters Fire Notifications from qmel

*********************************************************************************************************/
SELECT 
   iflot.functionallocation
   --Voltages
   ,LISTAGG(CASE WHEN ausp.charcinternalid = 2371 THEN ausp.charcvalue ELSE NULL END,', ') AS span_voltage
   ,LISTAGG(CASE WHEN ausp.charcinternalid = 2089 THEN ausp.charcvalue ELSE NULL END,', ') AS operating_voltage
   ,LISTAGG(CASE WHEN ausp.charcinternalid = 1895 THEN ausp.charcvalue ELSE NULL END,', ') AS voltages
   ,LISTAGG(CASE WHEN ausp.charcinternalid = 2724 THEN ausp.charcvalue ELSE NULL END,', ') AS operating_voltage_kv
   ,LISTAGG(CASE WHEN ausp.charcinternalid = 1903 THEN ausp.charcvalue ELSE NULL END,', ') AS feature_id 
   --Lat/Long
   ,CASE WHEN LISTAGG(CASE WHEN ausp.charcinternalid = 1665 THEN ausp.charcvalue ELSE NULL END,', ') = '' THEN NULL ELSE LISTAGG(CASE WHEN ausp.charcinternalid = 1664 THEN ausp.charcvalue ELSE NULL END,', ') END AS longitude
   ,CASE WHEN LISTAGG(CASE WHEN ausp.charcinternalid = 1664 THEN ausp.charcvalue ELSE NULL END,', ') = '' THEN NULL ELSE LISTAGG(CASE WHEN ausp.charcinternalid = 1665 THEN ausp.charcvalue ELSE NULL END,', ') END AS latitude
    ,LISTAGG(CASE WHEN ausp.charcinternalid = 1979 THEN ausp.charcvalue ELSE NULL END,', ') AS substation_id  
    ,LISTAGG(CASE WHEN ausp.charcinternalid = 2716 THEN ausp.charcvalue ELSE NULL END,', ') AS depot_area_code
    ,LISTAGG(CASE WHEN ausp.charcinternalid = 1977 THEN ausp.charcvalue ELSE NULL END,', ') AS atmospheric_corrosion_zone_code  
    ,LISTAGG(CASE WHEN ausp.charcinternalid = 2727 THEN ausp.charcvalue ELSE NULL END,', ') AS lga_council_code 
    ,LISTAGG(CASE WHEN ausp.charcinternalid = 1989 THEN ausp.charcvalue ELSE NULL END,', ') AS corrosion_zone_ground_level_code  
    ,LISTAGG(CASE WHEN ausp.charcinternalid = 1976 THEN ausp.charcvalue ELSE NULL END,', ') AS bushfire_risk_area_code  
    ,LISTAGG(CASE WHEN ausp.charcinternalid = 1810 THEN ausp.charcvalue ELSE NULL END,', ') AS fire_ban_district1_name
    ,LISTAGG(CASE WHEN ausp.charcinternalid = 2209 THEN ausp.charcvalue ELSE NULL END,', ') AS fire_ban_district2_name  
    ,LISTAGG(CASE WHEN ausp.charcinternalid = 2210 THEN ausp.charcvalue ELSE NULL END,', ') AS fire_ban_district3_name         
FROM {{ source('sap_erp', 'stg_iflot') }} iflot
INNER JOIN {{ ref('ausp_attributes') }} ausp
    ON iflot.functionallocation = ausp.clfnobjectid and ausp.classtype = '003'
GROUP BY iflot.functionallocation