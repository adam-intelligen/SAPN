



-- FUNCTIONAL_LOCATION
SELECT
 iflot.functionallocation AS functional_location_id
,flottxt.functionallocationname AS functional_location_name 
,iflot.superiorfunctionallocation AS superior_functional_location_id
,iflot.uuid AS asset_id
,fla.feature_id AS feature_id 
,CASE WHEN SUBSTR(iflot.functionallocation,0,3) = 'SUB' THEN 'SUBSTATION' ELSE 'DISTRIBUTION' END AS asset_responsible_area_ind   
,iflot.functionallocationcategory AS asset_category_code
,iflot.technicalobjecttype AS asset_class_code --#TODO -- need to source this
,iloa.assetroom AS primary_feeder_id
,TRY_TO_GEOGRAPHY(
'POINT('||fla.longitude||' '||fla.latitude||')') AS functional_location_point
,CASE WHEN fla.longitude = '' THEN NULL ELSE fla.longitude END AS functional_location_longitude
,CASE WHEN fla.latitude = '' THEN NULL ELSE fla.latitude END AS functional_location_latitude              
,CASE WHEN fla.substation_id = '' THEN NULL ELSE fla.substation_id END AS substation_id  
,CASE WHEN fla.depot_area_code = '' THEN NULL ELSE fla.depot_area_code END AS depot_area_code
,CASE WHEN fla.atmospheric_corrosion_zone_code = '' THEN NULL ELSE fla.atmospheric_corrosion_zone_code  END AS atmospheric_corrosion_zone_code  
,CASE WHEN fla.lga_council_code = '' THEN NULL ELSE fla.lga_council_code END AS lga_council_code 
,CASE WHEN fla.corrosion_zone_ground_level_code = '' THEN NULL ELSE fla.corrosion_zone_ground_level_code  END AS corrosion_zone_ground_level_code  
,CASE WHEN fla.bushfire_risk_area_code= '' THEN NULL ELSE fla.bushfire_risk_area_code END AS bushfire_risk_area_code 
,CASE WHEN fla.fire_ban_district1_name = '' THEN NULL ELSE fla.fire_ban_district1_name END AS fire_ban_district1_name
,CASE WHEN fla.fire_ban_district2_name = '' THEN NULL ELSE fla.fire_ban_district2_name END AS fire_ban_district2_name  
,CASE WHEN fla.fire_ban_district3_name = '' THEN NULL ELSE fla.fire_ban_district3_name END AS fire_ban_district3_name  
,NULL AS SCONRRR_category_code --#TODO -- need to source this
,NULL AS ESCOSA_region_code --#TODO -- need to source this
,CAST(NVL(fla.span_voltage,NVL(fla.operating_voltage,NVL(fla.operating_voltage_kv,fla.voltages))) AS VARCHAR) AS operating_voltage 
--,NULL AS operating_voltage
,iflot.operationstartdate AS startup_date                                    
,NULL AS is_connected_flag     --#TODO -- need to source this
,NULL AS subnetwork_controller_id    --#TODO -- need to source this                           
,NULL AS tier_rank_id      --#TODO -- need to source this
,NULL AS subnetwork_name --#TODO -- need to source this
,NULL AS supporting_subnetwork_name --#TODO -- need to source this
,NULL AS phase_code --#TODO -- need to source this
,NULL AS phase_count --#TODO -- need to source this
,NULL AS normal_device_state --#TODO -- need to source this
,NULL AS current_device_state --#TODO -- need to source this
,NULL AS mounting   --#TODO -- need to source this                                       
,NULL AS symbol_rotation   --#TODO -- need to source this                      
,iflot.funclocationstructure AS structure_ind
,iflot.creationdate AS created_date
,iflot.createdbyuser AS created_by_user_id
,iflot.lastchangedate AS last_updated_date
,iflot.lastchangedbyuser AS last_updated_by_user_id
,HASH(NVL(iflot.functionallocation,'x')) AS md_key_hash
,HASH(CONCAT(NVL(iflot.functionallocation,'x'),NVL(functional_location_name,'x'),NVL(superior_functional_location_id,'x'),NVL(asset_id,'x'),NVL(feature_id,'x'),NVL(asset_responsible_area_ind,'x'),NVL(asset_category_code,'x'),NVL(asset_class_code,'x'),NVL(primary_feeder_id,'x'),NVL(functional_location_latitude,'x'),
NVL(functional_location_longitude,'x'),NVL(substation_id,'x'),NVL(depot_area_code,'x'),NVL(lga_council_code,'x'),NVL(atmospheric_corrosion_zone_code,'x'),
NVL(corrosion_zone_ground_level_code,'x'),NVL(bushfire_risk_area_code,'x'),NVL(fire_ban_district1_name,'x'),NVL(fire_ban_district2_name ,'x'),
NVL(fire_ban_district3_name,'x'),NVL(SCONRRR_category_code,'x'),NVL(ESCOSA_region_code,'x'),NVL(operating_voltage,'x'),NVL(structure_ind,'x'),
NVL( created_date,'1900-01-01'),NVL(created_by_user_id,'x'),NVL( last_updated_date,'1900-01-01'),NVL(last_updated_by_user_id,'x'))) AS md_row_hash 
FROM {{ source('sap_erp', 'stg_iflot') }} iflot
LEFT JOIN {{ ref('functional_location_attributes') }} fla
    ON fla.functionallocation = iflot.functionallocation
LEFT JOIN {{ source('sap_erp', 'stg_iflotx') }} flottxt 
    ON iflot.functionallocation = flottxt.functionallocation
LEFT JOIN {{ source('sap_erp', 'stg_iloa') }} iloa 
    ON iloa.maintobjectlocacctassgmtnmbr = iflot.maintobjectlocacctassgmtnmbr