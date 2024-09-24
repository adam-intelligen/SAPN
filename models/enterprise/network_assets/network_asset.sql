


 /********************************************************************************************************
vw_network_asset
*********************************************************************************************************/

WITH conductor_data AS
(
    select
     cdtr.uuid as network_asset_uuid
    ,null as network_asset_name
    ,null as functional_location_id
    ,null as network_asset_superior_functional_location_id
    ,3 as network_asset_class_id
    ,null as network_asset_type
    ,gatt.network_asset_gis_group_description as network_asset_gis_group
    ,null as network_asset_functional_location_category_code
    ,feeder_id as network_asset_primary_feeder_id
    ,lv_tf_area_id as network_asset_lv_transformer_area_id
    ,to_geometry(null) as network_asset_sap_primary_gps
    ,to_geometry(null) as network_asset_gis_primary_gps
    ,g3e_fid as network_asset_gis_feature_id
    ,null as network_asset_startup_date
    ,state as network_asset_lifecycle_state_code
    ,'DISTRIBUTION' as network_asset_business_line_id
    ,null as network_asset_work_centre_id
    ,null as network_asset_substation_id
    ,null as network_asset_depot_area_code
    ,null as network_asset_atmospheric_corrosion_zone_code
    ,null as network_asset_lga_council_code
    ,null as network_asset_corrosion_zone_ground_level_code
    ,null as network_asset_bushfire_risk_area_code
    ,null as network_asset_fire_ban_district_code
    ,null as network_asset_fire_ban_district2_code
    ,null as network_asset_fire_ban_district3_code
    ,operating_voltage as network_asset_operating_voltage
    ,phasing as network_asset_phasing_code
    ,date_created as network_asset_created_date
    ,created_user as network_asset_created_by_name
    ,last_edited_date as network_asset_last_updated_date
    ,last_edited_user as network_asset_last_updated_by_name
    from {{ source('gis', 'vw_conductor_latest') }} cdtr
    left join {{ ref('network_asset_gis_group_attributes') }} gatt
        on cdtr.assetgroup = gatt.network_asset_gis_group_code and gatt.network_asset_class_type = 'Conductor'
    where cdtr.assetgroup in (1,2,3) -- conductor
        and cdtr.uuid is not null
),
cable_data as
(
    select
     cble.uuid as network_asset_uuid
    ,flottxt.functionallocationname as network_asset_name
    ,iflot.functionallocation as functional_location_id
    ,iflot.superiorfunctionallocation as network_asset_superior_functional_location_id
    ,1 as network_asset_class_id
    ,iflot.technicalobjecttype as network_asset_type
    ,gatt.network_asset_gis_group_description as network_asset_gis_group
    ,iflot.functionallocationcategory as network_asset_functional_location_category_code
    ,cble.feeder_id as network_asset_primary_feeder_id
    ,lv_tf_area_id as network_asset_lv_transformer_area_id
    ,to_geometry(st_asgeojson(naa.midpoint)) as network_asset_sap_primary_gps -- srid not added for sap geometry
    ,to_geometry(null) as network_asset_gis_primary_gps
    ,cble.g3e_fid as network_asset_gis_feature_id
    ,iflot.operationstartdate as network_asset_startup_date
    ,cble.state as network_asset_lifecycle_state_code
    ,case when SUBSTR(iflot.functionallocation,0,3) = 'SUB' then 'SUBSTATION' else 'DISTRIBUTION' end as network_asset_business_line_id
    ,workcenterinternalid as network_asset_work_centre_id
    ,naa.substation_id as network_asset_substation_id --substation_id from network_asset_attributes view
    ,naa.depot_area_code as network_asset_depot_area_code --depot_area_code from network_asset_attributes view
    ,naa.atmospheric_corrosion_zone_code as network_asset_atmospheric_corrosion_zone_code --atmospheric_corrosion_zone_code from network_asset_attributes view
    ,naa.lga_council_code as network_asset_lga_council_code --lga_council_code from network_asset_attributes view
    ,naa.corrosion_zone_ground_level_code as network_asset_corrosion_zone_ground_level_code --corrosion_zone_ground_level_code from network_asset_attributes view
    ,naa.bushfire_risk_area_code as network_asset_bushfire_risk_area_code --bushfire_risk_area_code from network_asset_attributes view
    ,naa.fire_ban_district1_name as network_asset_fire_ban_district_code --fire_ban_district_code from network_asset_attributes view
    ,naa.fire_ban_district2_name as network_asset_fire_ban_district2_code --fire_ban_district2_code from network_asset_attributes view
    ,naa.fire_ban_district3_name as network_asset_fire_ban_district3_code --fire_ban_district3_code from network_asset_attributes view
    ,cble.operating_voltage as network_asset_operating_voltage 
    ,cble.phasing as network_asset_phasing_code
    ,cble.date_created as network_asset_created_date
    ,cble.created_user as network_asset_created_by_name
    ,cble.last_edited_date as network_asset_last_updated_date
    ,cble.last_edited_user as network_asset_last_updated_by_name
    from {{ source('gis', 'vw_conductor_latest') }} cble
    left join {{ ref('network_asset_gis_group_attributes') }} gatt
        on cble.assetgroup = gatt.network_asset_gis_group_code and gatt.network_asset_class_type = 'Conductor'
    left join {{ source('sap_erp', 'stg_iflot') }} iflot
        on cble.uuid = iflot.uuid
    left join {{ source('sap_erp', 'stg_iflotx') }} flottxt 
        on iflot.functionallocation = flottxt.functionallocation 
    left join {{ ref('network_asset_attributes') }} naa
        on cble.uuid = naa.uuid
    where cble.assetgroup in (4,5,6) -- cable
        and cble.uuid is not null
),
span_data as
(
    select
     span.uuid as network_asset_uuid
    ,flottxt.functionallocationname as network_asset_name
    ,span.functionallocation as functional_location_id
    ,span.superiorfunctionallocation as network_asset_superior_functional_location_id
    ,8 as network_asset_class_id
    ,span.technicalobjecttype as network_asset_type
    ,null as network_asset_gis_group 
    ,span.functionallocationcategory as network_asset_functional_location_category_code
    ,naa.feeder_id as network_asset_primary_feeder_id
    ,null as network_asset_lv_transformer_area_id
    ,to_geometry(st_asgeojson(naa.span_midpoint)) as network_asset_sap_primary_gps
    ,to_geometry(null) as network_asset_gis_primary_gps
    ,naa.span_g3e_fid as network_asset_gis_feature_id
    ,span.operationstartdate as network_asset_startup_date
    ,null as network_asset_lifecycle_state_code
    ,case when SUBSTR(span.functionallocation,0,3) = 'SUB' then 'SUBSTATION' else 'DISTRIBUTION' end as network_asset_business_line_id
    ,span.workcenterinternalid as network_asset_work_centre_id
    ,naa.substation_id as network_asset_substation_id
    ,naa.depot_area_code as network_asset_depot_area_code
    ,naa.atmospheric_corrosion_zone_code as network_asset_atmospheric_corrosion_zone_code
    ,naa.lga_council_code as network_asset_lga_council_code
    ,naa.corrosion_zone_ground_level_code as network_asset_corrosion_zone_ground_level_code
    ,naa.bushfire_risk_area_code as network_asset_bushfire_risk_area_code
    ,naa.fire_ban_district1_name as network_asset_fire_ban_district_code
    ,naa.fire_ban_district2_name as network_asset_fire_ban_district2_code
    ,naa.fire_ban_district3_name as network_asset_fire_ban_district3_code
    ,naa.operating_voltage as network_asset_operating_voltage
    ,naa.phasing_val as network_asset_phasing_code
    ,span.creationdate as network_asset_created_date
    ,span.createdbyuser as network_asset_created_by_name
    ,span.lastchangedate as network_asset_last_updated_date
    ,span.lastchangedbyuser as network_asset_last_updated_by_name
    from {{ source('sap_erp', 'stg_iflot') }} span
    left join {{ source('sap_erp', 'stg_iflotx') }} flottxt 
        on span.functionallocation = flottxt.functionallocation 
    left join {{ ref('network_asset_attributes') }} naa
        on span.uuid = naa.uuid
    where span.technicalobjecttype = 'SPAN' -- span
        and span.uuid is not null
),
cable_set_data as
(
    select
     cble_s.uuid as network_asset_uuid
    ,flottxt.functionallocationname as network_asset_name
    ,cble_s.functionallocation as functional_location_id
    ,cble_s.superiorfunctionallocation as network_asset_superior_functional_location_id
    ,2 as network_asset_class_id
    ,cble_s.technicalobjecttype as network_asset_type
    ,null as network_asset_gis_group 
    ,cble_s.functionallocationcategory as network_asset_functional_location_category_code
    ,naa.feeder_id as network_asset_primary_feeder_id
    ,null as network_asset_lv_transformer_area_id
    ,to_geometry(st_asgeojson(naa.span_midpoint)) as network_asset_sap_primary_gps 
    ,to_geometry(null) as network_asset_gis_primary_gps
    ,naa.feature_id as network_asset_gis_feature_id
    ,cble_s.operationstartdate as network_asset_startup_date
    ,null as network_asset_lifecycle_state_code
    ,case when SUBSTR(cble_s.functionallocation,0,3) = 'SUB' then 'SUBSTATION' else 'DISTRIBUTION' end as network_asset_business_line_id
    ,cble_s.workcenterinternalid as network_asset_work_centre_id
    ,naa.substation_id as network_asset_substation_id
    ,naa.depot_area_code as network_asset_depot_area_code
    ,naa.atmospheric_corrosion_zone_code as network_asset_atmospheric_corrosion_zone_code
    ,naa.lga_council_code as network_asset_lga_council_code
    ,naa.corrosion_zone_ground_level_code as network_asset_corrosion_zone_ground_level_code
    ,naa.bushfire_risk_area_code as network_asset_bushfire_risk_area_code
    ,naa.fire_ban_district1_name as network_asset_fire_ban_district_code
    ,naa.fire_ban_district2_name as network_asset_fire_ban_district2_code
    ,naa.fire_ban_district3_name as network_asset_fire_ban_district3_code
    ,null as network_asset_operating_voltage
    ,null as network_asset_phasing_code
    ,cble_s.creationdate as network_asset_created_date
    ,cble_s.createdbyuser as network_asset_created_by_name
    ,cble_s.lastchangedate as network_asset_last_updated_date
    ,cble_s.lastchangedbyuser as network_asset_last_updated_by_name
    from {{ source('sap_erp', 'stg_iflot') }} cble_s
    left join {{ source('sap_erp', 'stg_iflotx') }} flottxt 
        on cble_s.functionallocation = flottxt.functionallocation 
    left join {{ ref('network_asset_attributes') }} naa
        on cble_s.uuid = naa.uuid
    where cble_s.technicalobjecttype = 'U_CABLE_SE' -- cable set
        and cble_s.uuid is not null
),
transformer_data as (
    select
     tf.uuid as network_asset_uuid
    ,flottxt.functionallocationname as network_asset_name
    ,iflot.functionallocation as functional_location_id
    ,iflot.superiorfunctionallocation as network_asset_superior_functional_location_id
    ,11 as network_asset_class_id
    ,iflot.technicalobjecttype as network_asset_type
    ,gatt.network_asset_gis_group_description as network_asset_gis_group
    ,iflot.functionallocationcategory as network_asset_functional_location_category_code
    ,tf.feeder_id as network_asset_primary_feeder_id
    ,null as network_asset_lv_transformer_area_id
    ,to_geometry(st_asgeojson(naa.midpoint)) as network_asset_sap_primary_gps
    ,tf.shape as network_asset_gis_primary_gps
    ,tf.g3e_fid as network_asset_gis_feature_id
    ,iflot.operationstartdate as network_asset_startup_date
    ,tf.state as network_asset_lifecycle_state_code
    ,case when SUBSTR(iflot.functionallocation,0,3) = 'SUB' then 'SUBSTATION' else 'DISTRIBUTION' end as network_asset_business_line_id
    ,iflot.workcenterinternalid as network_asset_work_centre_id
    ,naa.substation_id as network_asset_substation_id
    ,naa.depot_area_code as network_asset_depot_area_code
    ,naa.atmospheric_corrosion_zone_code as network_asset_atmospheric_corrosion_zone_code
    ,naa.lga_council_code as network_asset_lga_council_code
    ,naa.corrosion_zone_ground_level_code as network_asset_corrosion_zone_ground_level_code
    ,naa.bushfire_risk_area_code as network_asset_bushfire_risk_area_code
    ,naa.fire_ban_district1_name as network_asset_fire_ban_district_code
    ,naa.fire_ban_district2_name as network_asset_fire_ban_district2_code
    ,naa.fire_ban_district3_name as network_asset_fire_ban_district3_code
    ,tf.operating_voltage as network_asset_operating_voltage 
    ,tf.phasing as network_asset_phasing_code
    ,tf.date_created as network_asset_created_date
    ,tf.created_user as network_asset_created_by_name
    ,tf.last_edited_date as network_asset_last_updated_date
    ,tf.last_edited_user as network_asset_last_updated_by_name
    from {{ source('gis', 'vw_transformer_latest') }} tf
    left join {{ ref('network_asset_gis_group_attributes') }} gatt
        on tf.assetgroup = gatt.network_asset_gis_group_code and gatt.network_asset_class_type = 'Transformer'
    left join {{ source('sap_erp', 'stg_iflot') }} iflot
        on tf.uuid=iflot.uuid
    left join {{ source('sap_erp', 'stg_iflotx') }} flottxt 
        on iflot.functionallocation = flottxt.functionallocation 
    left join {{ ref('network_asset_attributes') }} naa
        on tf.uuid = naa.uuid
    where tf.assetgroup in (1,2,3) -- distribution transformer
        and tf.uuid is not null
),
cubicle_data as (
    select
     cubicle.uuid as network_asset_uuid
    ,flottxt.functionallocationname as network_asset_name
    ,iflot.functionallocation as functional_location_id
    ,iflot.superiorfunctionallocation as network_asset_superior_functional_location_id
    ,4 as network_asset_class_id
    ,iflot.technicalobjecttype as network_asset_type
    ,gatt.network_asset_gis_group_description as network_asset_gis_group
    ,iflot.functionallocationcategory as network_asset_functional_location_category_code
    ,naa.feeder_id as network_asset_primary_feeder_id
    ,null as network_asset_lv_transformer_area_id
    ,to_geometry(st_asgeojson(naa.midpoint)) as network_asset_sap_primary_gps
    ,cubicle.shape as network_asset_gis_primary_gps
    ,cubicle.g3e_fid as network_asset_gis_feature_id
    ,iflot.operationstartdate as network_asset_startup_date
    ,cubicle.state as network_asset_lifecycle_state_code
    ,case when SUBSTR(iflot.functionallocation,0,3) = 'SUB' then 'SUBSTATION' else 'DISTRIBUTION' end as network_asset_business_line_id
    ,iflot.workcenterinternalid as network_asset_work_centre_id
    ,naa.substation_id as network_asset_substation_id
    ,naa.depot_area_code as network_asset_depot_area_code
    ,naa.atmospheric_corrosion_zone_code as network_asset_atmospheric_corrosion_zone_code
    ,naa.lga_council_code as network_asset_lga_council_code
    ,naa.corrosion_zone_ground_level_code as network_asset_corrosion_zone_ground_level_code
    ,naa.bushfire_risk_area_code as network_asset_bushfire_risk_area_code
    ,naa.fire_ban_district1_name as network_asset_fire_ban_district_code
    ,naa.fire_ban_district2_name as network_asset_fire_ban_district2_code
    ,naa.fire_ban_district3_name as network_asset_fire_ban_district3_code
    ,null as network_asset_operating_voltage 
    ,cubicle.phasing as network_asset_phasing_code
    ,cubicle.date_created as network_asset_created_date
    ,cubicle.created_user as network_asset_created_by_name
    ,cubicle.last_edited_date as network_asset_last_updated_date
    ,cubicle.last_edited_user as network_asset_last_updated_by_name
    from {{ source('gis', 'vw_groundstructure_latest') }} cubicle
    left join {{ ref('network_asset_gis_group_attributes') }} gatt
        on cubicle.assetgroup = gatt.network_asset_gis_group_code and gatt.network_asset_class_type = 'GroundStructure'
    left join {{ source('sap_erp', 'stg_iflot') }} iflot
        on cubicle.uuid=iflot.uuid
    left join {{ source('sap_erp', 'stg_iflotx') }} flottxt 
        on iflot.functionallocation = flottxt.functionallocation 
    left join {{ ref('network_asset_attributes') }} naa
        on cubicle.uuid = naa.uuid
    where cubicle.assetgroup in (1,2,3) -- TF Cubicle & Switch Cubicle
    and cubicle.uuid is not null
),
support_structure_data as (
    select
     ss.uuid as network_asset_uuid
    ,flottxt.functionallocationname as network_asset_name
    ,iflot.functionallocation as functional_location_id
    ,iflot.superiorfunctionallocation as network_asset_superior_functional_location_id
    ,9 as network_asset_class_id
    ,iflot.technicalobjecttype as network_asset_type
    ,gatt.network_asset_gis_group_description as network_asset_gis_group
    ,iflot.functionallocationcategory as network_asset_functional_location_category_code
    ,naa.feeder_id as network_asset_primary_feeder_id
    ,null as network_asset_lv_transformer_area_id
    ,to_geometry(st_asgeojson(naa.midpoint)) as network_asset_sap_primary_gps
    ,ss.shape as network_asset_gis_primary_gps
    ,ss.g3e_fid as network_asset_gis_feature_id
    ,iflot.operationstartdate as network_asset_startup_date
    ,ss.state as network_asset_lifecycle_state_code
    ,case when SUBSTR(iflot.functionallocation,0,3) = 'SUB' then 'SUBSTATION' else 'DISTRIBUTION' end as network_asset_business_line_id
    ,iflot.workcenterinternalid as network_asset_work_centre_id
    ,naa.substation_id as network_asset_substation_id
    ,naa.depot_area_code as network_asset_depot_area_code
    ,naa.atmospheric_corrosion_zone_code as network_asset_atmospheric_corrosion_zone_code
    ,naa.lga_council_code as network_asset_lga_council_code
    ,naa.corrosion_zone_ground_level_code as network_asset_corrosion_zone_ground_level_code
    ,naa.bushfire_risk_area_code as network_asset_bushfire_risk_area_code
    ,naa.fire_ban_district1_name as network_asset_fire_ban_district_code
    ,naa.fire_ban_district2_name as network_asset_fire_ban_district2_code
    ,naa.fire_ban_district3_name as network_asset_fire_ban_district3_code
    ,null as network_asset_operating_voltage 
    ,null as network_asset_phasing_code
    ,ss.date_created as network_asset_created_date
    ,ss.created_user as network_asset_created_by_name
    ,ss.last_edited_date as network_asset_last_updated_date
    ,ss.last_edited_user as network_asset_last_updated_by_name
    from {{ source('gis', 'vw_supportstructure_latest') }} ss -- Support Structure(Poles)
    left join {{ ref('network_asset_gis_group_attributes') }} gatt
        on ss.assetgroup = gatt.network_asset_gis_group_code and gatt.network_asset_class_type = 'SupportStructure'
    left join {{ source('sap_erp', 'stg_iflot') }} iflot
        on ss.uuid=iflot.uuid
    left join {{ source('sap_erp', 'stg_iflotx') }} flottxt 
        on iflot.functionallocation = flottxt.functionallocation 
    left join {{ ref('network_asset_attributes') }} naa
        on ss.uuid = naa.uuid
    where ss.uuid is not null
),
other_asset_data as
(
    select
     othr_asset.uuid as network_asset_uuid
    ,flottxt.functionallocationname as network_asset_name
    ,othr_asset.functionallocation as functional_location_id
    ,othr_asset.superiorfunctionallocation as network_asset_superior_functional_location_id
    ,case when othr_asset.technicalobjecttype in ('U_CABLE', 'CABLE_SECT' ) then 1
          when othr_asset.technicalobjecttype in ('U_CABLE_SE' ) then 2
          when othr_asset.technicalobjecttype in ('CA_BLDG','CA_ENCLOSU','CA_FENCE','CUBICLE','GL_SW_CUB' ) then 4
          when othr_asset.technicalobjecttype in ('SPAN' ) then 8
          when othr_asset.technicalobjecttype in ('SS_PL_MTT','SS_PL_STOB','SS_POLE','SS_TW_LATF', 'LIGHT_CLMN' ) then 9
          when othr_asset.technicalobjecttype in ('TF_GND','TF_OH','TF_PD','TF_PWR','TF_SUB'  ) then 11
          else 0 end
        as network_asset_class_id
    ,othr_asset.technicalobjecttype as network_asset_type
    ,null as network_asset_gis_group 
    ,othr_asset.functionallocationcategory as network_asset_functional_location_category_code
    ,naa.feeder_id as network_asset_primary_feeder_id
    ,null as network_asset_lv_transformer_area_id
    ,to_geometry(st_asgeojson(naa.midpoint)) as network_asset_sap_primary_gps
    ,to_geometry(null) as network_asset_gis_primary_gps
    ,naa.span_g3e_fid as network_asset_gis_feature_id
    ,othr_asset.operationstartdate as network_asset_startup_date
    ,null as network_asset_lifecycle_state_code
    ,case when SUBSTR(othr_asset.functionallocation,0,3) = 'SUB' then 'SUBSTATION' else 'DISTRIBUTION' end as network_asset_business_line_id
    ,othr_asset.workcenterinternalid as network_asset_work_centre_id
    ,naa.substation_id as network_asset_substation_id --substation_id from network_asset_attributes view
    ,naa.depot_area_code as network_asset_depot_area_code --depot_area_code from network_asset_attributes view
    ,naa.atmospheric_corrosion_zone_code as network_asset_atmospheric_corrosion_zone_code --atmospheric_corrosion_zone_code from network_asset_attributes view
    ,naa.lga_council_code as network_asset_lga_council_code --lga_council_code from network_asset_attributes view
    ,naa.corrosion_zone_ground_level_code as network_asset_corrosion_zone_ground_level_code --corrosion_zone_ground_level_code from network_asset_attributes view
    ,naa.bushfire_risk_area_code as network_asset_bushfire_risk_area_code --bushfire_risk_area_code from network_asset_attributes view
    ,naa.fire_ban_district1_name as network_asset_fire_ban_district_code --fire_ban_district_code from network_asset_attributes view
    ,naa.fire_ban_district2_name as network_asset_fire_ban_district2_code --fire_ban_district2_code from network_asset_attributes view
    ,naa.fire_ban_district3_name as network_asset_fire_ban_district3_code --fire_ban_district3_code from network_asset_attributes view
    -- cast(nvl(span_voltage,nvl(operating_voltage,nvl(operating_voltage_kv,voltages))) from network_asset_attributes
    ,cast(nvl(naa.span_voltage,nvl(naa.operating_voltage,nvl(naa.operating_voltage_kv,naa.voltages))) as varchar) as network_asset_operating_voltage
    ,naa.phasing_val as network_asset_phasing_code --phasing_val from network_asset_attributes
    ,othr_asset.creationdate as network_asset_created_date
    ,othr_asset.createdbyuser as network_asset_created_by_name
    ,othr_asset.lastchangedate as network_asset_last_updated_date
    ,othr_asset.lastchangedbyuser as network_asset_last_updated_by_name
    from {{ source('sap_erp', 'stg_iflot') }} othr_asset
    left join {{ source('sap_erp', 'stg_iflotx') }} flottxt 
        on othr_asset.functionallocation = flottxt.functionallocation 
    left join {{ ref('network_asset_attributes') }} naa
        on othr_asset.uuid = naa.uuid
    where othr_asset.uuid is not null --primary key column not null
        and othr_asset.functionallocationcategory not in ('1','2','4','C','F','I','J','L','M','P','T','Y') --remove non-network asset flocs
        and othr_asset.technicalobjecttype not in ('U_CABLE', 'CABLE_SECT','U_CABLE_SE','CA_BLDG','CA_ENCLOSU','CA_FENCE','CUBICLE','GL_SW_CUB','SPAN'
        ,'SS_PL_MTT','SS_PL_STOB','SS_POLE','SS_TW_LATF','LIGHT_CLMN','TF_GND','TF_OH','TF_PD','TF_PWR','TF_SUB') -- remove sap class records already included from gis dataset
        and not exists (select 1 from cable_data cd where cd.network_asset_uuid=othr_asset.uuid) --exclude already included cable data 
        and not exists (select 1 from cable_set_data csd where csd.network_asset_uuid=othr_asset.uuid) --exclude already included cable set data 
        and not exists (select 1 from span_data sd where sd.network_asset_uuid=othr_asset.uuid) --exclude already included span data 
        and not exists (select 1 from transformer_data td where td.network_asset_uuid=othr_asset.uuid) --exclude already included transformer data 
        and not exists (select 1 from cubicle_data cbd where cbd.network_asset_uuid=othr_asset.uuid) --exclude already included cubicle data 
        and not exists (select 1 from support_structure_data ssd where ssd.network_asset_uuid=othr_asset.uuid) --exclude already included support structure data 
),
all_data as 
(
    select * from conductor_data
    union
    select * from cable_data
    union
    select * from cable_set_data
    union
    select * from span_data
    union
    select * from transformer_data
    union
    select * from cubicle_data
    union
    select * from support_structure_data
    union
    select * from other_asset_data
),
all_data_unique_uuid as (
    select *, row_number() over (partition by network_asset_uuid order by network_asset_last_updated_date desc) as rownum 
    from all_data
)
select 
    network_asset_uuid
    ,network_asset_name
    ,functional_location_id
    ,network_asset_superior_functional_location_id
    ,network_asset_class_id
    ,network_asset_type
    ,network_asset_gis_group
    ,network_asset_functional_location_category_code
    ,network_asset_primary_feeder_id
    ,network_asset_lv_transformer_area_id
    ,network_asset_sap_primary_gps
    ,network_asset_gis_primary_gps
    ,network_asset_gis_feature_id
    ,network_asset_startup_date
    ,network_asset_lifecycle_state_code
    ,network_asset_business_line_id
    ,network_asset_work_centre_id
    ,network_asset_substation_id
    ,network_asset_depot_area_code
    ,network_asset_atmospheric_corrosion_zone_code
    ,network_asset_lga_council_code
    ,network_asset_corrosion_zone_ground_level_code
    ,network_asset_bushfire_risk_area_code
    ,network_asset_fire_ban_district_code
    ,network_asset_fire_ban_district2_code
    ,network_asset_fire_ban_district3_code
    ,network_asset_operating_voltage 
    ,network_asset_phasing_code
    ,network_asset_created_date
    ,network_asset_created_by_name
    ,network_asset_last_updated_date
    ,network_asset_last_updated_by_name
,HASH(network_asset_uuid)  AS md_key_hash
,HASH(NVL(network_asset_uuid, 'x'), NVL(network_asset_name, 'x')
    , NVL(functional_location_id, 'x'), NVL(network_asset_superior_functional_location_id, 'x')
    , NVL(network_asset_class_id, -1), NVL(network_asset_type, 'x')
    , NVL(network_asset_gis_group, 'x'), NVL(network_asset_functional_location_category_code, 'x')
    , NVL(network_asset_primary_feeder_id, 'x'), NVL(network_asset_lv_transformer_area_id, 'x')
    , NVL(network_asset_sap_primary_gps, to_geometry('POINT(0 0)')), NVL(network_asset_gis_primary_gps, to_geometry('POINT(0 0)'))
    , NVL(network_asset_gis_feature_id, -1), NVL(network_asset_startup_date, '1900-01-01')
    , NVL(network_asset_lifecycle_state_code, 'x'), NVL(network_asset_business_line_id, 'x')
    , NVL(network_asset_work_centre_id, -1), NVL(network_asset_substation_id, 'x')
    , NVL(network_asset_depot_area_code, 'x'), NVL(network_asset_atmospheric_corrosion_zone_code, 'x')
    , NVL(network_asset_lga_council_code, 'x'), NVL(network_asset_corrosion_zone_ground_level_code, 'x')
    , NVL(network_asset_bushfire_risk_area_code, 'x'), NVL(network_asset_fire_ban_district_code, 'x')
    , NVL(network_asset_fire_ban_district2_code, 'x'), NVL(network_asset_fire_ban_district3_code, 'x')
    , NVL(network_asset_operating_voltage, 'x'), NVL(network_asset_phasing_code, 'x')
    , NVL(network_asset_created_date, '1900-01-01 00:01'), NVL(network_asset_created_by_name, 'x')
    , NVL(network_asset_last_updated_date, '1900-01-01 00:01'), NVL(network_asset_last_updated_by_name, 'x')
    )  AS md_row_hash
from all_data_unique_uuid
where rownum = 1