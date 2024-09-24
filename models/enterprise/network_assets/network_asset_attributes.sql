

        
/********************************************************************************************************
vw_network_asset_attributes
*********************************************************************************************************/

WITH network_asset_attributes AS 
(
    SELECT 
    iflot.uuid
   ,iflot.functionallocation
   ,row_number()  over (partition by iflot.uuid order by iflot.lastchangedate desc) as uuid_rank 
   ,LISTAGG(CASE WHEN ausp.charcinternalid = 2371 THEN ausp.charcvalue ELSE NULL END,', ') AS span_voltage
   ,LISTAGG(CASE WHEN ausp.charcinternalid = 2089 THEN ausp.charcvalue ELSE NULL END,', ') AS operating_voltage
   ,LISTAGG(CASE WHEN ausp.charcinternalid = 1895 THEN ausp.charcvalue ELSE NULL END,', ') AS voltages
   ,LISTAGG(CASE WHEN ausp.charcinternalid = 2724 THEN ausp.charcvalue ELSE NULL END,', ') AS operating_voltage_kv
   ,LISTAGG(CASE WHEN ausp.charcinternalid = 1903 THEN ausp.charcvalue ELSE NULL END,', ') AS feature_id 
   --Lat/Long
    ,LISTAGG(CASE WHEN ausp.charcinternalid = 1664 THEN ausp.charcvalue ELSE NULL END,', ') AS longitude
    ,LISTAGG(CASE WHEN ausp.charcinternalid = 1665 THEN ausp.charcvalue ELSE NULL END,', ') AS latitude
    ,LISTAGG(CASE WHEN ausp.charcinternalid = 2370 THEN ausp.charcvalue ELSE NULL END,', ') AS span_g3e_fid
    ,LISTAGG(CASE WHEN ausp.charcinternalid = 2731 THEN ausp.charcvalue ELSE NULL END,', ') AS phasing_val
    ,LISTAGG(CASE WHEN ausp.charcinternalid = 2379 THEN ausp.charcvalue ELSE NULL END,', ') AS start_longitude
    ,LISTAGG(CASE WHEN ausp.charcinternalid = 2377 THEN ausp.charcvalue ELSE NULL END,', ') AS start_latitude
    ,LISTAGG(CASE WHEN ausp.charcinternalid = 2378 THEN ausp.charcvalue ELSE NULL END,', ') AS end_longitude
    ,LISTAGG(CASE WHEN ausp.charcinternalid = 2381 THEN ausp.charcvalue ELSE NULL END,', ') AS end_latitude
    ,LISTAGG(CASE WHEN ausp.charcinternalid = 1979 THEN ausp.charcvalue ELSE NULL END,', ') AS substation_id  
    ,LISTAGG(CASE WHEN ausp.charcinternalid = 2716 THEN ausp.charcvalue ELSE NULL END,', ') AS depot_area_code
    ,LISTAGG(CASE WHEN ausp.charcinternalid = 1977 THEN ausp.charcvalue ELSE NULL END,', ') AS atmospheric_corrosion_zone_code  
    ,LISTAGG(CASE WHEN ausp.charcinternalid = 2727 THEN ausp.charcvalue ELSE NULL END,', ') AS lga_council_code 
    ,LISTAGG(CASE WHEN ausp.charcinternalid = 1989 THEN ausp.charcvalue ELSE NULL END,', ') AS corrosion_zone_ground_level_code  
    ,LISTAGG(CASE WHEN ausp.charcinternalid = 1976 THEN ausp.charcvalue ELSE NULL END,', ') AS bushfire_risk_area_code  
    ,LISTAGG(CASE WHEN ausp.charcinternalid = 1810 THEN ausp.charcvalue ELSE NULL END,', ') AS fire_ban_district1_name
    ,LISTAGG(CASE WHEN ausp.charcinternalid = 2209 THEN ausp.charcvalue ELSE NULL END,', ') AS fire_ban_district2_name  
    ,LISTAGG(CASE WHEN ausp.charcinternalid = 2210 THEN ausp.charcvalue ELSE NULL END,', ') AS fire_ban_district3_name
    ,max(iloa.assetroom) as feeder_id
    FROM {{ source('sap_erp', 'stg_iflot') }} iflot
    INNER JOIN {{ ref('ausp_attributes') }} ausp
        ON iflot.functionallocation = ausp.clfnobjectid
        AND ausp.classtype = '003'
    LEFT JOIN {{ source('sap_erp', 'stg_iloa') }} iloa 
        ON iloa.maintobjectlocacctassgmtnmbr = iflot.maintobjectlocacctassgmtnmbr
        AND iloa.functionallocation = iflot.functionallocation
    WHERE iflot.uuid is not null 
        and iflot.functionallocationcategory not in ('1','2','4','C','F','I','J','L','M','P','T','Y') -- remove non-network assets
    GROUP BY iflot.uuid,iflot.functionallocation,iflot.lastchangedate
),
network_asset_attributes_transformed as (
    SELECT 
        uuid
        ,functionallocation
        ,case when span_voltage='' then null else span_voltage end AS span_voltage
        ,case when operating_voltage='' then null else operating_voltage end AS operating_voltage
        ,case when voltages='' then null else voltages end AS voltages
        ,case when operating_voltage_kv='' then null else operating_voltage_kv end AS operating_voltage_kv
        ,case when feature_id='' then null else feature_id end AS feature_id 
        --Lat/Long
        ,case when try_cast(longitude as float) is null then null
                when try_cast(latitude as float) is null then null
                when longitude::number >180 or longitude::number <-180 then null
                when latitude::number >90 or latitude::number <-90 then null
                else longitude
        end AS longitude
        ,case when try_cast(latitude as float) is null then null
                when try_cast(longitude as float) is null then null
                when latitude::number >90 or latitude::number <-90 then null
                when longitude::number >180 or longitude::number <-180 then null
                else latitude
        end AS latitude
        ,case when span_g3e_fid='' then null else span_g3e_fid end AS span_g3e_fid
        ,case when phasing_val='' then null else phasing_val end AS phasing_val
        ,case when try_cast(start_longitude as float) is null then null
                when try_cast(start_latitude as float) is null then null
                when start_longitude::number >180 or start_longitude::number <-180 then null
                when start_latitude::number >90 or start_latitude::number <-90 then null
                else start_longitude
        end AS start_longitude
        ,case when try_cast(start_latitude as float) is null then null
                when try_cast(start_longitude as float) is null then null
                when start_latitude::number >90 or start_latitude::number <-90 then null
                when start_longitude::number >180 or start_longitude::number <-180 then null
                else start_latitude
        end AS start_latitude
        ,case when try_cast(end_longitude as float) is null then null
                when try_cast(end_latitude as float) is null then null
                when end_longitude::number >180 or end_longitude::number <-180 then null
                when end_latitude::number >90 or end_latitude::number <-90 then null
                else end_longitude
        end AS end_longitude
        ,case when try_cast(end_latitude as float) is null then null
                when try_cast(end_longitude as float) is null then null
                when end_latitude::number >90 or end_latitude::number <-90 then null
                when end_longitude::number >180 or end_longitude::number <-180 then null
                else end_latitude
        end AS end_latitude
        ,case when substation_id='' then null else substation_id end AS substation_id  
        ,case when depot_area_code='' then null else depot_area_code end AS depot_area_code
        ,case when atmospheric_corrosion_zone_code='' then null else atmospheric_corrosion_zone_code end AS atmospheric_corrosion_zone_code  
        ,case when lga_council_code='' then null else lga_council_code end AS lga_council_code 
        ,case when corrosion_zone_ground_level_code='' then null else corrosion_zone_ground_level_code end AS corrosion_zone_ground_level_code  
        ,case when bushfire_risk_area_code='' then null else bushfire_risk_area_code end AS bushfire_risk_area_code  
        ,case when fire_ban_district1_name='' then null else fire_ban_district1_name end AS fire_ban_district1_name
        ,case when fire_ban_district2_name='' then null else fire_ban_district2_name end AS fire_ban_district2_name
        ,case when fire_ban_district3_name='' then null else fire_ban_district3_name end AS fire_ban_district3_name
        ,case when feeder_id='' then null else feeder_id end as feeder_id
    from network_asset_attributes 
    where uuid_rank=1
)
    SELECT *
           ,st_centroid(st_makeline(
                           st_makepoint(start_longitude, start_latitude),
                           st_makepoint(end_longitude, end_latitude)
                       )) as span_midpoint
           ,st_makepoint(longitude, latitude) as midpoint
    FROM network_asset_attributes_transformed