


/********************************************************************************************************
vw_network_asset_gis_group_attributes
*********************************************************************************************************/
/*
WITH gis_group_attributes AS 
(
    SELECT
        SPLIT_PART(items.name, '.', -1) AS network_asset_class_type
        , xmlget(subtype.value, 'SubtypeCode'):"$"::integer as network_asset_gis_group_code
        , xmlget(subtype.value, 'SubtypeName'):"$"::varchar as network_asset_gis_group_description
    FROM {{ source('gis', 'vw_gdb_items_latest') }} items
    , lateral flatten(items.definition:"$") defeatureclassinfo
    , lateral flatten(to_array(defeatureclassinfo.value:"$")) subtype  -- extract content of Subtypes, to_array to capture single instance scenario
    WHERE GET(defeatureclassinfo.value, '@') = 'Subtypes'
 )
SELECT
    gatt.network_asset_class_type
    ,gatt.network_asset_gis_group_code
    ,gatt.network_asset_gis_group_description
FROM gis_group_attributes gatt
*/