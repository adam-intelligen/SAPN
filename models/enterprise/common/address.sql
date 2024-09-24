



SELECT 
   addressid AS address_id
  ,NULL as address_point -- #TODO will be geo location
  ,NULL as address_name -- #TODO
  ,housenumber1 as street_number
  ,INITCAP(streetname) as street_name
  ,postalcode as post_code
  ,UPPER(cityname) as city_name
  ,UPPER(NVL(region,'XX')) AS state_code 
  ,DECODE(UPPER(region)
       ,'SA','South Australia'
       ,'VIC','Victoria'
       ,'NSW','New South Wales'
       ,'QLD','Queensland'
       ,'WA','Western Australia'
       ,'NT','Northern Territory'
       ,'TAS','Tasmania'
       ,'ACT','Australian Capital Territory'
       ,'Not Applicable'     
  ) AS state_name
  ,NVL(country,'XX') AS country_code
  ,NULL as country_name
  ,HASH(CONCAT(NVL(address_id,'x'))) AS md_key_hash
  ,HASH(CONCAT(NVL(address_id,'x'),NVL(address_point,'x'),NVL(address_name,'x'),NVL(street_number,'x'),NVL(street_name,'x'),NVL(post_code,'x')
              ,NVL(city_name,'x'),NVL(state_code,'x'),NVL(state_name,'x'),NVL(country_code,'x'),NVL(country_name,'x')  
  )) AS md_row_hash
FROM {{ source('sap_erp', 'stg_adrc') }}