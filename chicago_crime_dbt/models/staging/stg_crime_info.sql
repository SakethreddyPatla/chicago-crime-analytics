WITH source AS (
    SELECT * FROM {{ source('raw', 'CRIME_INFO')}}
    WHERE ID IS NOT NULL
),

renamed AS (
    SELECT 
        ID AS crime_id,
        CASE_NUMBER AS case_number,
        TO_TIMESTAMP(DATE / 1000000000)::DATE AS crime_date,
        BLOCK AS incident_occured_block,
        IUCR AS iucr,
        PRIMARY_TYPE AS crime_type,
        DESCRIPTION AS crime_description,
        LOCATION_DESCRIPTION AS location_type,
        CASE WHEN ARREST='True' THEN TRUE ELSE FALSE END AS is_arrest,
        CASE WHEN DOMESTIC='True' THEN TRUE ELSE FALSE END AS is_domestic,
        BEAT AS beat,
        DISTRICT AS police_district,
        WARD AS ward,
        COMMUNITY_AREA AS community_area,
        FBI_CODE AS fbi_code,
        YEAR::INT AS crime_year,
        TO_TIMESTAMP(UPDATED_ON / 1000000000)::DATE AS updated_on,
        LATITUDE::float AS latitude,
        LONGITUDE::float AS longitude,
        current_timestamp() AS loaded_at
    FROM source
)
SELECT * FROM renamed