SELECT
    -- keys
    crime_id,
    case_number,

    -- time dimensions
    crime_date,
    crime_year,
    MONTH(crime_date) AS crime_month,
    DAYOFWEEK(crime_date) AS crime_day_of_week,
    

    -- location dimensions
    incident_occured_block,
    police_district,
    ward,
    community_area,
    location_type,
    latitude,
    longitude,

    -- crime dimensions
    iucr,
    crime_type,
    crime_description,
    fbi_code,

    -- measures/flags
    is_arrest,
    is_domestic,

    -- metadata
    updated_on
FROM {{ ref('stg_crime_info') }}
WHERE crime_id IS NOT NULL