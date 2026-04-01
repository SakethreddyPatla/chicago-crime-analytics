WITH staging AS (
    SELECT * FROM {{ ref('stg_crime_info') }}
    WHERE crime_id IS NOT NUll
)
SELECT 
    location_type,
    COUNT(*) AS total_crimes,
    SUM(CASE WHEN is_arrest = 'true' THEN 1 ELSE 0 END) AS total_arrests,
    SUM(CASE WHEN is_domestic = 'true' THEN 1 ELSE 0 END) AS total_domestic_cases
FROM staging 
GROUP BY location_type
ORDER BY total_crimes DESC 