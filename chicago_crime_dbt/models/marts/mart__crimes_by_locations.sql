WITH staging AS (
    SELECT * FROM {{ ref('stg_crime_info') }}
    WHERE crime_id IS NOT NUll
)
SELECT 
    police_district,
    community_area,
    COUNT(*) AS total_crimes,
    SUM(CASE WHEN is_arrest = 'true' THEN 1 ELSE 0 END) AS total_arrests,
    SUM(CASE WHEN is_domestic = 'true' THEN 1 ELSE 0 END) AS total_domestic_cases
FROM staging 
GROUP BY 1, 2
ORDER BY total_crimes DESC 
