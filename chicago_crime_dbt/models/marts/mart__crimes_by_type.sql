WITH staging AS (
    SELECT * FROM {{ ref('stg_crime_info') }}
    WHERE crime_id IS NOT NULL 
)

SELECT 
    crime_year,
    crime_type,
    COUNT(*) AS total_crimes,
    SUM(CASE WHEN is_arrest = 'true' THEN 1 ELSE 0 END) AS total_arrests,
    ROUND(SUM(CASE WHEN is_arrest = 'true' THEN 1 ELSE 0 END) * 100/
                COUNT(*), 2) AS total_arrests_pct
FROM staging 
GROUP BY crime_year, crime_type
ORDER by crime_year DESC, total_crimes DESC

