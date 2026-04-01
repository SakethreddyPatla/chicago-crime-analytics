WITH staging AS (
    SELECT * FROM {{ ref('stg_crime_info') }}
    WHERE crime_id IS NOT NUll
)
SELECT 
    police_district,
    crime_year,
    COUNT(*) AS total_crimes,
    SUM(CASE WHEN is_arrest='true' THEN 1 ELSE 0 END) AS total_arrests,
    ROUND(SUM(CASE WHEN is_arrest='true' THEN 1 ELSE 0 END) * 100 /
                COUNT(*), 2) AS arrests_rate_pct,
    RANK() OVER(PARTITION BY crime_year ORDER BY COUNT(*) DESC) AS rank 
FROM staging 
GROUP BY police_district, crime_year
QUALIFY rank <= 10
ORDER BY crime_year ASC, rank ASC