WITH staging AS (
    SELECT * FROM {{ ref('stg_crime_info') }}
    WHERE crime_id IS NOT NUll
)

SELECT 
    crime_year,
    MONTH(crime_date) AS crime_month,
    COUNT(*) AS total_crimes,
    SUM(CASE WHEN is_arrest='true' THEN 1 ELSE 0 END) AS total_arrests
FROM staging 
WHERE crime_date IS NOT NULL
GROUP BY crime_year, crime_month
ORDER BY crime_year DESC, crime_month ASC