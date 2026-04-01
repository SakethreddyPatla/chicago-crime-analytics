WITH staging AS (
    SELECT * FROM {{ ref('stg_crime_info') }}
    WHERE crime_id IS NOT NUll
),
yearly AS (
    SELECT 
        crime_year,
        COUNT(*) AS total_crimes,
        SUM(CASE WHEN is_arrest='true' THEN 1 ELSE 0 END) AS total_arrests
    FROM staging
    WHERE crime_year IS NOT NULL
    GROUP BY crime_year
)

SELECT 
    crime_year,
    total_crimes,
    total_arrests,
    LAG(total_crimes) OVER(ORDER BY crime_year) AS prev_year_crimes,
    total_crimes - LAG(total_crimes) OVER(ORDER BY crime_year) AS YOY_change,
    ROUND(
        (total_crimes - LAG(total_crimes) OVER(ORDER BY crime_year)) * 100 
        / NULLIF(LAG(total_crimes) OVER(ORDER BY crime_year), 0), 2) AS YOY_change_pct
    FROM yearly
    ORDER BY crime_year ASC