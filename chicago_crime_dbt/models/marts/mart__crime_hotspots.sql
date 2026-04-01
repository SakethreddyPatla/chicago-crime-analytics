WITH staging AS (
    SELECT * FROM {{ ref('stg_crime_info') }}
    WHERE crime_id IS NOT NUll
)

SELECT
    community_area,
    crime_type,
    crime_year,
    COUNT(*)                                            AS total_crimes,
    SUM(CASE WHEN is_arrest THEN 1 ELSE 0 END)          AS total_arrests,
    SUM(CASE WHEN is_domestic THEN 1 ELSE 0 END)        AS domestic_crimes,
    RANK() OVER (
        PARTITION BY crime_year
        ORDER BY COUNT(*) DESC
    )                                                   AS hotspot_rank
FROM staging
WHERE community_area IS NOT NULL
GROUP BY community_area, crime_type, crime_year
QUALIFY hotspot_rank <= 20
ORDER BY crime_year DESC, hotspot_rank ASC