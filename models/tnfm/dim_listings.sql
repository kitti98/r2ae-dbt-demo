WITH raw_listings AS (
     SELECT
         *
     FROM
         {{ ref("raw_listings") }}
)
SELECT
     listing_id,
     listing_name,
     listing_url,
     room_type,
     {{ change_dollar_sign_to_decimal("price_str", "decimal") }},
     {{ change_dollar_sign_to_decimal("price_str", "integer") }},
     src.neighbourhood AS neighbourhood,
     nbh.neighbourhood_group AS tier
FROM
 raw_listings AS src
LEFT JOIN {{ ref("seed_neighbourhood") }} AS nbh
ON nbh.neighbourhood = src.neighbourhood
