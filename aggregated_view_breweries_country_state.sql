-- Aggregated view of the quantity of breweries per type, state, city, and country
CREATE OR REPLACE VIEW breweries_july_dataset.aggregated_breweries_country_state AS
SELECT
  brewery_type,
  country,
  state,
  city,
  COUNT(*) AS quantity
FROM (
  SELECT
    brewery_type,
    'austria' AS country,
    state,
    city
  FROM
    `abinbev-case.breweries_july_dataset.austria_breweries`
  UNION ALL
  SELECT
    brewery_type,
    'england' AS country,
    state,
    city
  FROM
    `abinbev-case.breweries_july_dataset.england_brewweries`
  UNION ALL
  SELECT
    brewery_type,
    'france' AS country,
    state,
    city
  FROM
    `abinbev-case.breweries_july_dataset.france_breweries`
  UNION ALL
  SELECT
    brewery_type,
    'ireland' AS country,
    state,
    city
  FROM
    `abinbev-case.breweries_july_dataset.ireland_breweries`
  UNION ALL
  SELECT
    brewery_type,
    'isle_of_man' AS country,
    state,
    city
  FROM
    `abinbev-case.breweries_july_dataset.isle_of_man_breweries`
  UNION ALL
  SELECT
    brewery_type,
    'poland' AS country,
    state,
    city
  FROM
    `abinbev-case.breweries_july_dataset.poland_breweries`
  UNION ALL
  SELECT
    brewery_type,
    'portugal' AS country,
    state,
    city
  FROM
    `abinbev-case.breweries_july_dataset.portugal_breweries`
  UNION ALL
  SELECT
    brewery_type,
    'scotland' AS country,
    state,
    city
  FROM
    `abinbev-case.breweries_july_dataset.scotland_breweries`
  UNION ALL
  SELECT
    brewery_type,
    'singapore' AS country,
    state,
    city
  FROM
    `abinbev-case.breweries_july_dataset.singapore_breweries`
  UNION ALL
  SELECT
    brewery_type,
    'south_korea' AS country,
    state,
    city
  FROM
    `abinbev-case.breweries_july_dataset.south_korea_breweries`
  UNION ALL
  SELECT
    brewery_type,
    'sweden' AS country,
    state,
    city
  FROM
    `abinbev-case.breweries_july_dataset.sweden_breweries`
  UNION ALL
  SELECT
    brewery_type,
    'usa' AS country,
    state,
    city
  FROM
    `abinbev-case.breweries_july_dataset.usa_total_breweries`
)
GROUP BY
  brewery_type,
  country,
  state,
  city
ORDER BY
  country,
  state,
  city,
  brewery_type;
