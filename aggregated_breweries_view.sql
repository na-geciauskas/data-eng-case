-- Aggregated view of the quantity of breweries per type and location
CREATE OR REPLACE VIEW breweries_july_dataset.aggregated_breweries AS
SELECT
  brewery_type,
  location,
  COUNT(*) AS quantity
FROM (
  SELECT
    brewery_type,
    'austria' AS location
  FROM
    `abinbev-case.breweries_july_dataset.austria_breweries`
  UNION ALL
  SELECT
    brewery_type,
    'england' AS location
  FROM
    `abinbev-case.breweries_july_dataset.england_brewweries`
  UNION ALL
  SELECT
    brewery_type,
    'france' AS location
  FROM
    `abinbev-case.breweries_july_dataset.france_breweries`
  UNION ALL
  SELECT
    brewery_type,
    'ireland' AS location
  FROM
    `abinbev-case.breweries_july_dataset.ireland_breweries`
  UNION ALL
  SELECT
    brewery_type,
    'isle_of_man' AS location
  FROM
    `abinbev-case.breweries_july_dataset.isle_of_man_breweries`
  UNION ALL
  SELECT
    brewery_type,
    'poland' AS location
  FROM
    `abinbev-case.breweries_july_dataset.poland_breweries`
  UNION ALL
  SELECT
    brewery_type,
    'portugal' AS location
  FROM
    `abinbev-case.breweries_july_dataset.portugal_breweries`
  UNION ALL
  SELECT
    brewery_type,
    'scotland' AS location
  FROM
    `abinbev-case.breweries_july_dataset.scotland_breweries`
  UNION ALL
  SELECT
    brewery_type,
    'singapore' AS location
  FROM
    `abinbev-case.breweries_july_dataset.singapore_breweries`
  UNION ALL
  SELECT
    brewery_type,
    'south_korea' AS location
  FROM
    `abinbev-case.breweries_july_dataset.south_korea_breweries`
  UNION ALL
  SELECT
    brewery_type,
    'sweden' AS location
  FROM
    `abinbev-case.breweries_july_dataset.sweden_breweries`
  UNION ALL
  SELECT
    brewery_type,
    'usa' AS location
  FROM
    `abinbev-case.breweries_july_dataset.usa_total_breweries`
)
GROUP BY
  brewery_type,
  location
ORDER BY
  location,
  brewery_type;
