CREATE OR REPLACE TABLE `project-e17ceb98-1e65-4608-bf8.reporting_db.rentals_per_period_by_customer` AS
WITH rentals AS (
  SELECT *
  FROM `project-e17ceb98-1e65-4608-bf8.staging_db.stg_rental`
),

customers AS (
  SELECT *
  FROM `project-e17ceb98-1e65-4608-bf8.staging_db.stg_customer`
),

reporting_dates AS (
  SELECT *
  FROM `project-e17ceb98-1e65-4608-bf8.reporting_db.reporting_periods_table`
  WHERE reporting_period IN ('Day', 'Month', 'Year')
),

rentals_per_period AS (
  SELECT
    'Day' AS reporting_period,
    DATE_TRUNC(CAST(rentals.rental_date AS DATE), DAY) AS reporting_date,
    customers.customer_id,
    COUNT(*) AS total_rentals
  FROM rentals
  LEFT JOIN customers
    ON rentals.customer_id = customers.customer_id
  GROUP BY 1, 2, 3

  UNION ALL

  SELECT
    'Month' AS reporting_period,
    DATE_TRUNC(CAST(rentals.rental_date AS DATE), MONTH) AS reporting_date,
    customers.customer_id,
    COUNT(*) AS total_rentals
  FROM rentals
  LEFT JOIN customers
    ON rentals.customer_id = customers.customer_id
  GROUP BY 1, 2, 3

  UNION ALL

  SELECT
    'Year' AS reporting_period,
    DATE_TRUNC(CAST(rentals.rental_date AS DATE), YEAR) AS reporting_date,
    customers.customer_id,
    COUNT(*) AS total_rentals
  FROM rentals
  LEFT JOIN customers
    ON rentals.customer_id = customers.customer_id
  GROUP BY 1, 2, 3
),

final AS (
  SELECT
    reporting_dates.reporting_period,
    reporting_dates.reporting_date,
    rentals_per_period.customer_id,
    rentals_per_period.total_rentals AS total_rentals
  FROM reporting_dates
  INNER JOIN rentals_per_period
    ON reporting_dates.reporting_period = rentals_per_period.reporting_period
   AND reporting_dates.reporting_date = rentals_per_period.reporting_date
  WHERE reporting_dates.reporting_period = 'Day'

  UNION ALL

  SELECT
    reporting_dates.reporting_period,
    reporting_dates.reporting_date,
    rentals_per_period.customer_id,
    rentals_per_period.total_rentals AS total_rentals
  FROM reporting_dates
  INNER JOIN rentals_per_period
    ON reporting_dates.reporting_period = rentals_per_period.reporting_period
   AND reporting_dates.reporting_date = rentals_per_period.reporting_date
  WHERE reporting_dates.reporting_period = 'Month'

  UNION ALL

  SELECT
    reporting_dates.reporting_period,
    reporting_dates.reporting_date,
    rentals_per_period.customer_id,
    rentals_per_period.total_rentals AS total_rentals
  FROM reporting_dates
  INNER JOIN rentals_per_period
    ON reporting_dates.reporting_period = rentals_per_period.reporting_period
   AND reporting_dates.reporting_date = rentals_per_period.reporting_date
  WHERE reporting_dates.reporting_period = 'Year'
)

SELECT *
FROM final
ORDER BY customer_id, reporting_period, reporting_date;