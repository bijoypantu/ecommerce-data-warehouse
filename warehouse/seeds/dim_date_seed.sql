-- dim_data data insertion
INSERT INTO dw.dim_date (
  date_sk,
  full_date,
  year,
  quarter,
  month_number,
  month_name,
  day_of_month,
  day_name,
  day_of_week,
  week_of_year,
  is_weekend
)
SELECT
	(
	EXTRACT(year FROM d)::int * 10000
	+ EXTRACT(month FROM d)::int * 100
	+ EXTRACT(day FROM d)::int
	) AS date_sk,
	d::date AS full_date,
  	EXTRACT(year FROM d)::int AS year,
	  EXTRACT(quarter FROM d)::int AS quarter,
	  EXTRACT(month FROM d)::int AS month_number,
	  TRIM(to_char(d, 'Month')) AS month_name,
	  EXTRACT(day FROM d)::int AS day_of_month,
	  TRIM(to_char(d, 'Day'))   AS day_name,
	  EXTRACT(isodow FROM d)::int AS day_of_week,
	  EXTRACT(week FROM d)::int AS week_of_year,
	  CASE 
	    WHEN EXTRACT(isodow FROM d) IN (6, 7) THEN true
	    ELSE false
	  END AS is_weekend
FROM generate_series(
  '2000-01-01'::date,
  '2035-12-31'::date,
  '1 day'::interval
) AS d;

