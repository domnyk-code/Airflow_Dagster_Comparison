-- Simple script for getting analysis of daily trips
SELECT
    pickup_time::date AS trip_date,
    COUNT(*) AS total_trips,
    COUNT(DISTINCT vendor_id) as active_vendors,

    --Passenger metrics
    SUM(passenger_count) AS total_passengers,
    ROUND(AVG(passenger_count), 2) as avg_passengers,

    -- Financial metrics
    ROUND(SUM(fare_amount), 2) as total_fares,
	ROUND(SUM(tip_amount), 2) as total_tips,
	ROUND(SUM(tolls_amount), 2) as total_tolls,
	ROUND(SUM(total_amount), 2) as total_revenue,
	ROUND(AVG(fare_amount), 2) as average_fares
FROM {{ ref('taxi_silver') }}
GROUP BY trip_date
ORDER BY trip_date