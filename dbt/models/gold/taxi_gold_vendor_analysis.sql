-- -- Trip summary by vendor
SELECT
-- Add vendor name table possibly
	vendor_id,
	COUNT(*) as total_trips,
	COUNT(DISTINCT DATE(pickup_time)) as days_active,
	ROUND(COUNT(*) / COUNT(DISTINCT DATE(pickup_time)), 2) as avg_trips_daily,

	-- Trip metrics
	ROUND(AVG(trip_distance), 2) as avg_distance,
	ROUND(AVG(passenger_count), 2) as avg_passengers,

	-- Financial metrics
	ROUND(AVG(fare_amount), 2) as avg_fare,
	ROUND(AVG(total_amount), 2) as avg_revenue,
	ROUND(SUM(total_amount) ,2) as total_lifetime_revenue,
	ROUND(SUM(tip_amount), 2) as total_lifetime_tips,
	ROUND(AVG(tip_amount), 2) as avg_tip_per_trip
FROM {{ ref('taxi_silver') }}
GROUP BY vendor_id