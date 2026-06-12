-- Table for viewing records with invalid number of passengers
SELECT
	vendor_id,
	pickup_time,
	dropoff_time,
	passenger_count,
	trip_distance,
	ratecode_id,
	store_and_fwd,
	pickup_location,
	dropoff_location,
	payment_type,
	fare_amount,
	extra,
	mta_tax,
	tip_amount,
	tolls_amount,
	total_amount,
	congestion_surcharge
FROM FROM {{ ref('taxi_silver') }}
WHERE passenger_count IS NULL
OR passenger_count < 1
OR passenger_count > 4