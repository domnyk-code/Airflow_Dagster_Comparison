{{ config(
    materialized='incremental',
    on_schema_change='sync_all_columns'
) }}

-- Load incrementally if possible - after first run it should append the new rows to an existing table
WITH source AS (
    SELECT * FROM {{ source('raw', 'taxi_trips') }}
    {% if is_incremental() %}
        WHERE _loaded_at > (SELECT MAX(_loaded_at) FROM {{ this }})
    {% endif %}
),

nyc_taxi_bronze AS (
    SELECT
        "VendorID",
        tpep_pickup_datetime,
        tpep_dropoff_datetime,
        passenger_count,
        trip_distance,
        "RatecodeID",
        store_and_fwd_flag,
        "PULocationID",
        "DOLocationID",
        payment_type,
        fare_amount,
        extra,
        mta_tax,
        tip_amount,
        tolls_amount,
        total_amount,
        congestion_surcharge,
        _loaded_at,
        _source_file
    FROM source
)
SELECT * FROM nyc_taxi_bronze