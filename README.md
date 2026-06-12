# NYC Taxi Medallion Pipeline — Airflow vs Dagster Comparison

A fragment of a final project for BGD course presenting **Airflow vs Dagster** comparison 
on the same pipeline, utilising **dbt** as a data transformation tool.

```
ingest raw parquet
     ↓
bronze layer  (dbt: taxi_bronze)
     ↓
silver layer  (dbt: taxi_silver)       ← cleaning, typing, validation
     ↓
gold layer    (dbt: taxi_gold)   ← business aggregates
```

---

## Repository Structure

```
├── dbt/        
│    ├── dbt_project.yml
│    ├── profiles.yml
│    ├── macros/
│    │   └── generate_schema_name.sql
│    └── models/
│        ├── bronze/
│        │   └── taxi_bronze.sql
│        ├── silver/       taxi_silver.sql
│        │   └── taxi_silver.sql
│        └── gold/         gold_orders_daily.sql
│            ├── taxi_gold_daily_analysis.sql
│            ├── taxi_gold_invalid_passenger.sql
│            └── taxi_gold_vendor_analysis.sql
│
├── airflow/
│   ├── dags/
│   │   └── nyc_taxi_pipeline.py
│   ├── docker-compose.yml
│   ├── Dockerfile
│   └── requirements.txt
│
└── dagster/
    ├── medallion/
    │   └── assets/
    │       ├── bronze.py           ← @asset: Python ingestion
    │       └── dbt_assets.py       ← @dbt_assets: auto-generates silver+gold
    ├── definitions.py              ← Definitions object (jobs, schedules, sensors)
    ├── pyproject.toml
    ├── docker-compose.yml
    ├── Dockerfile
    └── dagster.yaml
```

---

## Data Source

The pipeline expects the **NYC TLC Yellow Taxi Trips** dataset at `data/`, in parquet format.

Link to data: https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page 

Each record in the dataset describes a single taxi trip registered by the carrier operating the vehicle. Each record contains the following fields:

    VendorID – Numeric identifier of the carrier.
    tpep_pickup_datetime – Date and time the trip started.
    tpep_dropoff_datetime – Date and time the trip ended.
    passenger_count – Number of passengers on the trip.
    trip_distance – Length of the trip (in miles).
    RatecodeID – Numeric identifier of the fare calculation method.
    store_and_fwd – Flag indicating whether trip data was sent to the carrier immediately or stored in the vehicle's memory first.
    PULocationID – Numeric identifier of the trip's pickup location.
    DOLocationID – Numeric identifier of the trip's drop-off location.
    payment_type – Numeric identifier of the payment method.
    fare_amount – Base fare for the trip (in dollars and cents).
    extra – Any additional charges (in dollars and cents).
    mta_tax – Tax amount calculated based on the applicable fare standard.
    tip_amount – Tip amount. Only tips paid by card are counted.
    tolls_amount – Total toll charges during the trip.
    total_amount – Sum of all charges and tips.
    congestion_surcharge – Additional surcharge for trips during peak hours.

The data is divided by year, and each year is further divided by month, meaning there are twelve datasets per year.

---

---

## Data Flow Diagram
![Data pipeline diagram extended](/images/pipeline_diagram_en.png)


## The Pipeline itself

The data was downloaded from the NYC TLC website in parquet format. The data files were placed in the data folder and then processed according to the medallion architecture.
The pipeline itself is the same for both Dagster and Airflow, for comparison purposes.

- In the bronze layer, using batch processing, raw data is loaded into the database in a table with general column formats. A field describing the data source (file name) and the timestamp of when the data was loaded are added. The **pandas** library is used here to load the data into a data frame.
- In the silver layer, data is copied from the bronze layer with conversion to appropriate column types. An initial filtering of records is performed, and records containing information that is invalid according to the accepted data rules are flagged. **dbt** is used for data processing in this layer, in combination with the appropriate SQL files.
- In the gold layer, tables are created containing specific information based on aggregations of data from the silver table. Tables were created to analyze daily trip statistics, taxi operator statistics, and tables containing suspicious records based on various criteria. **dbt** tools are also applied here to produce the tables.

---


---

## How to start the pipelines?

### Airflow

```bash
cd airflow/
# Place parquet file in a data folder (has to be created manually!)
docker compose up -d
# UI → http://localhost:8080  (admin / admin)
# Enable the `nyc_taxi_pipeline` DAG and trigger it manually.
```

### Dagster

```bash
cd dagster/
# Place parquet file in a data folder (has to be created manually!)
pip install -e .
dagster dev          # local dev mode — no Docker needed
# UI → http://localhost:3000
# Go to Assets → select all → Materialize
```

---

## Key differences between architectures

| Concept               | Airflow                              | Dagster                                   |
|-----------------------|--------------------------------------|-------------------------------------------|
| **Core primitive**    | Task (unit of work)                  | Asset (unit of data)                      |
| **DAG / graph**       | Defined explicitly with `>>`         | Inferred from function signatures         |
| **DBT integration**   | `astronomer-cosmos` (DbtTaskGroup)   | `dagster-dbt` (`@dbt_assets` decorator)   |
| **DBT granularity**   | One Airflow task per dbt model       | One Dagster asset per dbt model           |
| **Scheduling**        | Cron-first (`schedule` on DAG)       | Schedule + Sensor + manual materialise    |
| **Resources**         | Connections (UI-configured)          | Typed Python objects (injected)           |
| **Observability**     | Task logs, Gantt chart               | Asset catalogue, materialisation history  |
| **Run metadata**      | XCom (key-value push/pull)           | `MaterializeResult` (typed, structured)   |
| **File sensor**       | FileSensor plugin                    | First-class `@sensor` primitive           |
| **Boilerplate**       | More (explicit task wiring)          | Less (declarative, inference-heavy)       |

---

