# Architecture

## Overview

This pipeline simulates and processes e-commerce order data through a
bronze/silver/gold (medallion) lakehouse pattern, landing the final,
dimensionally-modeled data in Postgres.

## Data flow

### 1. Producer -> Kafka

`producers/producer.py` generates fake orders using `faker` (customer IDs,
one of 4 hardcoded products, quantity, computed `total_amount`, and an
ISO-8601 UTC `order_ts`) and publishes each as a JSON message to the Kafka
topic **`orders`**, keyed by `customer_id`. It connects to
`localhost:9092` (hardcoded, not read from `.env`).

Kafka itself (`docker-compose.yml`, service `kafka`) runs as a **single-node
broker in KRaft mode** (no ZooKeeper) with replication factor 1 for internal
topics — this is a development configuration, not production-durable (see
[README.md Production notes](../README.md#production-notes)). `kafka-ui` is
included for browsing topics/messages at `localhost:8080`.

### 2. Kafka -> Spark Structured Streaming -> bronze

`spark/streaming/kafka_to_bronze.py` is a long-running Spark Structured
Streaming job that:
- Subscribes to the `orders` topic from `earliest` offset.
- Parses the JSON `value` against an explicit schema
  (`order_id`, `customer_id`, `product_id`, `quantity`, `unit_price`,
  `total_amount`, `order_ts`).
- Derives an `order_date` column from `order_ts`.
- Writes append-only Parquet, partitioned by `order_date`, to
  `s3a://<DO_SPACES_BUCKET>/bronze/orders/` on DigitalOcean Spaces, with a
  30-second micro-batch trigger and checkpointing to
  `s3a://<DO_SPACES_BUCKET>/bronze/_checkpoints/orders/`.

This is the **bronze layer**: raw, append-only, schema-parsed but not
cleaned or deduplicated.

### 3. Bronze -> silver (batch)

`spark/batch/bronze_to_silver.py` reads the entire bronze dataset and:
- Casts `order_ts` to a proper timestamp type.
- Drops rows with a null `order_id` or `customer_id`, non-positive
  `quantity`, or negative `unit_price`.
- De-duplicates on `order_id`.
- Overwrites `s3a://<DO_SPACES_BUCKET>/silver/orders/`, partitioned by
  `order_date` (full overwrite each run, not incremental).

This is the **silver layer**: cleaned, validated, deduplicated order records.

### 4. Silver -> gold (batch, star schema)

`spark/batch/silver_to_gold.py` reads the silver dataset and builds four
in-memory DataFrames, each assigned a surrogate key via
`monotonically_increasing_id()`:

- `dim_customer` — distinct `customer_id` values.
- `dim_product` — distinct `(product_id, unit_price)` combinations.
- `dim_date` — distinct `order_date` values, with `day`/`month`/`quarter`/
  `year`/`is_weekend` derived columns.
- `fact_sales` — silver orders joined against the three dimensions to resolve
  surrogate keys, keeping `order_id`, `quantity`, `unit_price`,
  `total_amount`, `order_ts`.

Each DataFrame is written to Postgres over JDBC using
`.mode("overwrite")` — every run fully replaces the four tables rather than
upserting/appending new rows.

### 5. Orchestration (Airflow)

`airflow/dags/etl_dag.py` defines a DAG `ecommerce_etl`, scheduled `@hourly`,
with two `BashOperator` tasks:

```
bronze_to_silver  >>  silver_to_gold
```

Each task runs the corresponding batch script directly with `python` inside
the Airflow container (which has `pyspark`/`python-dotenv` pip-installed per
`docker/airflow.Dockerfile`) — not via `spark-submit`. The DAG only covers
the **batch** stage; the producer and the streaming `kafka_to_bronze.py` job
run outside of Airflow as separate long-running processes.

Airflow itself runs with `SequentialExecutor` and a SQLite metadata database
(`docker-compose.yml`) — a single-threaded, development-only configuration.

### 6. Gold layer storage (Postgres)

The gold-layer tables live in a Postgres database (DigitalOcean managed
Postgres, in this project) reached over JDBC with `sslmode=require`. Schema
is defined in `sql/ddl/star_schema.sql` and must be applied once, ahead of
time (Spark's JDBC overwrite mode does not guarantee the DDL's constraints
survive subsequent overwrites — see the corresponding TODO in
[README.md](../README.md)).

## Star schema

```
                        dim_date
                     ------------------
                     date_key (PK)
                     date
                     day
                     month
                     quarter
                     year
                     is_weekend
                            ^
                            |
dim_customer                |                dim_product
------------------          |                ------------------
customer_key (PK)           |                product_key (PK)
customer_id                 |                product_id
        ^                   |                unit_price
        |                   |                        ^
        |                   |                        |
        +---------------fact_sales----------------------+
                     ------------------
                     order_id (PK)
                     date_key (FK -> dim_date)
                     customer_key (FK -> dim_customer)
                     product_key (FK -> dim_product)
                     quantity
                     unit_price
                     total_amount
                     order_ts
```

`fact_sales` is grain-per-order (one row per `order_id`), with foreign keys
to each dimension. The exact DDL is in `sql/ddl/star_schema.sql`:

- `dim_customer(customer_key PK, customer_id UNIQUE NOT NULL)`
- `dim_product(product_key PK, product_id, unit_price NOT NULL)`
- `dim_date(date_key PK, date UNIQUE NOT NULL, day, month, quarter, year, is_weekend)`
- `fact_sales(order_id PK, date_key FK, customer_key FK, product_key FK, quantity NOT NULL, unit_price NOT NULL, total_amount NOT NULL, order_ts)`
