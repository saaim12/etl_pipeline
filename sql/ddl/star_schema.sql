CREATE TABLE IF NOT EXISTS dim_customer (
    customer_key BIGINT PRIMARY KEY,
    customer_id  VARCHAR(20) NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS dim_product (
    product_key BIGINT PRIMARY KEY,
    product_id  VARCHAR(20) NOT NULL,
    unit_price  NUMERIC(12,2) NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_date (
    date_key   BIGINT PRIMARY KEY,
    date       DATE NOT NULL UNIQUE,
    day        INT,
    month      INT,
    quarter    INT,
    year       INT,
    is_weekend BOOLEAN
);

CREATE TABLE IF NOT EXISTS fact_sales (
    order_id     VARCHAR(64) PRIMARY KEY,
    date_key     BIGINT REFERENCES dim_date(date_key),
    customer_key BIGINT REFERENCES dim_customer(customer_key),
    product_key  BIGINT REFERENCES dim_product(product_key),
    quantity     INT NOT NULL,
    unit_price   NUMERIC(12,2) NOT NULL,
    total_amount NUMERIC(12,2) NOT NULL,
    order_ts     TIMESTAMP
);