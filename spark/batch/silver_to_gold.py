import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, dayofmonth, month, quarter, year, dayofweek, monotonically_increasing_id
)

os.environ["HADOOP_HOME"] = r"C:\hadoop"
os.environ["PATH"] = r"C:\hadoop\bin;" + os.environ["PATH"]
load_dotenv()

KEY = os.environ["DO_SPACES_KEY"]
SECRET = os.environ["DO_SPACES_SECRET"]
ENDPOINT = os.environ["DO_SPACES_ENDPOINT"]
BUCKET = os.environ["DO_SPACES_BUCKET"]

PG_HOST = os.environ["PG_HOST"]
PG_PORT = os.environ["PG_PORT"]
PG_DB = os.environ["PG_DATABASE"]
PG_USER = os.environ["PG_USER"]
PG_PASSWORD = os.environ["PG_PASSWORD"]

JDBC_URL = f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DB}?sslmode=require"

spark = (
    SparkSession.builder
    .appName("silver-to-gold")
    .config("spark.jars.packages",
        "org.apache.hadoop:hadoop-aws:3.3.4,"
        "com.amazonaws:aws-java-sdk-bundle:1.12.262,"
        "org.postgresql:postgresql:42.7.3")
    .config("spark.hadoop.fs.s3a.endpoint", ENDPOINT)
    .config("spark.hadoop.fs.s3a.access.key", KEY)
    .config("spark.hadoop.fs.s3a.secret.key", SECRET)
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

silver = spark.read.parquet(f"s3a://{BUCKET}/silver/orders/")

# --- dim_customer ---
dim_customer = (
    silver.select("customer_id").distinct()
    .withColumn("customer_key", monotonically_increasing_id())
)

# --- dim_product ---
dim_product = (
    silver.select("product_id", "unit_price").distinct()
    .withColumn("product_key", monotonically_increasing_id())
)

# --- dim_date ---
dim_date = (
    silver.select(col("order_date").alias("date")).distinct()
    .withColumn("date_key", monotonically_increasing_id())
    .withColumn("day", dayofmonth("date"))
    .withColumn("month", month("date"))
    .withColumn("quarter", quarter("date"))
    .withColumn("year", year("date"))
    .withColumn("is_weekend", (dayofweek("date").isin(1, 7)))
)

# --- fact_sales (join to get surrogate keys) ---
fact_sales = (
    silver
    .join(dim_customer, "customer_id")
    .join(dim_product, ["product_id", "unit_price"])
    .join(dim_date, silver.order_date == dim_date.date)
    .select(
        "order_id",
        "date_key",
        "customer_key",
        "product_key",
        "quantity",
        "unit_price",
        "total_amount",
        "order_ts",
    )
)

def write_pg(df, table):
    (df.write
        .format("jdbc")
        .option("url", JDBC_URL)
        .option("dbtable", table)
        .option("user", PG_USER)
        .option("password", PG_PASSWORD)
        .option("driver", "org.postgresql.Driver")
        .mode("overwrite")
        .save())
    print(f"Wrote {table}: {df.count()} rows")

write_pg(dim_customer, "dim_customer")
write_pg(dim_product, "dim_product")
write_pg(dim_date, "dim_date")
write_pg(fact_sales, "fact_sales")

spark.stop()