import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_date
from pyspark.sql.types import StructType, StringType, IntegerType, DoubleType

os.environ["HADOOP_HOME"] = r"C:\hadoop"
os.environ["PATH"] = r"C:\hadoop\bin;" + os.environ["PATH"]
load_dotenv()

KEY = os.environ["DO_SPACES_KEY"]
SECRET = os.environ["DO_SPACES_SECRET"]
ENDPOINT = os.environ["DO_SPACES_ENDPOINT"]
BUCKET = os.environ["DO_SPACES_BUCKET"]

spark = (
    SparkSession.builder
    .appName("kafka-to-bronze")
    .config("spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
        "org.apache.hadoop:hadoop-aws:3.3.4,"
        "com.amazonaws:aws-java-sdk-bundle:1.12.262")
    # this is for digital ocean spaces, if you are using AWS S3, you can remove these lines
    .config("spark.hadoop.fs.s3a.endpoint", ENDPOINT)
    .config("spark.hadoop.fs.s3a.access.key", KEY)
    .config("spark.hadoop.fs.s3a.secret.key", SECRET)
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")
# Define the schema for the order data
order_schema = (
    StructType()
    .add("order_id", StringType())
    .add("customer_id", StringType())
    .add("product_id", StringType())
    .add("quantity", IntegerType())
    .add("unit_price", DoubleType())
    .add("total_amount", DoubleType())
    .add("order_ts", StringType())
)
# here we are actually getting the data from kafka and writing it to s3 in parquet format, partitioned by order_date
raw = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "orders")
    .option("startingOffsets", "earliest")
    .load()
)

parsed = (
    raw.selectExpr("CAST(value AS STRING) AS json_str")
    .select(from_json(col("json_str"), order_schema).alias("d"))
    .select("d.*")
    .withColumn("order_date", to_date(col("order_ts")))
)

query = (
    parsed.writeStream
    .format("parquet")
    .option("path", f"s3a://{BUCKET}/bronze/orders/")
    .option("checkpointLocation", f"s3a://{BUCKET}/bronze/_checkpoints/orders/")
    .partitionBy("order_date")
    .outputMode("append")
    .trigger(processingTime="30 seconds")
    .start()
)

query.awaitTermination()