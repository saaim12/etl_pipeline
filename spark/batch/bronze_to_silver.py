import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp

os.environ["HADOOP_HOME"] = r"C:\hadoop"
os.environ["PATH"] = r"C:\hadoop\bin;" + os.environ["PATH"]
load_dotenv()

KEY = os.environ["DO_SPACES_KEY"]
SECRET = os.environ["DO_SPACES_SECRET"]
ENDPOINT = os.environ["DO_SPACES_ENDPOINT"]
BUCKET = os.environ["DO_SPACES_BUCKET"]

spark = (
    SparkSession.builder
    .appName("bronze-to-silver")
    .config("spark.jars.packages",
        "org.apache.hadoop:hadoop-aws:3.3.4,"
        "com.amazonaws:aws-java-sdk-bundle:1.12.262")
    .config("spark.hadoop.fs.s3a.endpoint", ENDPOINT)
    .config("spark.hadoop.fs.s3a.access.key", KEY)
    .config("spark.hadoop.fs.s3a.secret.key", SECRET)
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

bronze = spark.read.parquet(f"s3a://{BUCKET}/bronze/orders/")

silver = (
    bronze
    .withColumn("order_ts", to_timestamp(col("order_ts")))
    .filter(col("order_id").isNotNull())
    .filter(col("customer_id").isNotNull())
    .filter(col("quantity") > 0)
    .filter(col("unit_price") >= 0)
    .dropDuplicates(["order_id"])
)

print("Bronze rows:", bronze.count())
print("Silver rows:", silver.count())

(silver.write
    .mode("overwrite")
    .partitionBy("order_date")
    .parquet(f"s3a://{BUCKET}/silver/orders/"))

spark.stop()