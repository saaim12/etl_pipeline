import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession

os.environ["HADOOP_HOME"] = r"C:\hadoop"
os.environ["PATH"] = r"C:\hadoop\bin;" + os.environ["PATH"]
load_dotenv()

KEY = os.environ["DO_SPACES_KEY"]
SECRET = os.environ["DO_SPACES_SECRET"]
ENDPOINT = os.environ["DO_SPACES_ENDPOINT"]
BUCKET = os.environ["DO_SPACES_BUCKET"]

spark = (
    SparkSession.builder
    .appName("check-bronze")
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

df = spark.read.parquet(f"s3a://{BUCKET}/bronze/orders/")
df.show(5)
print("Count:", df.count())
spark.stop()