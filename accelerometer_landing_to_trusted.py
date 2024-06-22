from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame

# Initialize Spark and Glue Contexts
spark = SparkSession.builder \
    .appName("Sanitize Accelerometer Data") \
    .getOrCreate()
glueContext = GlueContext(spark)

# Load data from S3
s3_bucket_path = "s3://cd0030bucket/accelerometer/"
accelerometer_df = spark.read.json(s3_bucket_path)

# Sanitize data: Filter accelerometer readings from customers who agreed to share their data
accelerometer_trusted_df = accelerometer_df.filter(col("sharewithresearchasofdate").isNotNull())

# Create a DynamicFrame
accelerometer_trusted_dyf = DynamicFrame.fromDF(accelerometer_trusted_df, glueContext, "accelerometer_trusted_dyf")

# Write to AWS Glue Catalog
glueContext.write_dynamic_frame.from_options(
    frame = accelerometer_trusted_dyf,
    connection_type = "s3",
    connection_options = {"path": "s3://path-to-trusted-zone/accelerometer_trusted"},
    format = "parquet"
)

# Log success
print("Accelerometer data sanitized and stored successfully.")
