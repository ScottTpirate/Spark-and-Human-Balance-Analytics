from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame

# Initialize Spark and Glue Contexts
spark = SparkSession.builder \
    .appName("Create Curated Customers Table") \
    .getOrCreate()
glueContext = GlueContext(spark)

# Load Trusted Customer Data
s3_bucket_customer_trusted = "s3://path-to-trusted-zone/customer_trusted/"
customer_trusted_df = spark.read.parquet(s3_bucket_customer_trusted)

# Load Trusted Accelerometer Data
s3_bucket_accelerometer_trusted = "s3://path-to-trusted-zone/accelerometer_trusted/"
accelerometer_trusted_df = spark.read.parquet(s3_bucket_accelerometer_trusted)

# Join Customer and Accelerometer Data on user identification
curated_df = customer_trusted_df.join(accelerometer_trusted_df, customer_trusted_df.serialnumber == accelerometer_trusted_df.user, "inner")

# Select relevant columns (customize this based on the fields you need)
curated_df = curated_df.select("serialnumber", "customername", "email", "phone", "timeStamp", "x", "y", "z")

# Create a DynamicFrame
customers_curated_dyf = DynamicFrame.fromDF(curated_df, glueContext, "customers_curated_dyf")

# Write to AWS Glue Catalog in the Curated Zone
glueContext.write_dynamic_frame.from_options(
    frame = customers_curated_dyf,
    connection_type = "s3",
    connection_options = {"path": "s3://path-to-curated-zone/customers_curated"},
    format = "parquet"
)

# Log success
print("Curated customers data table created and stored successfully.")
