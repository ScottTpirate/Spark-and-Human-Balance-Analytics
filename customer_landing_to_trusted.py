from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame

# Initialize Spark and Glue Contexts
spark = SparkSession.builder \
    .appName("Sanitize Customer Data") \
    .getOrCreate()
glueContext = GlueContext(spark)

# Load data from S3
s3_bucket_path = "s3://cd0030bucket/customers/"
customer_df = spark.read.json(s3_bucket_path)

# Sanitize data: Select customers who have agreed to share their data for research
customer_trusted_df = customer_df.filter(col("sharewithresearchasofdate").isNotNull())

# Convert date strings to date type if necessary
customer_trusted_df = customer_trusted_df.withColumn("sharewithresearchasofdate", to_date(col("sharewithresearchasofdate")))

# Create a DynamicFrame
customer_trusted_dyf = DynamicFrame.fromDF(customer_trusted_df, glueContext, "customer_trusted_dyf")

# Write to AWS Glue Catalog
glueContext.write_dynamic_frame.from_options(
    frame=customer_trusted_dyf,
    connection_type="s3",
    connection_options={"path": "s3://path-to-trusted-zone/customer_trusted"},
    format="parquet"
)

# Log success
print("Customer data sanitized and stored successfully.")
