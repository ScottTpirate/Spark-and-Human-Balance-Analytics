from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame

# Initialize Spark and Glue Contexts
spark = SparkSession.builder \
    .appName("Process Step Trainer IoT Data") \
    .getOrCreate()
glueContext = GlueContext(spark)

# Load Step Trainer data from S3
s3_bucket_path = "s3://cd0030bucket/step_trainer/"
step_trainer_df = spark.read.json(s3_bucket_path)

# Join with trusted customer data to ensure we only process data from customers who consented
s3_bucket_customer_trusted = "s3://path-to-trusted-zone/customer_trusted/"
customer_trusted_df = spark.read.parquet(s3_bucket_customer_trusted)

# Join on serial number and filter records based on customer consent
trusted_step_trainer_df = step_trainer_df.join(customer_trusted_df, step_trainer_df.serialNumber == customer_trusted_df.serialnumber) \
                                         .filter(customer_trusted_df.sharewithresearchasofdate.isNotNull())

# Select relevant columns for the step_trainer_trusted table
trusted_step_trainer_df = trusted_step_trainer_df.select("sensorReadingTime", "serialNumber", "distanceFromObject")

# Create a DynamicFrame
step_trainer_trusted_dyf = DynamicFrame.fromDF(trusted_step_trainer_df, glueContext, "step_trainer_trusted_dyf")

# Write to AWS Glue Catalog in the Trusted Zone
glueContext.write_dynamic_frame.from_options(
    frame = step_trainer_trusted_dyf,
    connection_type = "s3",
    connection_options = {"path": "s3://path-to-trusted-zone/step_trainer_trusted"},
    format = "parquet"
)

# Log success
print("Step Trainer trusted data table created and stored successfully.")
