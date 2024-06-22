from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame

# Initialize Spark and Glue Contexts
spark = SparkSession.builder \
    .appName("Aggregate Step Trainer and Accelerometer Data") \
    .getOrCreate()
glueContext = GlueContext(spark)

# Load Trusted Step Trainer Data
s3_bucket_step_trainer_trusted = "s3://path-to-trusted-zone/step_trainer_trusted/"
step_trainer_trusted_df = spark.read.parquet(s3_bucket_step_trainer_trusted)

# Load Trusted Accelerometer Data
s3_bucket_accelerometer_trusted = "s3://path-to-trusted-zone/accelerometer_trusted/"
accelerometer_trusted_df = spark.read.parquet(s3_bucket_accelerometer_trusted)

# Join the data on the condition of matching serial number/user and close timestamps
aggregated_df = step_trainer_trusted_df.join(
    accelerometer_trusted_df,
    (step_trainer_trusted_df.serialNumber == accelerometer_trusted_df.user) &
    (abs(step_trainer_trusted_df.sensorReadingTime.cast("long") - accelerometer_trusted_df.timeStamp.cast("long")) <= 5)
)

# Select relevant columns and potentially create new features or transformations
aggregated_df = aggregated_df.select(
    step_trainer_trusted_df.sensorReadingTime,
    step_trainer_trusted_df.serialNumber,
    step_trainer_trusted_df.distanceFromObject,
    accelerometer_trusted_df.x,
    accelerometer_trusted_df.y,
    accelerometer_trusted_df.z
)

# Create a DynamicFrame
machine_learning_curated_dyf = DynamicFrame.fromDF(aggregated_df, glueContext, "machine_learning_curated_dyf")

# Write to AWS Glue Catalog in the Curated Zone
glueContext.write_dynamic_frame.from_options(
    frame = machine_learning_curated_dyf,
    connection_type = "s3",
    connection_options = {"path": "s3://path-to-curated-zone/machine_learning_curated"},
    format = "parquet"
)

# Log success
print("Machine learning curated data table created and stored successfully.")
