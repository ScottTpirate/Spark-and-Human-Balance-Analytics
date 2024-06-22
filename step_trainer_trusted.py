import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Source: Load step trainer data
step_trainer_source = glueContext.create_dynamic_frame.from_catalog(database = "your_database_name", table_name = "step_trainer_landing", transformation_ctx = "step_trainer_source")

# Source: Load curated customer data
customer_curated_source = glueContext.create_dynamic_frame.from_catalog(database = "your_database_name", table_name = "customer_curated", transformation_ctx = "customer_curated_source")

# Transform: Join step trainer data with curated customer data by serial number
joined_data = Join.apply(step_trainer_source, customer_curated_source, 'serialNumber', 'serialnumber')

# Target: Write data to the curated zone
datasink = glueContext.write_dynamic_frame.from_options(frame = joined_data, connection_type = "s3", connection_options = {"path": "s3://path-to-curated-zone/step_trainer_curated", "partitionKeys": []}, format = "parquet", format_options = {"mergeSchema": "true"}, transformation_ctx = "datasink")

job.commit()
