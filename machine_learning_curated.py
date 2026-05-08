import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Replace this with your own S3 bucket before running the Glue job.
STEDI_BUCKET = "s3://scott-wgu-d609"

# Script generated for node accelerometer_trusted
accelerometer_trusted_node1719086370653 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer_trusted", transformation_ctx="accelerometer_trusted_node1719086370653")

# Script generated for node customer_curated
customer_curated_node1719086370042 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_curated", transformation_ctx="customer_curated_node1719086370042")

# Script generated for node step_trainer_trusted
step_trainer_trusted_node1719086371042 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="step_trainer_trusted", transformation_ctx="step_trainer_trusted_node1719086371042")

# Script generated for node SQL Query
SqlQuery0 = '''
SELECT 
    s.sensorReadingTime,
    s.serialNumber as stepTrainerSerialNumber,
    CAST(s.distanceFromObject AS FLOAT) as distanceFromObject, -- casting to FLOAT if necessary
    a.user as email,
    a.timeStamp as accelerometerTimeStamp,
    CAST(a.x AS FLOAT) as x,  -- ensure this matches the FLOAT type in table definition
    CAST(a.y AS FLOAT) as y,
    CAST(a.z AS FLOAT) as z
FROM 
    step_trainer_trusted s
JOIN 
    accelerometer_trusted a ON s.sensorReadingTime = a.timeStamp
JOIN 
    customer_curated c ON s.serialNumber = c.serialnumber
'''
SQLQuery_node1719086409736 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"step_trainer_trusted":step_trainer_trusted_node1719086371042, "accelerometer_trusted":accelerometer_trusted_node1719086370653, "customer_curated":customer_curated_node1719086370042}, transformation_ctx = "SQLQuery_node1719086409736")

# Script generated for node machine_learning_curated
machine_learning_curated_sink = glueContext.getSink(
    path=f"{STEDI_BUCKET}/step_trainer/curated/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="machine_learning_curated_sink"
)
machine_learning_curated_sink.setCatalogInfo(catalogDatabase="stedi", catalogTableName="machine_learning_curated")
machine_learning_curated_sink.setFormat("json")
machine_learning_curated_sink.writeFrame(SQLQuery_node1719086409736)

job.commit()
