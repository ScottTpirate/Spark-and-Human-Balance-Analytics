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

# Script generated for node step_trainer_landing
steptrainerlanding_node1719085317734 = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    format="json",
    connection_options={"paths": [f"{STEDI_BUCKET}/step_trainer/landing/"], "recurse": True},
    transformation_ctx="steptrainerlanding_node1719085317734"
)

# Script generated for node customer curated
customercurated_node1719085342041 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_curated", transformation_ctx="customercurated_node1719085342041")

# Script generated for node SQL Query
SqlQuery0 = '''
SELECT 
    c.sensorReadingTime,
    c.serialNumber,
    c.distanceFromObject
FROM 
    step_trainer_landing c
JOIN 
    customer_curated a
ON 
    c.serialNumber = a.serialnumber
'''
SQLQuery_node1719085377858 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"customer_curated":customercurated_node1719085342041, "step_trainer_landing":steptrainerlanding_node1719085317734}, transformation_ctx = "SQLQuery_node1719085377858")

# Script generated for node step_trainer_trusted
step_trainer_trusted_sink = glueContext.getSink(
    path=f"{STEDI_BUCKET}/step_trainer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="step_trainer_trusted_sink"
)
step_trainer_trusted_sink.setCatalogInfo(catalogDatabase="stedi", catalogTableName="step_trainer_trusted")
step_trainer_trusted_sink.setFormat("json")
step_trainer_trusted_sink.writeFrame(SQLQuery_node1719085377858)

job.commit()
